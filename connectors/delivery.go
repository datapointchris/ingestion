package connectors

import (
	"errors"
	"fmt"

	"github.com/datapointchris/ingestion/store"
)

// Writes is what one deliverer can write. Takes answers why it cannot write
// an item, and nil where it can, and a nil Takes writes every item. FilePath
// says it writes an item that holds bytes by its file: it takes such an item
// only with a live home below the root that SetDelivererRoot names, and its
// delivery keeps that home's path. An item that holds no bytes, such as a
// video, reads no home, and Takes alone decides it. Chapters says it writes a
// book's chapter as part of its book's record, so a chapter waits for its
// book's delivery and keeps that record's id. Takes and FilePath read no
// chapter, which holds no bytes or home. Playlist says it adds an item to the
// playlist that a person's sort picked, and refuses a delivery without one.
type Writes struct {
	Takes    func(store.Item) error
	FilePath bool
	Chapters bool
	Playlist bool
}

// deliverers holds what each deliverer that this build registers writes, by
// name.
var deliverers = map[string]Writes{}

// delivererRoots holds the [serve.homes] root that each deliverer writing by
// file path reads, as serve's config names it.
var delivererRoots = map[string]string{}

// RegisterDeliverer records that this build carries the deliverer called
// name, which a destination names as its deliverer, and what it writes.
// deliver.Register calls it beside the deliverer's constructor, so a name that
// a set may give is one a worker can open. Registering a name twice panics,
// as a duplicate is a build error.
func RegisterDeliverer(name string, w Writes) {
	if _, ok := deliverers[name]; ok {
		panic("connectors: deliverer " + name + " is registered twice")
	}
	if w.Takes == nil {
		w.Takes = func(store.Item) error { return nil }
	}
	deliverers[name] = w
}

// TakesPlaylist says whether the deliverer called name adds an item to the
// playlist that a person's watch picked. No tag, rule, side default or
// placement by destination carries a playlist, so only the watch choice places
// an item on such a deliverer's destination.
func TakesPlaylist(name string) bool { return deliverers[name].Playlist }

// SetDelivererRoot names root as the [serve.homes] root below which the
// deliverer called name reads each item's file. serve calls it for each
// [deliver.<name>] table that names a root. It refuses a deliverer that this
// build does not register, or that writes no file path.
func SetDelivererRoot(name, root string) error {
	d, ok := deliverers[name]
	switch {
	case !ok:
		return fmt.Errorf("[deliver.%s] names a root, and %s is no deliverer this build registers", name, name)
	case !d.FilePath:
		return fmt.Errorf("[deliver.%s] names a root, and %s writes no file path", name, name)
	}
	delivererRoots[name] = root
	return nil
}

// FilePath is the path at which the deliverer called name writes an item
// whose homes are homes: the first live one found below the deliverer's root.
// It is nil for a deliverer that writes no file path. Its error says why the
// deliverer cannot take the item.
func FilePath(name string, homes []store.Home) (*string, error) {
	if d, ok := deliverers[name]; !ok || !d.FilePath {
		return nil, nil
	}
	root, ok := delivererRoots[name]
	if !ok {
		return nil, fmt.Errorf("serve's config names no root in [deliver.%s]", name)
	}
	for _, h := range homes {
		if h.Root == root && h.GoneTS == nil {
			return &h.Path, nil
		}
	}
	return nil, fmt.Errorf("it has no live home under root %s", root)
}

var errNoChapters = errors.New("it is a book's chapter, and the deliverer writes no chapters")

// Why an item placed on a side and read as is reaches no deliverer, in the
// words of schemas/item.schema.json.
const (
	// UndeliveredNoSideDefault is a placement naming a side and no
	// destination, on a side with no default destination. The item waits
	// until a set names one.
	UndeliveredNoSideDefault = "no-side-default"
	// UndeliveredNotDeclared is a placement naming a destination that the
	// set in force does not declare.
	UndeliveredNotDeclared = "not-declared"
	// UndeliveredNoDeliverer is a destination naming no deliverer, such as
	// one whose reader pulls its items itself.
	UndeliveredNoDeliverer = "no-deliverer"
	// UndeliveredNotTaken is an item that the destination's deliverer cannot
	// write, such as one with no URL for a deliverer that saves links.
	UndeliveredNotTaken = "not-taken"
	// UndeliveredBookUnsettled is a book's chapter whose book holds no
	// settled delivery to the destination. The chapter names its book's
	// record there, so it waits until the book's delivery settles.
	UndeliveredBookUnsettled = "book-unsettled"
	// UndeliveredBookRefused is a book's chapter whose book the destination
	// refused, so no record there holds the chapter.
	UndeliveredBookRefused = "book-refused"
)

// Undelivered is why an item that a side decision placed reaches no
// deliverer.
type Undelivered struct {
	Reason string
	Side   string
	// Destination names where the placement sends the item, and is empty
	// where it sends it nowhere.
	Destination string
	// Deliverer and Why name the deliverer that cannot take the item, and why.
	Deliverer, Why string
}

func (u *Undelivered) Error() string {
	switch u.Reason {
	case UndeliveredNoSideDefault:
		return fmt.Sprintf("no destination in the set in force is default_for_side %s, so an item placed on that side with no destination named waits", u.Side)
	case UndeliveredNotDeclared:
		return fmt.Sprintf("%s is not a destination the set in force declares", u.Destination)
	case UndeliveredNotTaken:
		return fmt.Sprintf("%s's deliverer %s cannot take the item: %s", u.Destination, u.Deliverer, u.Why)
	case UndeliveredBookUnsettled:
		return fmt.Sprintf("the item is a chapter, and its book's delivery to %s has not settled, so the chapter waits for it", u.Destination)
	case UndeliveredBookRefused:
		return fmt.Sprintf("the item is a chapter, and %s refused its book, so the chapter has no book there to be part of", u.Destination)
	default:
		return fmt.Sprintf("%s names no deliverer, so nothing writes the items placed there", u.Destination)
	}
}

// DeliveryTo is the destination that item, placed by side, reaches: the one
// side names, else its side's default. It is an Undelivered where that
// destination is not declared, names no deliverer, or has a deliverer that
// cannot take item, and where side names none and its side has no default.
// homes are the item's homes, whose live ones a deliverer writing by file
// path reads. It takes a book's chapter only where the deliverer writes
// chapters.
func (s Set) DeliveryTo(side store.Decision, item store.Item, homes []store.Home) (Destination, *Undelivered) {
	var d Destination
	var ok bool
	if side.Destination != nil {
		if d, ok = s.Destination(*side.Destination); !ok {
			return d, &Undelivered{Reason: UndeliveredNotDeclared, Side: side.Verdict, Destination: *side.Destination}
		}
	} else if d, ok = s.SideDefault(side.Verdict); !ok {
		return d, &Undelivered{Reason: UndeliveredNoSideDefault, Side: side.Verdict}
	}
	if d.Deliverer == "" {
		return d, &Undelivered{Reason: UndeliveredNoDeliverer, Side: side.Verdict, Destination: d.Name}
	}
	if w, ok := deliverers[d.Deliverer]; ok {
		var err error
		switch {
		case item.PlacedThroughParent() && !w.Chapters:
			err = errNoChapters
		case !item.PlacedThroughParent():
			if err = w.Takes(item); err == nil && item.BlobSHA256 != nil {
				_, err = FilePath(d.Deliverer, homes)
			}
		}
		if err != nil {
			return d, &Undelivered{Reason: UndeliveredNotTaken, Side: side.Verdict, Destination: d.Name, Deliverer: d.Deliverer, Why: err.Error()}
		}
	}
	return d, nil
}
