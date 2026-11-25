package connectors

import (
	"fmt"

	"github.com/datapointchris/ingestion/store"
)

// deliverers holds the name of each deliverer that this build registers.
var deliverers = map[string]bool{}

// RegisterDeliverer records that this build carries the deliverer called
// name, which a destination names as its deliverer. The package holding a
// deliverer registers it from its own init. Registering a name twice panics,
// as a duplicate is a build error.
func RegisterDeliverer(name string) {
	if deliverers[name] {
		panic("connectors: deliverer " + name + " is registered twice")
	}
	deliverers[name] = true
}

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
)

// Undelivered is why an item that a side decision placed reaches no
// deliverer.
type Undelivered struct {
	Reason string
	Side   string
	// Destination names where the placement sends the item, and is empty
	// where it sends it nowhere.
	Destination string
}

func (u *Undelivered) Error() string {
	switch u.Reason {
	case UndeliveredNoSideDefault:
		return fmt.Sprintf("no destination in the set in force is default_for_side %s, so an item placed on that side with no destination named waits", u.Side)
	case UndeliveredNotDeclared:
		return fmt.Sprintf("%s is not a destination the set in force declares", u.Destination)
	default:
		return fmt.Sprintf("%s names no deliverer, so nothing writes the items placed there", u.Destination)
	}
}

// DeliveryTo is the destination that the item placed by side reaches: the one
// side names, else its side's default. It is an Undelivered where that
// destination is not declared, names no deliverer, or where side names none
// and its side has no default.
func (s Set) DeliveryTo(side store.Decision) (Destination, *Undelivered) {
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
	return d, nil
}
