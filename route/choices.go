package route

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/store"
)

// The choices that review offers a person for an item that waits on them. A choice with a Side
// places it on a destination. Archive keeps its record with no destination, and drop drops it.
const (
	ChoiceArticles   = "articles"
	ChoiceLearning   = "learning"
	ChoiceSuggestion = "suggestion"
	ChoiceWatch      = "watch"
	ChoicePsyche     = "psyche"
	ChoiceArchive    = "archive"
	ChoiceDrop       = "drop"
)

// LensFleet is the lens that a suggestion's model read takes.
const LensFleet = connectors.SideFleet

// The deliverers whose destination a choice places an item on, as each registers its name.
const (
	ArticlesDeliverer = "life-articles"
	LearningDeliverer = "learning-resources"
	WatchDeliverer    = "ypl-watch"
)

// PsycheDestination is the destination that the psyche choice and a push to psyche place an item
// on.
const PsycheDestination = "psyche"

// Choice is what one of review's choices does with an item. A placing choice puts it on Side, on the
// destination whose deliverer is Deliverer, or the one named DestinationName, or the side's default
// where it names neither, and reads it as Read says, under Lens where a model reads it. Archive and
// drop name no side.
type Choice struct {
	Name            string
	Side            string
	Deliverer       string
	DestinationName string
	Read            string
	Lens            string
	// RunsHere says the client that chose it hands the item to psyche relate on its own machine once
	// the placement lands, and nothing in the service runs it.
	RunsHere bool
	// Help is the choice's line in the CLI's help and the review tab's keys.
	Help string
}

// Choices is every choice that review offers, in the order the review tab binds them.
var Choices = []Choice{
	{
		Name: ChoiceArticles, Side: connectors.SidePersonal, Deliverer: ArticlesDeliverer, Read: connectors.ReadAsIs,
		Help: "delivered to your articles to read as is, with no summary",
	},
	{
		Name: ChoiceLearning, Side: connectors.SidePersonal, Deliverer: LearningDeliverer, Read: connectors.ReadAsIs,
		Help: "delivered to your learning resources, whatever the gate answered",
	},
	{
		Name: ChoiceSuggestion, Side: connectors.SideFleet, Read: connectors.ReadModel, Lens: LensFleet,
		Help: "a model reads it under the fleet lens and suggests fleet work",
	},
	{
		Name: ChoiceWatch, Side: connectors.SidePersonal, Deliverer: WatchDeliverer, Read: connectors.ReadAsIs,
		Help: "added to the playlist you pick at its source, or the default",
	},
	{
		Name: ChoicePsyche, Side: connectors.SidePersonal, DestinationName: PsycheDestination, Read: connectors.ReadAsIs, RunsHere: true,
		Help: "placed at psyche, and psyche relate reads it here at once",
	},
	{
		Name: ChoiceArchive,
		Help: "done: kept with no destination, marked read, asks what you liked",
	},
	{
		Name: ChoiceDrop,
		Help: "dropped with its row kept, after asking what decided it",
	},
}

// ChoiceNames is each choice's name, in order.
func ChoiceNames() []string {
	names := make([]string, len(Choices))
	for i, c := range Choices {
		names[i] = c.Name
	}
	return names
}

// PlacingChoiceNames is the name of each choice that places an item on a destination, in order.
func PlacingChoiceNames() []string {
	var names []string
	for _, c := range Choices {
		if c.Places() {
			names = append(names, c.Name)
		}
	}
	return names
}

// ChoiceOf is the choice named name, and false where none is.
func ChoiceOf(name string) (Choice, bool) {
	i := slices.IndexFunc(Choices, func(c Choice) bool { return c.Name == name })
	if i < 0 {
		return Choice{}, false
	}
	return Choices[i], true
}

// Places says whether the choice places an item on a destination.
func (c Choice) Places() bool { return c.Side != "" }

// TakesPlaylist says whether the choice's deliverer adds the item to a playlist that the person
// picks.
func (c Choice) TakesPlaylist() bool {
	return c.Deliverer != "" && connectors.TakesPlaylist(c.Deliverer)
}

// Destination is where the choice places an item under set: the destination it names, the first
// destination whose deliverer is its own, or for a choice naming neither, its side's default, else
// the side's first. Only a named destination may be push-only. Archive and drop place on none, so
// they answer nil. missing says why the choice places nothing, where set declares no destination it
// takes.
func (c Choice) Destination(set connectors.Set) (destination *connectors.Destination, missing string) {
	takes := func(d connectors.Destination) bool { return !d.PushOnly && d.Deliverer == c.Deliverer && d.Side != "" }
	switch {
	case !c.Places():
		return nil, ""
	case c.DestinationName != "":
		if d, ok := set.Destination(c.DestinationName); ok && d.Side != "" {
			return &d, ""
		}
		return nil, fmt.Sprintf("the set in force declares no destination %s on a side, so %s places nothing", c.DestinationName, c.Name)
	case c.Deliverer == "":
		if d, ok := set.SideDefault(c.Side); ok && !d.PushOnly {
			return &d, ""
		}
		takes = func(d connectors.Destination) bool { return !d.PushOnly && d.Side == c.Side }
	}
	if i := slices.IndexFunc(set.Destinations, takes); i >= 0 {
		return &set.Destinations[i], ""
	}
	if c.Deliverer == "" {
		return nil, fmt.Sprintf("the set in force declares no destination on the %s side, so %s places nothing", c.Side, c.Name)
	}
	return nil, fmt.Sprintf("no destination of the set in force names the deliverer %s, so %s places nothing", c.Deliverer, c.Name)
}

// Pick is a person's pick of an item by one placing choice.
type Pick struct {
	Choice string
	// Playlist is the playlist that a watch adds the video to, and DefaultPlaylist the one it takes
	// where Playlist is empty.
	Playlist, DefaultPlaylist string
	// Cause and Note are a Placement's.
	Cause           string
	Note            *string
	Login, ClientID string
	Presented       *store.Presented
	DecidedAfterMS  *int64
}

// ChoiceRefusedError is a pick that its choice cannot carry out on the item: no destination of the
// set in force takes the choice, the destination's deliverer cannot take the item, a watch names no
// playlist, or the item's delivery already holds another playlist.
type ChoiceRefusedError struct {
	Choice string
	Why    string
}

func (e *ChoiceRefusedError) Error() string { return e.Choice + " places nothing: " + e.Why }

// ErrUnknownChoice is a pick naming no placing choice of Choices.
var ErrUnknownChoice = errors.New("the choice is none that places an item")

// Choose appends a person's pick of the item under key by p's choice: its side decision on the
// choice's destination, under the cause the person named or unstated, and its read decision, decided
// by the person under the choice's name. A choice whose Read is as-is reads it as is, so no model
// reads it, whatever the quality gate answered. A suggestion reads it by a model under the
// fleet lens, after a note's draft, and takes a draft already done. Watch keeps the playlist on its
// side decision for the delivery, and a watch whose item's delivery already holds another playlist
// is refused, since a delivery adds the video to one. A pick that repeats the newest side decision,
// playlist included, appends nothing. A pick of an item whose drop the gate showed or hid disagrees
// with the gate, as a placement does. It answers what Place answers, a ChoiceRefusedError, and
// ErrUnknownChoice.
func Choose(ctx context.Context, tx *store.Tx, key string, p Pick) ([]store.Decision, error) {
	c, ok := ChoiceOf(p.Choice)
	if !ok || !c.Places() {
		return nil, fmt.Errorf("%q: %w, which are %s", p.Choice, ErrUnknownChoice, strings.Join(PlacingChoiceNames(), ", "))
	}
	item, err := placeable(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	destination, missing := c.Destination(set)
	if missing != "" {
		return nil, &ChoiceRefusedError{Choice: c.Name, Why: missing}
	}
	side := personsSide(destination.Side, p.Cause, p.Note, p.Login, p.ClientID, p.Presented, p.DecidedAfterMS)
	side.Destination = &destination.Name
	var homes []store.Home
	if item.BlobSHA256 != nil {
		if homes, err = tx.LiveHomes(ctx, *item.BlobSHA256); err != nil {
			return nil, err
		}
	}
	if !destination.PushOnly {
		if _, undelivered := set.DeliveryTo(side, item, homes); undelivered != nil && undelivered.Reason == connectors.UndeliveredNotTaken {
			return nil, &ChoiceRefusedError{Choice: c.Name, Why: undelivered.Error()}
		}
	}
	if c.TakesPlaylist() {
		playlist := strings.TrimSpace(p.Playlist)
		if playlist == "" {
			playlist = strings.TrimSpace(p.DefaultPlaylist)
		}
		if playlist == "" {
			return nil, &ChoiceRefusedError{Choice: c.Name, Why: "the pick names no playlist, and serve's [watch] default_playlist names none"}
		}
		held, found, err := tx.Delivery(ctx, key, destination.Name)
		if err != nil {
			return nil, err
		}
		if found && text(held.Playlist) != playlist {
			return nil, &ChoiceRefusedError{Choice: c.Name, Why: fmt.Sprintf(
				"its delivery to %s holds the playlist %s, as %s, and a delivery adds the video to one playlist", destination.Name, text(held.Playlist), held.Outcome)}
		}
		side.Playlist = &playlist
	}
	if made, ok, err := repeated(ctx, tx, key, side); err != nil || ok {
		return made, err
	}
	read := store.Decision{Gate: store.GateRead, Verdict: c.Read, DecidedBy: side.DecidedBy, Cause: c.Name, ClientID: side.ClientID}
	if c.Lens != "" {
		lens := c.Lens
		read.Lens, read.Note = &lens, p.Note
	}
	if store.NoteDigest(p.Note) == nil {
		drafted, err := takeDraft(ctx, tx, key, read, side)
		if err != nil {
			return nil, err
		}
		if drafted != nil {
			read = *drafted
		}
	}
	return decideByPerson(ctx, tx, key, side, set, p.Note, &read)
}

// The likes that an archive may name: what the person liked about the item, which a keep-rule
// proposal reads as the archive's cause. LikedThisItem says only the item itself was liked.
const (
	LikedChannel  = CauseChannel
	LikedTheme    = CauseTheme
	LikedThisItem = "this-item"
)

// Likes is every like an archive may name, in the order review asks them.
var Likes = []string{LikedChannel, LikedTheme, LikedThisItem}

// Archival is a person's archive of an item: done, such as a video watched elsewhere. Liked is what
// they liked, one of Likes, or empty where they named none. Login, ClientID, Presented,
// DecidedAfterMS and Unmarked are a Placement's, and Unmarked keeps the read mark unwritten.
type Archival struct {
	Liked           string
	Login, ClientID string
	Presented       *store.Presented
	DecidedAfterMS  *int64
	Unmarked        bool
}

// ErrUnknownLike is an archive naming a like that is none of Likes.
var ErrUnknownLike = errors.New("the like is none that an archive names")

// Archive appends a person's archive of the item under key: its side decision on the personal side
// with no destination, under the like as its cause, or unstated, and its read decision, verdict
// archive, so nothing reads or delivers it. It then writes the person's read mark, which teaches the
// quality gate as the mark does, unless the archival is Unmarked. An archive that repeats the
// newest side decision appends nothing. It answers store.ErrNoItem, a ChapterError and
// ErrUnknownLike.
func Archive(ctx context.Context, tx *store.Tx, key string, a Archival) ([]store.Decision, error) {
	if a.Liked != "" && !slices.Contains(Likes, a.Liked) {
		return nil, fmt.Errorf("%q: %w, which are %s", a.Liked, ErrUnknownLike, strings.Join(Likes, ", "))
	}
	if _, err := placeable(ctx, tx, key); err != nil {
		return nil, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	side := personsSide(connectors.SidePersonal, a.Liked, nil, a.Login, a.ClientID, a.Presented, a.DecidedAfterMS)
	if made, ok, err := repeated(ctx, tx, key, side); err != nil || ok {
		return made, err
	}
	read := store.Decision{Gate: store.GateRead, Verdict: store.VerdictArchive, DecidedBy: side.DecidedBy, Cause: ChoiceArchive, ClientID: side.ClientID}
	made, err := decide(ctx, tx, key, side, set, nil, nil, &read)
	if err != nil || a.Unmarked {
		return made, err
	}
	_, _, err = gatetrust.Mark(ctx, tx, key, gatetrust.Marking{Mark: gatetrust.MarkRead, DecidedBy: side.DecidedBy, ClientID: a.ClientID})
	return made, err
}

// placeable reads the item under key, and answers store.ErrNoItem where the store holds none and a
// ChapterError for a book's chapter, which takes its book's decisions.
func placeable(ctx context.Context, tx *store.Tx, key string) (store.Item, error) {
	item, ok, err := tx.Item(ctx, key)
	switch {
	case err != nil:
		return store.Item{}, err
	case !ok:
		return store.Item{}, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	case item.PlacedThroughParent():
		return store.Item{}, &ChapterError{Chapter: key, Book: *item.ParentKey}
	}
	return item, nil
}

// personsSide is a person's side decision on verdict, under cause, or unstated where it is empty.
func personsSide(verdict, cause string, note *string, login, clientID string, presented *store.Presented, after *int64) store.Decision {
	side := store.Decision{
		Gate: store.GateSide, Verdict: verdict, DecidedBy: ByPerson(login), Cause: cause, Note: note,
		Presented: presented, DecidedAfterMS: after,
	}
	if side.Cause == "" {
		side.Cause = CauseUnstated
	}
	if clientID != "" {
		side.ClientID = &clientID
	}
	return side
}

// decideByPerson appends a person's side decision and read, and where the gate showed or hid the
// item's drop, disagrees with it, as a placement on a destination does.
func decideByPerson(ctx context.Context, tx *store.Tx, key string, side store.Decision, set connectors.Set, note *string, read *store.Decision) ([]store.Decision, error) {
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return nil, err
	}
	held, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil {
		return nil, err
	}
	made, err := decide(ctx, tx, key, side, set, sightings, note, read)
	if err != nil || !sided || !gateDropped(held) {
		return made, err
	}
	_, err = gatetrust.Disagree(ctx, tx, key, side.DecidedBy, text(side.ClientID))
	return made, err
}
