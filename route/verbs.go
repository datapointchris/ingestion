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

// The sort verbs, each a route that a person's sort in the review tab takes.
// Drop is not one of them: the drop route takes it, with its cause.
const (
	VerbFleetRelate        = "fleet-relate"
	VerbLearning           = "learning"
	VerbPersonalRelate     = "personal-relate"
	VerbPersonalSummarize  = "personal-summarize"
	VerbTechnicalSummarize = "technical-summarize"
	VerbWatch              = "watch"
)

// The lenses that a verb reads an item through.
const (
	LensFleet    = connectors.SideFleet
	LensPersonal = connectors.SidePersonal
	LensLearning = "learning"
)

// The deliverers whose destination a verb places an item on, as each
// registers its name.
const (
	LearningDeliverer = "learning-resources"
	WatchDeliverer    = "ypl-watch"
)

// VerbRoute is what one verb does with an item: the side it places it on, the
// deliverer whose destination it places it on, or the side's fleet
// destination, or none, and how it is read then. Read is as-is, model or
// shelf. A shelf's Lens is the lens through which opening it reads the item,
// and empty where opening runs nothing in the service.
type VerbRoute struct {
	Verb      string
	Side      string
	Deliverer string
	Read      string
	Lens      string
	// NeedsPass sends the item back to review as learning-refused, rather
	// than reading it, where the quality gate's newest answer did not pass it.
	NeedsPass bool
	// OpensHere says opening a shelved item runs the verb on the machine that
	// opens it, and nothing in the service.
	OpensHere bool
	// Help is the verb's line in the CLI's help and the review tab's keys.
	Help string
}

// Verbs is every sort verb, in the order the review tab binds them.
var Verbs = []VerbRoute{
	{
		Verb: VerbFleetRelate, Side: connectors.SideFleet, Read: connectors.ReadModel, Lens: LensFleet,
		Help: "a model reads it under the fleet lens against your current work, after a note's draft, and keeps its suggestion",
	},
	{
		Verb: VerbLearning, Side: connectors.SidePersonal, Deliverer: LearningDeliverer, Read: connectors.ReadAsIs, NeedsPass: true,
		Help: "delivered to the learning plan where the quality gate's newest answer passes it, and back in review as " + CauseLearningRefused + " otherwise",
	},
	{
		Verb: VerbPersonalRelate, Side: connectors.SidePersonal, Read: store.VerdictShelf, OpensHere: true,
		Help: "shelved: nothing runs until you open it, which hands its parse to psyche relate on your machine",
	},
	{
		Verb: VerbPersonalSummarize, Side: connectors.SidePersonal, Read: store.VerdictShelf, Lens: LensPersonal,
		Help: "shelved: nothing runs until you open it, which summarizes it once under the personal lens and keeps the summary",
	},
	{
		Verb: VerbTechnicalSummarize, Side: connectors.SidePersonal, Read: store.VerdictShelf, Lens: LensLearning,
		Help: "shelved: nothing runs until you open it, which summarizes it once under the learning lens and keeps the summary",
	},
	{
		Verb: VerbWatch, Side: connectors.SidePersonal, Deliverer: WatchDeliverer, Read: connectors.ReadAsIs,
		Help: "added to a playlist at its source to watch, the one you pick or the service's default",
	},
}

// VerbNames is each verb's name, in order.
func VerbNames() []string {
	names := make([]string, len(Verbs))
	for i, v := range Verbs {
		names[i] = v.Verb
	}
	return names
}

// VerbOf is the verb named name, and false where none is.
func VerbOf(name string) (VerbRoute, bool) {
	i := slices.IndexFunc(Verbs, func(v VerbRoute) bool { return v.Verb == name })
	if i < 0 {
		return VerbRoute{}, false
	}
	return Verbs[i], true
}

// Shelves says whether the verb puts an item on the shelf.
func (v VerbRoute) Shelves() bool { return v.Read == store.VerdictShelf }

// TakesPlaylist says whether the verb's deliverer adds the item to a playlist
// that the sort picks.
func (v VerbRoute) TakesPlaylist() bool {
	return v.Deliverer != "" && connectors.TakesPlaylist(v.Deliverer)
}

// ShelfVerbs is each verb that puts an item on the shelf, in order.
func ShelfVerbs() []string {
	var out []string
	for _, v := range Verbs {
		if v.Shelves() {
			out = append(out, v.Verb)
		}
	}
	return out
}

// Destination is where the verb places an item under set: the first
// destination whose deliverer is the verb's, or, for fleet relate, the fleet
// side's default, else its first. A push-only destination is never one. A
// shelf places on none, so it answers nil. missing says why the verb places
// nothing, where set declares no destination it takes.
func (v VerbRoute) Destination(set connectors.Set) (destination *connectors.Destination, missing string) {
	takes := func(d connectors.Destination) bool { return !d.PushOnly && d.Deliverer == v.Deliverer && d.Side != "" }
	switch {
	case v.Shelves():
		return nil, ""
	case v.Deliverer == "":
		if d, ok := set.SideDefault(v.Side); ok && !d.PushOnly {
			return &d, ""
		}
		takes = func(d connectors.Destination) bool { return !d.PushOnly && d.Side == v.Side }
	}
	if i := slices.IndexFunc(set.Destinations, takes); i >= 0 {
		return &set.Destinations[i], ""
	}
	if v.Deliverer == "" {
		return nil, fmt.Sprintf("the set in force declares no destination on the %s side, so %s places nothing", v.Side, v.Verb)
	}
	return nil, fmt.Sprintf("no destination of the set in force names the deliverer %s, so %s places nothing", v.Deliverer, v.Verb)
}

// Sort is a person's sort of an item by one verb.
type Sort struct {
	Verb string
	// Playlist is the playlist that a watch adds the video to, and
	// DefaultPlaylist the one it takes where Playlist is empty.
	Playlist, DefaultPlaylist string
	Login, ClientID           string
	// Presented and DecidedAfterMS are a Placement's.
	Presented      *store.Presented
	DecidedAfterMS *int64
}

// VerbRefusedError is a sort that its verb cannot carry out on the item: no
// destination of the set in force takes the verb, the destination's deliverer
// cannot take the item, a watch names no playlist, or the item's delivery
// already holds another playlist.
type VerbRefusedError struct {
	Verb string
	Why  string
}

func (e *VerbRefusedError) Error() string { return e.Verb + " sorts nothing: " + e.Why }

// ErrUnknownVerb is a sort naming no verb of Verbs.
var ErrUnknownVerb = errors.New("the verb is none that a sort takes")

// SortItem appends a person's sort of the item under key by s's verb: its
// side decision, under the verb as its cause, and its read decision, decided
// by the person under the verb too. Fleet relate reads it by a model under the
// fleet lens, after a note's draft, and takes a draft already done. Learning
// reads it as is where the quality gate's newest decision passes it, and
// otherwise sends it to review at the read gate as learning-refused, decided
// by the gate's run. Watch reads it as is, and keeps the playlist on its side
// decision for the delivery. A watch whose item's delivery already holds
// another playlist is refused, since a delivery adds the video to one. A shelf
// verb reads it by nothing until a person opens it. A sort that repeats the
// newest side decision, playlist included, appends nothing. A
// sort of an item whose drop the gate showed or hid disagrees with the gate,
// as a placement does. It answers what Place answers, a VerbRefusedError, and
// ErrUnknownVerb.
func SortItem(ctx context.Context, tx *store.Tx, key string, s Sort) ([]store.Decision, error) {
	v, ok := VerbOf(s.Verb)
	if !ok {
		return nil, fmt.Errorf("%q: %w, which are %s", s.Verb, ErrUnknownVerb, strings.Join(VerbNames(), ", "))
	}
	item, ok, err := tx.Item(ctx, key)
	switch {
	case err != nil:
		return nil, err
	case !ok:
		return nil, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	case item.PlacedThroughParent():
		return nil, &ChapterError{Chapter: key, Book: *item.ParentKey}
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	destination, missing := v.Destination(set)
	if missing != "" {
		return nil, &VerbRefusedError{Verb: v.Verb, Why: missing}
	}
	side := store.Decision{
		Gate: store.GateSide, Verdict: v.Side, DecidedBy: ByPerson(s.Login), Cause: v.Verb,
		Presented: s.Presented, DecidedAfterMS: s.DecidedAfterMS,
	}
	if s.ClientID != "" {
		side.ClientID = &s.ClientID
	}
	if destination != nil {
		side.Verdict, side.Destination = destination.Side, &destination.Name
		var homes []store.Home
		if item.BlobSHA256 != nil {
			if homes, err = tx.LiveHomes(ctx, *item.BlobSHA256); err != nil {
				return nil, err
			}
		}
		if _, undelivered := set.DeliveryTo(side, item, homes); undelivered != nil && undelivered.Reason == connectors.UndeliveredNotTaken {
			return nil, &VerbRefusedError{Verb: v.Verb, Why: undelivered.Error()}
		}
	}
	if v.TakesPlaylist() {
		playlist := strings.TrimSpace(s.Playlist)
		if playlist == "" {
			playlist = strings.TrimSpace(s.DefaultPlaylist)
		}
		if playlist == "" {
			return nil, &VerbRefusedError{Verb: v.Verb, Why: "the sort names no playlist, and serve's [watch] default_playlist names none"}
		}
		held, found, err := tx.Delivery(ctx, key, destination.Name)
		if err != nil {
			return nil, err
		}
		if found && text(held.Playlist) != playlist {
			return nil, &VerbRefusedError{Verb: v.Verb, Why: fmt.Sprintf(
				"its delivery to %s holds the playlist %s, as %s, and a delivery adds the video to one playlist", destination.Name, text(held.Playlist), held.Outcome)}
		}
		side.Playlist = &playlist
	}
	if made, ok, err := repeated(ctx, tx, key, side); err != nil || ok {
		return made, err
	}
	read, err := verbRead(ctx, tx, key, v, side)
	if err != nil {
		return nil, err
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return nil, err
	}
	held, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil {
		return nil, err
	}
	made, err := decide(ctx, tx, key, side, set, sightings, nil, &read)
	if err != nil || !sided || !gateDropped(held) {
		return made, err
	}
	_, err = gatetrust.Disagree(ctx, tx, key, side.DecidedBy, s.ClientID)
	return made, err
}

// verbRead is the read decision of a sort by v whose side decision is side.
func verbRead(ctx context.Context, tx *store.Tx, key string, v VerbRoute, side store.Decision) (store.Decision, error) {
	read := store.Decision{Gate: store.GateRead, Verdict: v.Read, DecidedBy: side.DecidedBy, Cause: v.Verb, ClientID: side.ClientID}
	if v.Lens != "" {
		lens := v.Lens
		read.Lens = &lens
	}
	if v.NeedsPass {
		quality, judged, err := tx.NewestDecision(ctx, key, store.GateQuality)
		if err != nil || (judged && quality.Verdict == store.VerdictPass) {
			return read, err
		}
		refused := store.Decision{Gate: store.GateRead, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseLearningRefused}
		if judged {
			refused.DecidedBy, refused.Confidence = quality.DecidedBy, quality.Confidence
		}
		return refused, nil
	}
	drafted, err := takeDraft(ctx, tx, key, read, side)
	if err != nil || drafted == nil {
		return read, err
	}
	return *drafted, nil
}

// The states of a shelved item, as the shelf answers them.
const (
	// ShelfWaiting is an item on the shelf that nobody opened.
	ShelfWaiting = store.ShelfWaiting
	// ShelfOpened is a relate that a person opened, which runs on their
	// machine and keeps nothing in the service.
	ShelfOpened = store.ShelfOpened
	// ShelfReading, ShelfDone and ShelfFailed are a summary's model read
	// once a person opened it: queued or running, kept, and failed.
	ShelfReading = "reading"
	ShelfDone    = "done"
	ShelfFailed  = "failed"
)

// Shelved is one item on the shelf, as its records stand.
type Shelved struct {
	Verb string
	// Lens is the lens through which opening it reads the item, and empty
	// for a relate.
	Lens string
	// OpensHere says opening it runs its verb on the machine that opens it.
	OpensHere bool
	State     string
	Side      store.Decision
	Read      store.Decision
	Opened    *store.Decision
	ReadDone  *store.Output
	// Error is why the newest model read under the lens failed, where State
	// is failed: its output's error, else its task's.
	Error *string
}

// ShelfOf is where the item stands on the shelf, and false where it is off
// it, as store.ItemRecords.ShelfStanding decides. Its verb is its side
// decision's cause, and its lens its read decision's. An opened summary's
// state follows its model read under that lens.
func ShelfOf(r store.ItemRecords) (Shelved, bool) {
	standing := r.ShelfStanding()
	if standing == "" {
		return Shelved{}, false
	}
	side, _ := r.NewestDecision(store.GateSide)
	read, _ := r.NewestDecision(store.GateRead)
	v, _ := VerbOf(side.Cause)
	s := Shelved{Verb: side.Cause, Lens: text(read.Lens), OpensHere: v.OpensHere, Side: side, Read: read, State: standing}
	if standing == ShelfWaiting {
		return s, true
	}
	s.Opened = &read
	if s.Lens == "" {
		return s, true
	}
	var failure *store.Output
	for i, o := range r.Outputs {
		switch {
		case readDoneUnder(read, o) && (s.ReadDone == nil || o.CreatedTS > s.ReadDone.CreatedTS):
			s.ReadDone = &r.Outputs[i]
		case o.Stage == store.StageModelRead && o.State == store.OutputFailed && underLens(read, o) &&
			(failure == nil || o.CreatedTS > failure.CreatedTS):
			failure = &r.Outputs[i]
		}
	}
	open := slices.ContainsFunc(r.Tasks, func(t store.Task) bool {
		return t.Stage == store.StageModelRead && (t.State == store.TaskQueued || t.State == store.TaskLeased)
	})
	failed := slices.IndexFunc(r.Tasks, func(t store.Task) bool { return t.Stage == store.StageModelRead && t.State == store.TaskFailed })
	switch {
	case s.ReadDone != nil:
		s.State = ShelfDone
	case open:
		s.State = ShelfReading
	case failure != nil:
		s.State, s.Error = ShelfFailed, failure.Error
	case failed >= 0:
		s.State, s.Error = ShelfFailed, r.Tasks[failed].Error
	default:
		s.State = ShelfReading
	}
	return s, true
}

// NotShelvedError is an opening or removal of an item that is not on the
// shelf.
type NotShelvedError struct{ Key string }

func (e *NotShelvedError) Error() string {
	return e.Key + " is not on the shelf: a sort by " + strings.Join(ShelfVerbs(), ", ") + " puts an item there"
}

// shelved reads the item under key and where it stands on the shelf, and
// answers store.ErrNoItem and a NotShelvedError.
func shelved(ctx context.Context, tx *store.Tx, key string) (Shelved, error) {
	r, err := tx.Records(ctx, key)
	if err != nil {
		return Shelved{}, err
	}
	if r.Item.Key == "" {
		return Shelved{}, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	}
	s, ok := ShelfOf(r)
	if !ok {
		return Shelved{}, &NotShelvedError{Key: key}
	}
	return s, nil
}

// RemoveShelved takes the shelved item under key off the shelf for the person
// under login, by a read decision that keeps the newest read's verdict, lens
// and note under cause shelf-remove. So a summary's model read already queued
// runs on, and a kept summary stays in the item's outputs. A removal of an
// item that a removal already took off answers false and appends nothing. It
// answers store.ErrNoItem and a NotShelvedError.
func RemoveShelved(ctx context.Context, tx *store.Tx, key, login, clientID string) (removed bool, err error) {
	s, err := shelved(ctx, tx, key)
	var notShelved *NotShelvedError
	if errors.As(err, &notShelved) {
		read, ok, readErr := tx.NewestDecision(ctx, key, store.GateRead)
		if readErr != nil {
			return false, readErr
		}
		if ok && read.Cause == CauseShelfRemove {
			return false, nil
		}
	}
	if err != nil {
		return false, err
	}
	remove := store.Decision{
		ItemKey: key, Gate: store.GateRead, Verdict: s.Read.Verdict, Lens: s.Read.Lens, Note: s.Read.Note,
		DecidedBy: ByPerson(login), Cause: CauseShelfRemove,
	}
	if clientID != "" {
		remove.ClientID = &clientID
	}
	read, err := tx.AddDecision(ctx, remove)
	if err != nil {
		return false, err
	}
	return true, reconcileModelRead(ctx, tx, key, s.Side, read)
}

// OpenShelved opens the shelved item under key for the person under login,
// and answers where it stands after. The first opening appends the person's
// read decision as shelf-open: a read by a model under the shelf's lens for a
// summary, which queues its model read, and the shelf again for a relate,
// which runs on the person's machine. A later opening appends nothing and
// queues nothing, so a summary runs once and a second opening shows the kept
// one with no model call. A failed read runs again only through a retry of its
// task, which dispatches it under a new key. opened says whether this call
// opened it. It answers store.ErrNoItem and a NotShelvedError.
func OpenShelved(ctx context.Context, tx *store.Tx, key, login, clientID string) (s Shelved, opened bool, err error) {
	if s, err = shelved(ctx, tx, key); err != nil {
		return Shelved{}, false, err
	}
	if s.State != ShelfWaiting {
		return s, false, nil
	}
	open := store.Decision{ItemKey: key, Gate: store.GateRead, Verdict: store.VerdictShelf, DecidedBy: ByPerson(login), Cause: CauseShelfOpen}
	if clientID != "" {
		open.ClientID = &clientID
	}
	if s.Lens != "" {
		lens := s.Lens
		open.Verdict, open.Lens = connectors.ReadModel, &lens
	}
	read, err := tx.AddDecision(ctx, open)
	if err != nil {
		return Shelved{}, false, err
	}
	if err := reconcileModelRead(ctx, tx, key, s.Side, read); err != nil {
		return Shelved{}, false, err
	}
	s, err = shelved(ctx, tx, key)
	return s, err == nil, err
}
