// Package history is the history tab of `ingestion tui`: every item that came through ingestion,
// newest first, with its connector, its outcome, who decided it and why, under the outcome filter
// that h and l step through. A book's chapters take their book's decisions, so the tab leaves them
// out. A choice's key places the item under the cursor and E archives it, through the routes that
// `ingestion review place` and `ingestion review archive` call. Placing an item that the gate
// dropped disagrees with the gate, as a placement from review does. Archiving it writes the read
// mark, which disagrees too. Either returns each family of the gate's answer to rung 0.
package history

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/views/review"
)

// Service is the set of routes that the tab reads and writes through. cmd fills it with the calls
// that `ingestion items list` and `ingestion review` make, under the same login, so a key is
// refused wherever those commands are, and records the decision as the person's.
type Service interface {
	review.ChoiceService
	// Items reads up to limit items, newest first, of outcome, or of every outcome where it is
	// empty, leaving out each book's chapters.
	Items(ctx context.Context, outcome string, limit int) (server.ItemsAnswer, error)
	// Queue reads the review queue, which the tab reads for the choices of the set in force and the
	// watch's default playlist alone.
	Queue(ctx context.Context) (server.ReviewAnswer, error)
	// Place and Archive write the decision and answer the sentence that `ingestion review place`
	// and `ingestion review archive` print.
	Place(ctx context.Context, key string, body server.PlaceRequest) (string, error)
	Archive(ctx context.Context, key string, body server.ArchiveRequest) (string, error)
}

// Limit is how many items one read of the history holds, the newest first.
const Limit = 500

// The sources' names, as the strip and the ? overlay name them.
const (
	sourceItems   = "items"
	sourceChoices = "choices"
)

// filters are the outcome filters that h and l step through, every outcome first.
var filters = append([]string{""}, store.Outcomes...)

// held is one read of the items under the outcome filter it was read with. readFrom is when the
// read began, which settles each write that landed before it.
type held struct {
	at, readFrom time.Time
	outcome      string
	items        []server.ItemListed
	index        map[string]int
	more         bool
}

// offered is the choices of the set in force, as the service answers them, and the watch's
// default playlist.
type offered struct {
	choices      []server.ReviewChoice
	watchDefault *string
}

func (h held) item(key string) (server.ItemListed, bool) {
	i, ok := h.index[key]
	if !ok {
		return server.ItemListed{}, false
	}
	return h.items[i], true
}

// Board is the history tab: a gotermboard board with keys of its own. Everything but the sources,
// the filter and the writes is the screen loop's own.
type Board struct {
	ctx     context.Context
	service Service
	items   *gotermboard.Source[held]
	// itemsErr is why the newest read of the items failed, and nil where it read.
	itemsErr atomic.Pointer[error]
	// choices holds the choices that each key checks before it places, read as review's are.
	choices *gotermboard.Source[offered]
	// choiceKeys asks what a choice's key needs, such as a watch's playlist, and relates a psyche
	// placement, as the review tab's keys do.
	choiceKeys *review.ChoiceKeys
	// filterAt is the outcome filter in force, as an index into filters.
	filterAt atomic.Int32
	writes   keywrites.Writes
}

// NewBoard declares the board's reads, reading nothing yet.
func NewBoard(ctx context.Context, service Service, outside review.Outside) *Board {
	b := &Board{ctx: ctx, service: service, choiceKeys: review.NewChoiceKeys(ctx, service, outside)}
	b.items = gotermboard.NewSource(sourceItems, time.Minute, b.readItems)
	b.choices = gotermboard.NewSource(sourceChoices, 10*time.Minute, b.readChoices)
	return b
}

// Outcome is the outcome filter in force, and "" for every outcome.
func (b *Board) Outcome() string { return filters[b.filterAt.Load()] }

// ShowOutcome puts the filter named outcome in force, "" for every outcome, and reads the items
// again under it. It answers false where outcome is none of store.Outcomes.
func (b *Board) ShowOutcome(outcome string) bool {
	at := slices.Index(filters, outcome)
	if at < 0 {
		return false
	}
	b.filterAt.Store(int32(at))
	b.items.Force()
	return true
}

func (b *Board) readItems(ctx context.Context) (held, string, error) {
	readFrom, outcome := time.Now(), b.Outcome()
	answer, err := b.service.Items(ctx, outcome, Limit)
	if err != nil {
		b.itemsErr.Store(&err)
		return b.items.Value(), "", err
	}
	b.itemsErr.Store(nil)
	h := held{at: time.Now(), readFrom: readFrom, outcome: outcome, items: answer.Items, index: make(map[string]int, len(answer.Items)), more: answer.Next != nil}
	for i, it := range answer.Items {
		h.index[it.Key] = i
	}
	return h, "", nil
}

// readChoices reads the choices of the set in force from the review queue's answer.
func (b *Board) readChoices(ctx context.Context) (offered, string, error) {
	answer, err := b.service.Queue(ctx)
	if err != nil {
		return b.choices.Value(), "", err
	}
	return offered{choices: answer.Choices, watchDefault: answer.WatchDefaultPlaylist}, "", nil
}

// Sources is gotermboard.Board's.
func (b *Board) Sources() []gotermboard.Refresher {
	return []gotermboard.Refresher{b.items, b.choices, b.choiceKeys.Playlists()}
}

// The groupings that v cycles, the first the default: none, newest first, then by outcome, by
// connector and by who decided.
const (
	GroupNone      = "none"
	GroupOutcome   = "outcome"
	GroupConnector = "connector"
	GroupDecider   = "decider"
)

// Groupings is gotermboard.Board's.
func (b *Board) Groupings() []string {
	return []string{GroupNone, GroupOutcome, GroupConnector, GroupDecider}
}

// TwoLines is gotermboard.TwoLineBoard's: each row's key and who decided sit dim under it.
func (b *Board) TwoLines() bool { return true }

// ReadAll reads the choices, then the items under the filter in force, for a frame printed once
// or a snapshot. It answers why the items were not read, so a printed frame of items never read fails as `ingestion
// items list` does.
func (b *Board) ReadAll(ctx context.Context) error {
	b.choices.Refresh(ctx)
	b.items.Refresh(ctx)
	if failed := b.itemsErr.Load(); failed != nil {
		return *failed
	}
	return nil
}

// reread reads the items now, or straight after a read already under way, which may have begun
// before the writes landed.
func (b *Board) reread() {
	b.items.Force()
	if b.items.Claim(time.Now()) {
		b.items.Refresh(b.ctx)
	}
}

// AwaitWrites waits for every write running or queued to end, then answers how each ended.
// Quitting calls it, so the process never exits under a decision half sent.
func (b *Board) AwaitWrites(announce func(doing []string)) []keywrites.Ended {
	return b.writes.Await(announce)
}

// Writing says a write a key started runs or waits.
func (b *Board) Writing() bool { return b.writes.Writing() }
