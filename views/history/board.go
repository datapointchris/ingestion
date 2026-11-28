// Package history is the history tab of `ingestion tui`: every item that came through ingestion,
// newest first, with its connector, its outcome, who decided it and why, under the outcome filter
// that l cycles. A choice's key places the item under the cursor and E archives it, through the
// routes that `ingestion review place` and `ingestion review archive` call. So placing or
// archiving an item that the gate dropped is the person's word against the gate, which gate
// trust takes as it takes a placement from review.
package history

import (
	"context"
	"fmt"
	"os/exec"
	"slices"
	"sync/atomic"
	"time"

	tea "charm.land/bubbletea/v2"
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
	// Items reads up to limit items, newest first, of outcome, or of every outcome where it is
	// empty.
	Items(ctx context.Context, outcome string, limit int) (server.ItemsAnswer, error)
	// Place and Archive write the decision and answer the sentence that `ingestion review place`
	// and `ingestion review archive` print.
	Place(ctx context.Context, key string, body server.PlaceRequest) (string, error)
	Archive(ctx context.Context, key string, body server.ArchiveRequest) (string, error)
	// Relate is the command that hands the item to psyche relate on this machine, which the psyche
	// choice hands the terminal to.
	Relate(ctx context.Context, key string) *exec.Cmd
	// Playlists lists the playlists that a watch can add a video to, as this machine reads them.
	Playlists(ctx context.Context) ([]review.Playlist, error)
}

// Outside is how the tab reaches the programs beside it. Open hands a URL to the browser, and
// HandOver gives the terminal to psyche relate, as gotermboard.Model.HandOver does where it is nil.
type Outside struct {
	Open     func(url string) error
	HandOver func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd
}

// Limit is how many items one read of the history holds, the newest first.
const Limit = 500

// The sources' names, as the strip and the ? overlay name them.
const (
	sourceItems     = "items"
	sourcePlaylists = "playlists"
)

// filters are the outcome filters that l cycles, every outcome first.
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
	outside Outside
	items   *gotermboard.Source[held]
	// itemsErr is why the newest read of the items failed, and nil where it read.
	itemsErr atomic.Pointer[error]
	// playlists holds the playlists that a watch picks from, read once the first watch asks for
	// them. playlistsErr is why the newest read failed.
	playlists     *gotermboard.Source[[]review.Playlist]
	wantPlaylists atomic.Bool
	playlistsErr  atomic.Pointer[error]
	// filterAt is the outcome filter in force, as an index into filters.
	filterAt atomic.Int32
	writes   keywrites.Writes
}

// NewBoard declares the board's reads, reading nothing yet.
func NewBoard(ctx context.Context, service Service, outside Outside) *Board {
	if outside.HandOver == nil {
		outside.HandOver = func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd {
			return m.HandOver(cmd, then)
		}
	}
	b := &Board{ctx: ctx, service: service, outside: outside}
	b.items = gotermboard.NewSource(sourceItems, time.Minute, b.readItems)
	b.playlists = gotermboard.NewSource(sourcePlaylists, time.Hour, b.readPlaylists)
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

// readPlaylists reads the playlists once a watch asked for them, and holds none before.
func (b *Board) readPlaylists(ctx context.Context) ([]review.Playlist, string, error) {
	if !b.wantPlaylists.Load() {
		return b.playlists.Value(), "", nil
	}
	lists, err := b.service.Playlists(ctx)
	if err != nil {
		b.playlistsErr.Store(&err)
		return b.playlists.Value(), "", fmt.Errorf("the playlists were not read: %w", err)
	}
	b.playlistsErr.Store(nil)
	return lists, "", nil
}

// Sources is gotermboard.Board's.
func (b *Board) Sources() []gotermboard.Refresher {
	return []gotermboard.Refresher{b.items, b.playlists}
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

// ReadAll reads the items under the filter in force, for a frame printed once or a snapshot. It
// answers why they were not read, so a printed frame of items never read fails as `ingestion
// items list` does.
func (b *Board) ReadAll(ctx context.Context) error {
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
