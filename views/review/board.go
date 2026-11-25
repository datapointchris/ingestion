// Package review is the review tab of `ingestion tui`: the items that wait on a person's
// placement, each opening into its text, the quality gate's whole answer and the scan's findings,
// and placed or dropped by key through the routes `ingestion review` calls.
package review

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"time"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/markdown"

	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/keywrites"
)

// Service is the set of routes that the tab reads and writes through. cmd fills it with the calls `ingestion
// review` makes, under the same login, so a key is refused wherever that command is, and records
// the decision as the person's.
type Service interface {
	Queue(ctx context.Context) (server.ReviewAnswer, error)
	// Parse is the item's newest parse with the scan's findings, and false where it holds no done
	// parse.
	Parse(ctx context.Context, key string) (server.ParseAnswer, bool, error)
	// Place and Drop write the decision and answer the sentence `ingestion review place` and
	// `ingestion review drop` print.
	Place(ctx context.Context, key string, body server.PlaceRequest) (string, error)
	Drop(ctx context.Context, key string, body server.DropRequest) (string, error)
}

// The sources' names, as the strip and the ? overlay name them.
const (
	sourceQueue  = "review queue"
	sourceParses = "parse"
)

// The groupings v cycles, the first the one the tab opens on.
const (
	byReason = "reason"
	bySource = "source"
	byNone   = "none"
)

// held is one read of the queue, each item decoded once off the screen's loop. readFrom is when the
// read began, which settles each write that landed before it.
type held struct {
	at, readFrom time.Time
	destinations []server.ReviewDestination
	items        []item
	index        map[string]int
}

// item is one waiting item with the quality gate's answer decoded, or why it could not be.
type item struct {
	server.ReviewItem
	answer      *qualitygate.Answer
	answerFault string
}

func (h held) item(key string) (item, bool) {
	i, ok := h.index[key]
	if !ok {
		return item{}, false
	}
	return h.items[i], true
}

// parsed is one item's newest parse, or found false where it holds none.
type parsed struct {
	answer server.ParseAnswer
	found  bool
}

// Board is the review tab: a gotermboard board with keys of its own. Everything but the sources,
// the cursor and the writes is the screen loop's own.
type Board struct {
	ctx     context.Context
	service Service
	open    func(url string) error
	queue   *gotermboard.Source[held]
	// parses holds the parse of each item the cursor has rested on. Each is read off the screen's
	// loop, as the cursor lands, rather than one per row on every read of the queue.
	parses   *gotermboard.Source[map[string]parsed]
	cursor   atomic.Pointer[string]
	renderer *markdown.Renderer
	writes   keywrites.Writes
	// drawn is every row's key in the order the last frame drew them, and drawnIn the group each
	// drew under, which a decided row passes the cursor along.
	drawn   []string
	drawnIn map[string]string
}

// NewBoard declares the board's reads, reading nothing yet. open hands a URL to the browser.
func NewBoard(ctx context.Context, service Service, open func(url string) error) *Board {
	b := &Board{ctx: ctx, service: service, open: open, renderer: markdown.New(nil), drawnIn: map[string]string{}}
	b.queue = gotermboard.NewSource(sourceQueue, 30*time.Second, b.readQueue)
	b.parses = gotermboard.NewSource(sourceParses, time.Hour, b.readParse)
	return b
}

func (b *Board) readQueue(ctx context.Context) (held, string, error) {
	readFrom := time.Now()
	answer, err := b.service.Queue(ctx)
	if err != nil {
		return held{}, "", err
	}
	h := held{at: time.Now(), readFrom: readFrom, destinations: answer.Destinations, index: make(map[string]int, len(answer.Items))}
	for i, it := range answer.Items {
		decoded := item{ReviewItem: it}
		if len(it.QualityAnswer) > 0 && string(it.QualityAnswer) != "null" {
			if a, err := qualitygate.ReadAnswer(it.QualityAnswer); err != nil {
				decoded.answerFault = err.Error()
			} else {
				decoded.answer = &a
			}
		}
		h.items = append(h.items, decoded)
		h.index[it.Key] = i
	}
	return h, "", nil
}

// readParse reads the parse of the item under the cursor, keeping every parse read so far. A read
// that fails holds nothing for the item, so the source reports it and the next force reads it
// again.
func (b *Board) readParse(ctx context.Context) (map[string]parsed, string, error) {
	kept := b.parses.Value()
	at := b.cursor.Load()
	if at == nil {
		return kept, "", nil
	}
	if _, read := kept[*at]; read {
		return kept, "", nil
	}
	if _, listed := b.queue.Value().item(*at); !listed {
		return kept, "", nil
	}
	answer, found, err := b.service.Parse(ctx, *at)
	if err != nil {
		return kept, "", fmt.Errorf("the parse of %s was not read: %w", *at, err)
	}
	next := maps.Clone(kept)
	if next == nil {
		next = map[string]parsed{}
	}
	next[*at] = parsed{answer: answer, found: found}
	return next, "", nil
}

// CursorOn is gotermboard.CursorBoard's. The parse reads on the next tick, so holding a key down
// reads only the row it stops on.
func (b *Board) CursorOn(key string) {
	b.cursor.Store(&key)
	if _, read := b.parses.Value()[key]; !read {
		if _, listed := b.queue.Value().item(key); listed {
			b.parses.Force()
		}
	}
}

// Sources is gotermboard.Board's.
func (b *Board) Sources() []gotermboard.Refresher {
	return []gotermboard.Refresher{b.queue, b.parses}
}

// Groupings is gotermboard.Board's: why each item waits, which the tab opens on, its source, or
// none.
func (b *Board) Groupings() []string { return []string{byReason, bySource, byNone} }

// TwoLines is gotermboard.TwoLineBoard's: each row's key and the gate's idea sit dim under it.
func (b *Board) TwoLines() bool { return true }

// ReadAll reads the queue, then the parse of the first row that the opening grouping draws, for a frame
// printed once or a snapshot, so the printed side panel reads as a person's first screen does.
func (b *Board) ReadAll(ctx context.Context) {
	b.queue.Refresh(ctx)
	if rows := b.ordered(b.Groupings()[0]); len(rows) > 0 {
		b.CursorOn(rows[0].Key)
		b.parses.Refresh(ctx)
	}
}

// reread reads the queue now, or straight after a read already under way, which may have begun
// before the writes landed.
func (b *Board) reread() {
	b.queue.Force()
	if b.queue.Claim(time.Now()) {
		b.queue.Refresh(b.ctx)
	}
}

// AwaitWrites waits for every write running or queued to end, then answers how each ended.
// Quitting calls it, so the process never exits under a decision half sent.
func (b *Board) AwaitWrites(announce func(doing []string)) []keywrites.Ended {
	return b.writes.Await(announce)
}

// Writing says a write a key started runs or waits.
func (b *Board) Writing() bool { return b.writes.Writing() }

// sourceOf is what the SOURCE column draws for an item: the source's own field where a sighting
// carries one, as r/golang for a Reddit save's subreddit, and else the connector that brought it.
func sourceOf(it item) string {
	for _, s := range it.Sightings {
		if sub := s.Fields["subreddit"]; sub != "" {
			return "r/" + sub
		}
		for _, name := range sortedKeys(s.Fields) {
			if v := s.Fields[name]; v != "" {
				return v
			}
		}
	}
	if len(it.Connectors) > 0 {
		return strings.Join(it.Connectors, ", ")
	}
	return "—"
}
