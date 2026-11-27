// Package shelf is the shelf tab of `ingestion tui`: each item that a person sorted by a shelf
// verb, where nothing runs until they open it. Opening a relate hands the terminal to `ingestion
// shelf open`, which hands the item to psyche relate on this machine. Opening a summary queues its
// model read once, and the side panel shows the summary once it is kept. An item stays on the
// shelf until the person takes it off.
package shelf

import (
	"context"
	"net/url"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
)

// Service holds each route that the tab reads and writes through. cmd fills it with the calls of
// `ingestion shelf`, under the same login, so an opening is the person's decision.
type Service interface {
	Shelf(ctx context.Context) (server.ShelfAnswer, error)
	// Open opens a summary's item and answers the sentence that says where it stands.
	Open(ctx context.Context, key string) (string, error)
	// Remove takes the item off the shelf and answers the sentence that says so.
	Remove(ctx context.Context, key string) (string, error)
	// Relate is the command that opens a relate's item on this machine, to which the tab hands
	// the terminal.
	Relate(ctx context.Context, key string) *exec.Cmd
}

// Outside is how the tab reaches the programs beside it, as the review tab's Outside is.
type Outside struct {
	Open     func(url string) error
	HandOver func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd
}

const sourceShelf = "shelf"

// The keys the tab binds. o opens the item's URL outside the program, as in every fleet TUI, so
// opening the shelved item, which runs its verb, takes a key of its own.
const (
	KeyOpen    = "a"
	KeyBrowser = "o"
	KeyRemove  = "D"
)

// The groupings v cycles: by the verb that shelved each item, by where it stands, or none.
const (
	GroupVerb  = "verb"
	GroupState = "state"
	GroupNone  = "none"
)

type held struct {
	readFrom time.Time
	items    []server.ShelfItem
}

// Board is the shelf tab.
type Board struct {
	ctx      context.Context
	service  Service
	outside  Outside
	shelf    *gotermboard.Source[held]
	shelfErr atomic.Pointer[error]
	writes   keywrites.Writes
}

// NewBoard declares the board's read, reading nothing yet.
func NewBoard(ctx context.Context, service Service, outside Outside) *Board {
	if outside.HandOver == nil {
		outside.HandOver = func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd {
			return m.HandOver(cmd, then)
		}
	}
	b := &Board{ctx: ctx, service: service, outside: outside}
	b.shelf = gotermboard.NewSource(sourceShelf, 30*time.Second, b.readShelf)
	return b
}

func (b *Board) readShelf(ctx context.Context) (held, string, error) {
	readFrom := time.Now()
	answer, err := b.service.Shelf(ctx)
	if err != nil {
		b.shelfErr.Store(&err)
		return held{}, "", err
	}
	b.shelfErr.Store(nil)
	return held{readFrom: readFrom, items: answer.Items}, "", nil
}

// Sources is gotermboard.Board's.
func (b *Board) Sources() []gotermboard.Refresher { return []gotermboard.Refresher{b.shelf} }

// Groupings is gotermboard.Board's.
func (b *Board) Groupings() []string { return []string{GroupVerb, GroupState, GroupNone} }

// TwoLines is gotermboard.TwoLineBoard's: each row's key sits dim under it.
func (b *Board) TwoLines() bool { return true }

// ReadAll reads the shelf for a frame printed once or a snapshot, and answers why it was not read.
func (b *Board) ReadAll(ctx context.Context) error {
	b.shelf.Refresh(ctx)
	if failed := b.shelfErr.Load(); failed != nil {
		return *failed
	}
	return nil
}

// AwaitWrites waits for every opening sent to end. Quitting calls it.
func (b *Board) AwaitWrites(announce func(doing []string)) []keywrites.Ended {
	return b.writes.Await(announce)
}

// reread reads the shelf now, or straight after a read already under way.
func (b *Board) reread() {
	b.shelf.Force()
	if b.shelf.Claim(time.Now()) {
		b.shelf.Refresh(b.ctx)
	}
}

// Actions is gotermboard.Keyed's.
func (b *Board) Actions() []gotermboard.Action {
	return []gotermboard.Action{
		{
			Keys: []string{KeyOpen}, Footer: KeyOpen + " open item", Run: b.onItem(b.open),
			Label: "open the item, as your decision: a relate hands the terminal to " + push.Program + " relate on this machine, and a summary " +
				"queues its model read once, which the side panel shows once kept. Opening it again runs nothing new",
		},
		{Keys: []string{KeyBrowser}, Footer: KeyBrowser + " open", Label: "open the item's URL in the browser", Run: b.onItem(b.openURL)},
		{
			Keys: []string{KeyRemove}, Footer: KeyRemove + " off the shelf", Run: b.onItem(b.remove),
			Label: "take the item off the shelf, as your decision. A summary's read already queued runs on, and the item keeps everything it holds",
		},
	}
}

// remove takes it off the shelf through the remove route, off the screen's loop.
func (b *Board) remove(m *gotermboard.Model, it server.ShelfItem) tea.Cmd {
	key := it.Key
	return b.writes.Start(m, keywrites.Write{Doing: "taking " + key + " off the shelf", Row: key, Run: func() (string, error) {
		return b.service.Remove(b.ctx, key)
	}}, b.reread)
}

func (b *Board) onItem(act func(*gotermboard.Model, server.ShelfItem) tea.Cmd) func(*gotermboard.Model) tea.Cmd {
	return func(m *gotermboard.Model) tea.Cmd {
		h := b.shelf.Value()
		i := slices.IndexFunc(h.items, func(it server.ShelfItem) bool { return it.Key == m.Cursor() })
		if i < 0 {
			return nil
		}
		if pending, held := b.writes.Pending(h.items[i].Key); held && pending.Refusal() != "" {
			m.Say(pending.Refusal())
			return nil
		}
		return act(m, h.items[i])
	}
}

// open opens it: a relate by handing the terminal to the command that relates it here, a summary
// through the open route, off the screen's loop.
func (b *Board) open(m *gotermboard.Model, it server.ShelfItem) tea.Cmd {
	key := it.Key
	if it.OpensHere {
		return b.outside.HandOver(m, b.service.Relate(b.ctx, key), func(m *gotermboard.Model, err error, stderr []byte) tea.Cmd {
			if err != nil {
				m.Say("the relate of " + outsider(key) + " failed: " + gotermboard.LastLine(stderr, err))
			} else {
				m.Say("related " + outsider(key) + " in " + push.Program)
			}
			b.reread()
			return nil
		})
	}
	return b.writes.Start(m, keywrites.Write{Doing: "opening " + key, Row: key, Run: func() (string, error) {
		return b.service.Open(b.ctx, key)
	}}, b.reread)
}

// openURL hands its URL to the browser, where it is a web address.
func (b *Board) openURL(m *gotermboard.Model, it server.ShelfItem) tea.Cmd {
	if it.URL == nil || *it.URL == "" {
		m.Say(outsider(it.Key) + " has no URL")
		return nil
	}
	u, err := url.Parse(*it.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		m.Say("only an http or https address opens, and " + outsider(it.Key) + "'s is not one")
		return nil
	}
	if err := b.outside.Open(u.String()); err != nil {
		m.Say("the browser did not open: " + err.Error())
		return nil
	}
	m.Say("opened " + outsider(u.String()))
	return nil
}

// outsider is text that a stranger may have written, with every terminal control taken out.
func outsider(text string) string { return terminalcontrol.Strip(text) }

// wrapped is text as lines of at most width runes, broken at spaces, with a blank line kept
// between paragraphs.
func wrapped(text string, width int) []string {
	var out []string
	for i, paragraph := range strings.Split(strings.TrimSpace(text), "\n\n") {
		if i > 0 {
			out = append(out, "")
		}
		line := ""
		for _, word := range strings.Fields(paragraph) {
			if line != "" && len([]rune(line))+1+len([]rune(word)) > width {
				out, line = append(out, line), ""
			}
			if line != "" {
				line += " "
			}
			line += word
		}
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// stateWords says where an item stands, for its panel.
var stateWords = map[string]string{
	route.ShelfWaiting: "nobody opened it, so nothing ran. " + KeyOpen + " opens it",
	route.ShelfOpened:  "opened: its relate ran on your machine, and " + KeyOpen + " runs it again",
	route.ShelfReading: "a model reads it under its lens. The summary shows here once kept",
	route.ShelfDone:    "its summary is kept, below",
	route.ShelfFailed:  "its model read failed. `ingestion tasks retry` runs it again",
}
