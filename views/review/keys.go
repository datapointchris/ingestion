package review

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/keywrites"
)

// Actions is gotermboard.Keyed's: the review's keys, each acting on the item under the cursor.
// Every key is bound on every row and says on the footer why it does nothing where it cannot act.
func (b *Board) Actions() []gotermboard.Action {
	var actions []gotermboard.Action
	for n := 1; n <= 9; n++ {
		digit := gotermboard.Action{Keys: []string{strconv.Itoa(n)}, Run: b.onItem(func(m *gotermboard.Model, it item) tea.Cmd { return b.place(m, it, n, nil) })}
		if n == 1 {
			digit.Label = "place the item on the destination the strip numbers with that digit, at once, as your decision. " +
				"It takes that destination's side, or drops where the destination takes none, and leaves the queue"
			digit.Footer = "1-9 place"
		}
		actions = append(actions, digit)
	}
	return append(actions,
		gotermboard.Action{Keys: []string{"x"}, Label: "drop the item, which keeps its row, after asking what decided it: a field of its source such as its subreddit, its title, its theme, or none", Footer: "x drop", Run: b.onItem(b.drop)},
		gotermboard.Action{Keys: []string{"n"}, Label: "place the item with a note: the questions and why it was saved, which a model read answers. Asks the note, then the destination", Footer: "n note", Run: b.onItem(b.note)},
		gotermboard.Action{Keys: []string{"o"}, Label: "open the item's URL in the browser", Footer: "o open", Run: b.onItem(b.openURL)},
	)
}

// onItem runs act on the item under the cursor. A key on a group or on nothing does nothing. A key
// on an item whose write is queued, running, or landed and not yet read again refuses, because
// the item drawn is what the queue held before that write. An item whose write failed takes its
// next key.
func (b *Board) onItem(act func(*gotermboard.Model, item) tea.Cmd) func(*gotermboard.Model) tea.Cmd {
	return func(m *gotermboard.Model) tea.Cmd {
		it, listed := b.queue.Value().item(m.Cursor())
		if !listed {
			return nil
		}
		if pending, held := b.writes.Pending(it.Key); held && pending.Refusal() != "" {
			m.Say(pending.Refusal())
			return nil
		}
		return act(m, it)
	}
}

// place places it on the destination the strip numbers n, with note where one was typed.
func (b *Board) place(m *gotermboard.Model, it item, n int, note *string) tea.Cmd {
	destinations := b.queue.Value().destinations
	if n > len(destinations) {
		m.Say(fmt.Sprintf("the set in force declares %d destinations, so %d places nothing", len(destinations), n))
		return nil
	}
	key, name := it.Key, destinations[n-1].Name
	body := server.PlaceRequest{Destination: name, Note: note}
	return b.writeRow(m, key, "placing "+key+" on "+name, func() (string, error) { return b.service.Place(b.ctx, key, body) })
}

// drop asks what decided the drop, from a menu, then drops it under that cause.
func (b *Board) drop(m *gotermboard.Model, it item) tea.Cmd {
	causes := causesOf(it)
	choices := make([]gotermboard.Choice, len(causes))
	for i, c := range causes {
		choices[i] = gotermboard.Choice{Key: c.key, Label: c.label}
	}
	key := it.Key
	m.Ask(gotermboard.Question{
		Text: "drop " + outsider(key) + ", decided by its:", Choices: choices, Cancel: nothingWritten(m),
		Answer: func(picked string) tea.Cmd {
			i := slices.IndexFunc(causes, func(c cause) bool { return c.key == picked })
			body := server.DropRequest{Cause: causes[i].cause}
			return b.writeRow(m, key, "dropping "+key, func() (string, error) { return b.service.Drop(b.ctx, key, body) })
		},
	})
	return nil
}

// cause is one answer to what decided a drop: the key that picks it, what the menu says, and the
// cause the drop records, empty for none.
type cause struct{ key, label, cause string }

// causeSpelling is how the service spells a cause.
var causeSpelling = regexp.MustCompile(`^[a-z][a-z-]{0,63}$`)

// causesOf is what can have decided a drop of it: each field that its sightings carry, such as a
// subreddit, keyed by its first letter where that is free, then its title, its theme, and none.
func causesOf(it item) []cause {
	causes := []cause{{"t", "title", "title"}, {"h", "theme", "theme"}, {"n", "none, so it is kept as unstated", ""}}
	taken := map[string]bool{"t": true, "h": true, "n": true, "title": true, "theme": true}
	var fields []cause
	for _, s := range it.Sightings {
		for _, name := range sortedKeys(s.Fields) {
			spelled := strings.ReplaceAll(strings.ToLower(name), "_", "-")
			if taken[spelled] || !causeSpelling.MatchString(spelled) {
				continue
			}
			taken[spelled] = true
			key := spelled[:1]
			for digit := 1; taken[key]; digit++ {
				key = strconv.Itoa(digit)
			}
			taken[key] = true
			fields = append(fields, cause{key, outsider(spelled + ", " + s.Fields[name]), spelled})
		}
	}
	return append(fields, causes...)
}

// note asks the note, then the destination from a menu, and places it there with the note.
func (b *Board) note(m *gotermboard.Model, it item) tea.Cmd {
	destinations := b.queue.Value().destinations
	if len(destinations) == 0 {
		m.Say("the set in force declares no destination, so nothing places")
		return nil
	}
	m.Ask(gotermboard.Question{
		Text: "note on placing " + outsider(it.Key) + ", the questions and why it was saved:", Typed: true, Cancel: nothingWritten(m),
		Answer: func(note string) tea.Cmd {
			if note == "" {
				m.Say("the note was left empty, so nothing was placed")
				return nil
			}
			var choices []gotermboard.Choice
			for i, d := range destinations[:min(len(destinations), 9)] {
				choices = append(choices, gotermboard.Choice{Key: strconv.Itoa(i + 1), Label: destinationLabel(d)})
			}
			m.Ask(gotermboard.Question{
				Text: "place " + outsider(it.Key) + " with the note on:", Choices: choices, Cancel: nothingWritten(m),
				Answer: func(picked string) tea.Cmd {
					n, _ := strconv.Atoi(picked)
					return b.place(m, it, n, &note)
				},
			})
			return nil
		},
	})
	return nil
}

// openURL hands the item's URL to the browser. Only a web address opens, because the URL is the
// source's, and a stranger may have written it.
func (b *Board) openURL(m *gotermboard.Model, it item) tea.Cmd {
	if it.URL == nil || *it.URL == "" {
		m.Say(outsider(it.Key) + " has no URL")
		return nil
	}
	u, err := url.Parse(*it.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		m.Say("only an http or https address opens, and " + outsider(it.Key) + "'s is not one")
		return nil
	}
	if err := b.open(u.String()); err != nil {
		m.Say("the browser did not open: " + err.Error())
		return nil
	}
	m.Say("opened " + outsider(u.String()))
	return nil
}

// nothingWritten is every question's esc.
func nothingWritten(m *gotermboard.Model) func() tea.Cmd {
	return func() tea.Cmd {
		m.Say("nothing written")
		return nil
	}
}

// writeRow queues run on the item key names, confirmed on the screen's loop. The item leaves the
// table at once, because every decision takes it out of the queue, and the cursor goes on to the
// rest of its group below it, then above it, then the other groups. A key on the item refuses
// until a read begun after the write ended lands, unless the write failed, which puts the item
// back with its error on it.
func (b *Board) writeRow(m *gotermboard.Model, key, doing string, run func() (string, error)) tea.Cmd {
	onRow := m.Cursor() == key
	wasOpen := onRow && m.CursorOpenInPlace()
	next := b.rowsAfter(key)
	cmd := b.writes.Start(m, keywrites.Write{Doing: doing, Run: run, Row: key}, b.reread)
	m.Rebuild()
	if !onRow {
		return cmd
	}
	for _, k := range next {
		if m.MoveCursorTo(k) {
			break
		}
	}
	if wasOpen {
		m.OpenInPlace()
	}
	return cmd
}

// rowsAfter is every row the last frame drew but key's, in the order the cursor takes them once
// key's row leaves.
func (b *Board) rowsAfter(key string) []string {
	at := slices.Index(b.drawn, key)
	if at < 0 {
		return nil
	}
	in := b.drawnIn[key]
	var below, above, others []string
	for i, k := range b.drawn {
		switch {
		case i == at:
		case b.drawnIn[k] != in:
			others = append(others, k)
		case i > at:
			below = append(below, k)
		default:
			above = slices.Insert(above, 0, k)
		}
	}
	return slices.Concat(below, above, others)
}
