package review

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
)

// Actions is gotermboard.Keyed's: the review's keys, each acting on the item under the cursor.
// Every key is bound on every row and says on the footer why it does nothing where it cannot act.
func (b *Board) Actions() []gotermboard.Action {
	var actions []gotermboard.Action
	for n := 1; n <= 9; n++ {
		digit := gotermboard.Action{Keys: []string{strconv.Itoa(n)}, Run: b.onItem(func(m *gotermboard.Model, it item) tea.Cmd { return b.place(m, it, n, nil, "") })}
		if n == 1 {
			digit.Label = "place the item on the destination the strip numbers with that digit, at once, as your decision. " +
				"It takes that destination's side, or drops where the destination takes none, and leaves the queue"
			digit.Footer = "1-9 place"
		}
		actions = append(actions, digit)
	}
	return append(actions,
		gotermboard.Action{Keys: []string{"x"}, Label: "drop the item, which keeps its row, after asking what decided it: a field of its source such as its subreddit, its title, its theme, or none", Footer: "x drop", Run: b.onItem(b.drop)},
		gotermboard.Action{Keys: []string{"n"}, Label: "write a note on the item in $VISUAL or $EDITOR: the questions and why it was saved, which a model read answers. Then pick the destination it is placed on with the note", Footer: "n note", Run: b.onItem(b.note)},
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

// place places it on the destination numbered n, with a note where one was written. A note's file
// goes once the placement lands, and stays, named in the error, where it does not.
func (b *Board) place(m *gotermboard.Model, it item, n int, note *string, noteFile string) tea.Cmd {
	destinations := b.queue.Value().destinations
	if missing := destinationsMissing(destinations); missing != "" {
		m.Say(missing)
		return nil
	}
	if n > len(destinations) {
		m.Say(fmt.Sprintf("the set in force declares %d destinations, so %d places nothing", len(destinations), n))
		return nil
	}
	key, name := it.Key, destinations[n-1].Name
	body := server.PlaceRequest{Destination: name, Note: note}
	return b.writeRow(m, key, "placing "+key+" on "+name, func() (string, error) {
		said, err := b.service.Place(b.ctx, key, body)
		if noteFile == "" {
			return said, err
		}
		if err != nil {
			return "", fmt.Errorf("%w; the note is kept at %s", err, noteFile)
		}
		_ = os.Remove(noteFile)
		return said, nil
	})
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

// causesOf is what can have decided a drop of it: each field that its sightings carry, such as a
// subreddit, keyed by its first letter where that is free, then its title, its theme, and none. A
// field whose name the server would refuse as a cause is left out.
func causesOf(it item) []cause {
	causes := []cause{{"t", route.CauseTitle, route.CauseTitle}, {"h", route.CauseTheme, route.CauseTheme}, {"n", "none, so it is kept as " + route.CauseUnstated, ""}}
	taken := map[string]bool{"t": true, "h": true, "n": true, route.CauseTitle: true, route.CauseTheme: true}
	var fields []cause
	for _, s := range it.Sightings {
		for _, name := range sortedKeys(s.Fields) {
			spelled := strings.ReplaceAll(strings.ToLower(name), "_", "-")
			if taken[spelled] || !server.ValidCause(spelled) {
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

// note hands the terminal to the person's editor on a file for the note, then asks the destination
// from a menu and places the item there with the note. A note that is not placed stays in its
// file, and the footer names the file.
func (b *Board) note(m *gotermboard.Model, it item) tea.Cmd {
	destinations := b.queue.Value().destinations
	if missing := destinationsMissing(destinations); missing != "" {
		m.Say(missing)
		return nil
	}
	path, err := noteFile(it)
	if err != nil {
		m.Say("the note did not open for writing: " + err.Error())
		return nil
	}
	editor := gotermboard.EditorCommand(context.Background(), path)
	return b.outside.HandOver(m, editor, func(m *gotermboard.Model, err error, stderr []byte) tea.Cmd {
		if err != nil {
			m.Say("the editor failed: " + gotermboard.LastLine(stderr, err) + "; the note is kept at " + path)
			return nil
		}
		written, err := os.ReadFile(path)
		if err != nil {
			m.Say("the note was not read back: " + err.Error())
			return nil
		}
		note := noteText(string(written))
		if note == "" {
			_ = os.Remove(path)
			m.Say("the note was left empty, so nothing was placed")
			return nil
		}
		var choices []gotermboard.Choice
		for i, d := range destinations[:min(len(destinations), 9)] {
			choices = append(choices, gotermboard.Choice{Key: strconv.Itoa(i + 1), Label: destinationLabel(d)})
		}
		m.Ask(gotermboard.Question{
			Text: "place " + outsider(it.Key) + " with the note on:", Choices: choices,
			Cancel: func() tea.Cmd {
				m.Say("nothing placed; the note is kept at " + path)
				return nil
			},
			Answer: func(picked string) tea.Cmd {
				n, _ := strconv.Atoi(picked)
				return b.place(m, it, n, &note, path)
			},
		})
		return nil
	})
}

// noteFile writes a file for the note on it, opening on # lines that say what the note is for and
// name the item.
func noteFile(it item) (string, error) {
	f, err := os.CreateTemp("", "ingestion-note-*.md")
	if err != nil {
		return "", err
	}
	guide := "\n# The questions this item should answer, and why it was saved. A model read answers them.\n" +
		"# Lines opening with # are dropped, and an empty note places nothing.\n" +
		"# " + strings.Join(strings.Fields(terminalcontrol.Strip(it.Key+" · "+orDash(it.Title))), " ") + "\n"
	_, err = f.WriteString(guide)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// noteText is what the editor saved, less every # line.
func noteText(written string) string {
	var kept []string
	for _, line := range strings.Split(written, "\n") {
		if !strings.HasPrefix(line, "#") {
			kept = append(kept, line)
		}
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
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
	if err := b.outside.Open(u.String()); err != nil {
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
