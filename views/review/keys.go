package review

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

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
	for _, c := range route.Choices {
		bound, ok := ChoiceBindings[c.Name]
		if !ok {
			continue
		}
		run := b.onItem(b.chooseBy(c))
		switch c.Name {
		case route.ChoiceArchive:
			run = b.onItem(b.archive)
		case route.ChoiceDrop:
			run = b.onItem(b.drop)
		}
		actions = append(actions, gotermboard.Action{
			Keys: []string{bound.Key}, Label: c.Name + ", as your decision: " + c.Help, Footer: bound.Key + " " + bound.Footer, Run: run,
		})
	}
	return append(actions,
		gotermboard.Action{Keys: []string{KeyComments}, Label: "read what the video's top comments say about it: one model call, once per item, whose answer the card shows once it lands. A second c shows how the read stands, and asks again where the read failed", Footer: KeyComments + " comments", Run: b.onItem(b.comments)},
		gotermboard.Action{Keys: []string{KeyNote}, Label: "write a note on the item in $VISUAL or $EDITOR: the questions and why it was saved, which a suggestion's model read answers. Then pick by its digit the choice that places it with the note", Footer: KeyNote + " note", Run: b.onItem(b.note)},
		gotermboard.Action{Keys: []string{"o"}, Label: "open the item's URL in the browser", Footer: "o open", Run: b.onItem(func(m *gotermboard.Model, it item) tea.Cmd { return b.choiceKeys.OpenURL(m, it.Key, it.URL) })},
		gotermboard.Action{Keys: []string{"s"}, Label: sortLabel(), Footer: "s sort", Run: func(m *gotermboard.Model) tea.Cmd {
			b.sortAt = (b.sortAt + 1) % len(sorts)
			m.Say("sorted by " + sorts[b.sortAt] + ", " + sortWords[sorts[b.sortAt]])
			m.Rebuild()
			return nil
		}},
	)
}

// sortLabel is the s key's label: each sort in the order s cycles them.
func sortLabel() string {
	words := make([]string, len(sorts))
	for i, sort := range sorts {
		words[i] = sortWords[sort]
	}
	return "next sort, in turn: " + strings.Join(words, ", then ")
}

// The keys of the drop, the comments' read and the note.
const (
	KeyDrop     = "d"
	KeyComments = "c"
	KeyNote     = "n"
)

// ChoiceBinding is the key that picks one choice, and the word the footer names it by.
type ChoiceBinding struct{ Key, Footer string }

// ChoiceBindings is the binding of each of route.Choices, which a test holds to route.Choices. Each
// placing choice and archive takes an uppercase key, so it never takes a lowercase key that
// gotermboard binds on every board. E is Gmail's archive.
var ChoiceBindings = map[string]ChoiceBinding{
	route.ChoiceArticles:   {Key: "A", Footer: "articles"},
	route.ChoiceLearning:   {Key: "L", Footer: "learning"},
	route.ChoiceSuggestion: {Key: "S", Footer: "suggestion"},
	route.ChoiceWatch:      {Key: "W", Footer: "watch"},
	route.ChoicePsyche:     {Key: "P", Footer: "psyche"},
	route.ChoiceArchive:    {Key: "E", Footer: "archive"},
	route.ChoiceDrop:       {Key: KeyDrop, Footer: "drop"},
}

// chooseBy picks it by c, once the queue says the set in force takes c.
func (b *Board) chooseBy(c route.Choice) func(*gotermboard.Model, item) tea.Cmd {
	return func(m *gotermboard.Model, it item) tea.Cmd {
		choice, why := ChoiceIn(b.queue.Value().choices, c.Name)
		if why != "" {
			m.Say(why)
			return nil
		}
		return b.pick(m, it, choice, nil, "")
	}
}

// noteEntry is one place that the note's menu numbers: a choice that places an item, or a
// destination of the set in force that no such choice reaches, which the placement names.
type noteEntry struct {
	choice      *server.ReviewChoice
	destination *server.ReviewDestination
}

// name is the entry as the menu and the snapshot name it.
func (e noteEntry) name() string {
	if e.choice != nil {
		return e.choice.Choice
	}
	return e.destination.Name
}

// noteEntries is each choice in the queue's read that places an item, in order, then each
// destination that none of them reaches, which the note's menu numbers from 1 to 9.
func noteEntries(h held) []noteEntry {
	var out []noteEntry
	reached := map[string]bool{}
	for _, c := range h.choices {
		if r, ok := route.ChoiceOf(c.Choice); ok && r.Places() {
			out = append(out, noteEntry{choice: &c})
			if c.Destination != nil {
				reached[*c.Destination] = true
			}
		}
	}
	for _, d := range h.destinations {
		if !reached[d.Name] {
			out = append(out, noteEntry{destination: &d})
		}
	}
	return out[:min(len(out), 9)]
}

// pick places it by choice, with a note where one was written, as ChoiceKeys.Pick asks and
// relates.
func (b *Board) pick(m *gotermboard.Model, it item, choice server.ReviewChoice, note *string, noteFile string) tea.Cmd {
	return b.choiceKeys.Pick(m, it.Key, choice, b.queue.Value().watchDefault, server.PlaceRequest{Note: note},
		func(body server.PlaceRequest, landed chan<- bool) tea.Cmd {
			return b.placeWrite(m, it, body, noteFile, landed)
		})
}

// placeWrite sends body, the placement of it, with the card as presented. A note's file goes once
// the placement lands, and stays, named in the error, where it does not. Where landed is not nil,
// it carries whether the placement landed, and a failure says that nothing went to psyche.
func (b *Board) placeWrite(m *gotermboard.Model, it item, body server.PlaceRequest, noteFile string, landed chan<- bool) tea.Cmd {
	key := it.Key
	body.Presented, body.DecidedAfterMS = b.presented(m, key, time.Now())
	doing := "placing " + key + " by " + body.Choice
	switch {
	case body.Destination != "":
		doing = "placing " + key + " on " + body.Destination
	case body.Playlist != "":
		doing += " into " + body.Playlist
	}
	return b.writeRow(m, key, doing, func() (string, error) {
		said, err := b.service.Place(b.ctx, key, body)
		err = Landed(landed, err)
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

// comments asks for the read of what it's top comments say, which queues it, asks a failed one
// again, or answers how it stands. A done read is said at once. The queue is read again once the
// request lands, so the card shows the read's state.
func (b *Board) comments(m *gotermboard.Model, it item) tea.Cmd {
	if it.Comments == nil {
		m.Say("no source of the service reads the comments of a " + it.MediaType + ", so " + KeyComments + " reads nothing")
		return nil
	}
	if it.Comments.State == server.CommentsDone {
		m.Say("the comments of " + Outsider(it.Key) + ": " + Outsider(commentsWords(it.Comments)))
		return nil
	}
	key := it.Key
	return b.writes.Start(m, keywrites.Write{Doing: "asking for the comments of " + key, Run: func() (string, error) {
		c, err := b.service.Comments(b.ctx, key)
		if err != nil {
			return "", err
		}
		return "the comments of " + key + ": " + commentsWords(&c), nil
	}}, b.reread)
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

// archive asks what was liked, from a menu, then archives it under that like.
func (b *Board) archive(m *gotermboard.Model, it item) tea.Cmd {
	key := it.Key
	return AskLike(m, key, channelOf(it), func(liked string) tea.Cmd {
		body := server.ArchiveRequest{Liked: liked}
		body.Presented, body.DecidedAfterMS = b.presented(m, key, time.Now())
		return b.writeRow(m, key, "archiving "+key, func() (string, error) { return b.service.Archive(b.ctx, key, body) })
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
		Text: "drop " + Outsider(key) + ": what decided it? The answer teaches the filter what to drop:", Choices: choices, Cancel: NothingWritten(m),
		Answer: func(picked string) tea.Cmd {
			i := slices.IndexFunc(causes, func(c cause) bool { return c.key == picked })
			body := server.DropRequest{Cause: causes[i].cause}
			body.Presented, body.DecidedAfterMS = b.presented(m, key, time.Now())
			return b.writeRow(m, key, "dropping "+key, func() (string, error) { return b.service.Drop(b.ctx, key, body) })
		},
	})
	return nil
}

// cause is one answer to what decided a drop: the key that picks it, what the menu says, and the
// cause the drop records, empty for none.
type cause struct{ key, label, cause string }

// causesOf is what can have decided a drop of it: its channel where its source names one that the
// drop records as channel, each other field that its sightings carry, such as a subreddit, keyed by
// its first letter where that is free, then its title, its theme, its low quality, and none. The
// fields that the channel stands for are left out, and so is a field whose name the server would
// refuse as a cause.
func causesOf(it item) []cause {
	causes := []cause{
		{"t", route.CauseTitle, route.CauseTitle},
		{"h", route.CauseTheme, route.CauseTheme},
		{"l", "low quality, which holds nothing worth keeping, and marks it low quality", route.CauseLowQuality},
		{"n", "none, so it is kept as " + route.CauseUnstated, ""},
	}
	taken := map[string]bool{"t": true, "h": true, "l": true, "n": true, route.CauseTitle: true, route.CauseTheme: true, route.CauseLowQuality: true}
	var fields []cause
	if channel := channelOf(it); channel != "" && len(it.ChannelFields) > 0 {
		fields = append(fields, cause{"c", Outsider(route.CauseChannel + ", " + channel), route.CauseChannel})
		taken["c"], taken[route.CauseChannel] = true, true
	}
	for _, s := range it.Sightings {
		for _, name := range sortedKeys(s.Fields) {
			if slices.Contains(it.ChannelFields, name) {
				continue
			}
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
			fields = append(fields, cause{key, Outsider(spelled + ", " + s.Fields[name]), spelled})
		}
	}
	return append(fields, causes...)
}

// note hands the terminal to the person's editor on a file for the note, then asks by its digit
// the choice or destination that places the item with the note. A note that is not placed stays in
// its file, and the footer names the file.
func (b *Board) note(m *gotermboard.Model, it item) tea.Cmd {
	placing := noteEntries(b.queue.Value())
	if len(placing) == 0 {
		_, why := ChoiceIn(b.queue.Value().choices, route.ChoiceArticles)
		m.Say(cmp.Or(why, "the service names no choice that places, so a note places nothing"))
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
		for i, e := range placing {
			var label string
			if e.choice != nil {
				label = choiceLabel(*e.choice, b.queue.Value().watchDefault)
			} else {
				label = destinationLabel(*e.destination)
			}
			choices = append(choices, gotermboard.Choice{Key: strconv.Itoa(i + 1), Label: label})
		}
		m.Ask(gotermboard.Question{
			Text: "place " + Outsider(it.Key) + " with the note by:", Choices: choices,
			Cancel: func() tea.Cmd {
				m.Say("nothing placed; the note is kept at " + path)
				return nil
			},
			Answer: func(picked string) tea.Cmd {
				n, _ := strconv.Atoi(picked)
				e := placing[n-1]
				switch {
				case e.destination != nil:
					return b.placeWrite(m, it, server.PlaceRequest{Destination: e.destination.Name, Note: &note}, path, nil)
				case e.choice.Missing != nil:
					m.Say(*e.choice.Missing + "; the note is kept at " + path)
					return nil
				}
				return b.pick(m, it, *e.choice, &note, path)
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
	guide := "\n# The questions this item should answer, and why it was saved. A suggestion's model read answers them.\n" +
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
