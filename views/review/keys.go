package review

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
)

// Actions is gotermboard.Keyed's: the review's keys, each acting on the item under the cursor.
// Every key is bound on every row and says on the footer why it does nothing where it cannot act.
func (b *Board) Actions() []gotermboard.Action {
	var actions []gotermboard.Action
	for _, v := range route.Verbs {
		key := VerbKeys[v.Verb]
		actions = append(actions, gotermboard.Action{
			Keys: []string{key}, Label: v.Verb + ", at once, as your decision: " + v.Help, Footer: key + " " + verbFooters[v.Verb], Run: b.onItem(b.sortBy(v)),
		})
	}
	return append(actions,
		gotermboard.Action{Keys: []string{KeyDrop}, Label: "drop the item, which keeps its row, after asking what decided it: its channel or another field of its source such as its subreddit, its title, its theme, its low quality, or none", Footer: KeyDrop + " drop", Run: b.onItem(b.drop)},
		gotermboard.Action{Keys: []string{KeyLowQuality}, Label: "mark the item low quality: it held nothing worth keeping. The mark teaches the quality gate and leaves the item waiting", Footer: KeyLowQuality + " low quality", Run: b.onItem(b.mark(gatetrust.MarkLowQuality))},
		gotermboard.Action{Keys: []string{KeyRead}, Label: "mark the item read: it was read and gave what was wanted. The mark teaches the quality gate and leaves the item waiting", Footer: KeyRead + " read", Run: b.onItem(b.mark(gatetrust.MarkRead))},
		gotermboard.Action{Keys: []string{KeyComments}, Label: "read what the video's top comments say about it: one model call, once per item, whose answer the card shows once it lands. A second c shows how the read stands, and asks again where the read failed", Footer: KeyComments + " comments", Run: b.onItem(b.comments)},
		gotermboard.Action{Keys: []string{"n"}, Label: "write a note on the item in $VISUAL or $EDITOR: the questions and why it was saved, which a model read answers. Then pick the destination it is placed on with the note", Footer: "n note", Run: b.onItem(b.note)},
		gotermboard.Action{Keys: []string{"o"}, Label: "open the item's URL in the browser", Footer: "o open", Run: b.onItem(b.openURL)},
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

// The keys of the sort verbs, the drop and the marks.
const (
	KeyFleetRelate        = "f"
	KeyLearning           = "l"
	KeyPersonalRelate     = "p"
	KeyPersonalSummarize  = "m"
	KeyTechnicalSummarize = "t"
	KeyWatch              = "w"
	KeyDrop               = "d"
	KeyLowQuality         = "x"
	KeyRead               = "r"
	KeyComments           = "c"
)

// VerbKeys is the key that sorts by each of route.Verbs.
var VerbKeys = map[string]string{
	route.VerbFleetRelate:        KeyFleetRelate,
	route.VerbLearning:           KeyLearning,
	route.VerbPersonalRelate:     KeyPersonalRelate,
	route.VerbPersonalSummarize:  KeyPersonalSummarize,
	route.VerbTechnicalSummarize: KeyTechnicalSummarize,
	route.VerbWatch:              KeyWatch,
}

// verbFooters is each verb as the footer names it beside its key.
var verbFooters = map[string]string{
	route.VerbFleetRelate:        "fleet",
	route.VerbLearning:           "learn",
	route.VerbPersonalRelate:     "relate",
	route.VerbPersonalSummarize:  "summarize",
	route.VerbTechnicalSummarize: "technical",
	route.VerbWatch:              "watch",
}

// sortBy sorts it by v, once the queue says the set in force takes v. A watch first asks for the
// playlist.
func (b *Board) sortBy(v route.VerbRoute) func(*gotermboard.Model, item) tea.Cmd {
	return func(m *gotermboard.Model, it item) tea.Cmd {
		h := b.queue.Value()
		if why := verbMissing(h, v.Verb); why != "" {
			m.Say(why)
			return nil
		}
		if v.Verb == route.VerbWatch {
			return b.watch(m, it, h.watchDefault)
		}
		return b.sortWrite(m, it, v.Verb, "")
	}
}

// verbMissing is why the verb sorts nothing under the queue's read, or empty where it sorts.
func verbMissing(h held, verb string) string {
	if h.verbs == nil {
		return "the queue came without its verbs, so no verb's key sorts: the service could not read its set in force, or predates the verbs"
	}
	i := slices.IndexFunc(h.verbs, func(v server.ReviewVerb) bool { return v.Verb == verb })
	switch {
	case i < 0:
		return "the service names no verb " + verb + ", so " + VerbKeys[verb] + " sorts nothing"
	case h.verbs[i].Missing != nil:
		return *h.verbs[i].Missing
	}
	return ""
}

// sortWrite sorts it by verb, and a watch into playlist, where it names one.
func (b *Board) sortWrite(m *gotermboard.Model, it item, verb, playlist string) tea.Cmd {
	key := it.Key
	body := server.SortRequest{Verb: verb, Playlist: playlist}
	body.Presented, body.DecidedAfterMS = b.presented(m, key, time.Now())
	doing := "sorting " + key + " by " + verb
	if playlist != "" {
		doing += " into " + playlist
	}
	return b.writeRow(m, key, doing, func() (string, error) { return b.service.Sort(b.ctx, key, body) })
}

// watch asks which playlist the video goes to, listing the service's default first and then each
// playlist this machine reads, numbered. Enter takes the default, a number its playlist, and any
// other text names a playlist by its title or YouTube id. The first watch begins the read, so its
// list shows on the next.
func (b *Board) watch(m *gotermboard.Model, it item, fallback *string) tea.Cmd {
	if b.wantPlaylists.CompareAndSwap(false, true) {
		b.playlists.Force()
	}
	lists := playlistOrder(b.playlists.Value(), fallback)
	var listing []gotermboard.Line
	for i, p := range lists {
		label := fmt.Sprintf("%d  %s", i+1, outsider(p.Title))
		if fallback != nil && (p.Title == *fallback || p.ID == *fallback) {
			label += "  (the default)"
		}
		listing = append(listing, gotermboard.Line{gotermboard.C(label, "")})
	}
	switch failed := b.playlistsErr.Load(); {
	case failed != nil:
		listing = append(listing, gotermboard.Line{gotermboard.C("the playlists were not read: "+(*failed).Error(), gotermboard.Yellow)})
	case len(lists) == 0 || (fallback != nil && len(lists) == 1):
		listing = append(listing, gotermboard.Line{gotermboard.C("reading the playlists; "+KeyWatch+" again lists them once read", "")})
	}
	text := "watch " + outsider(it.Key) + " in which playlist? Type its number, title or id"
	if fallback != nil {
		text += ", or enter for " + outsider(*fallback)
	}
	m.Ask(gotermboard.Question{
		Listing: listing, Text: text, Typed: true, Cancel: nothingWritten(m),
		Answer: func(typed string) tea.Cmd {
			playlist := typed
			if n, err := strconv.Atoi(typed); err == nil && n >= 1 && n <= len(lists) {
				playlist = lists[n-1].ID
			}
			if playlist == "" && fallback == nil {
				m.Say("no playlist named, and the service names no default, so nothing was sorted")
				return nil
			}
			return b.sortWrite(m, it, route.VerbWatch, playlist)
		},
	})
	return nil
}

// playlistOrder is the default first, then every other playlist by title. A default that no
// listed playlist names is listed by itself, so its number still picks it.
func playlistOrder(lists []Playlist, fallback *string) []Playlist {
	out := slices.Clone(lists)
	slices.SortStableFunc(out, func(a, b Playlist) int { return strings.Compare(strings.ToLower(a.Title), strings.ToLower(b.Title)) })
	if fallback == nil {
		return out
	}
	i := slices.IndexFunc(out, func(p Playlist) bool { return p.Title == *fallback || p.ID == *fallback })
	if i < 0 {
		return slices.Insert(out, 0, Playlist{ID: *fallback, Title: *fallback})
	}
	first := out[i]
	return slices.Insert(slices.Delete(out, i, i+1), 0, first)
}

// mark posts the person's mark on it. The item stays in the queue, since a mark decides nothing,
// and the queue is read again once the drain ends, as every write's is, because one reread serves
// every write the drain ran.
func (b *Board) mark(mark string) func(*gotermboard.Model, item) tea.Cmd {
	return func(m *gotermboard.Model, it item) tea.Cmd {
		key := it.Key
		return b.writes.Start(m, keywrites.Write{Doing: "marking " + key + " " + mark, Run: func() (string, error) {
			return b.service.Mark(b.ctx, key, mark)
		}}, b.reread)
	}
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
		m.Say("the comments of " + outsider(it.Key) + ": " + outsider(commentsWords(it.Comments)))
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
	body.Presented, body.DecidedAfterMS = b.presented(m, key, time.Now())
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
		{"l", "low quality, which holds nothing worth keeping", route.CauseLowQuality},
		{"n", "none, so it is kept as " + route.CauseUnstated, ""},
	}
	taken := map[string]bool{"t": true, "h": true, "l": true, "n": true, route.CauseTitle: true, route.CauseTheme: true, route.CauseLowQuality: true}
	var fields []cause
	if channel := channelOf(it); channel != "" && len(it.ChannelFields) > 0 {
		fields = append(fields, cause{"c", outsider(route.CauseChannel + ", " + channel), route.CauseChannel})
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
