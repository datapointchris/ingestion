package review

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const dim = "\033[2m"

// columns are a row's first line: the channel, the title, why it waits, the gate's verdict,
// quality, fit and side guess, the source's length, views and likes per view, and how long ago the
// item was made at its source. The title is the cut column, so a narrow screen cuts it before the
// numbers after it. The item's key and the gate's idea sit dim on the line under it.
var columns = []gotermboard.Column{
	{Title: "CHANNEL"},
	{Title: "TITLE", Cut: true},
	{Title: "WHY"},
	{Title: "GATE"},
	{Title: "Q", Right: true},
	{Title: "FIT", Right: true},
	{Title: "SIDE"},
	{Title: "LEN", Right: true},
	{Title: "VIEWS", Right: true},
	{Title: "L/V", Right: true},
	{Title: "AGE", Right: true},
}

// Frame is gotermboard.Board's, one frame for a live table and --once alike.
func (b *Board) Frame(now time.Time, grouping string) gotermboard.Frame {
	b.writes.SayEnded()
	h := b.queue.Value()
	unread := b.queue.Status().ReadAt.IsZero()
	items := sorted(b.shown(h), sorts[b.sortAt])
	b.grouping, b.ranked = grouping, b.ranked[:0]
	for _, it := range items {
		b.ranked = append(b.ranked, it.Key)
	}
	groups := b.grouped(items, grouping, now)
	b.drawn = b.drawn[:0]
	clear(b.drawnIn)
	for _, g := range groups {
		for _, r := range g.Rows {
			b.drawn = append(b.drawn, r.Key)
			b.drawnIn[r.Key] = g.Key
		}
	}
	if len(b.drawn) == 0 && !unread {
		groups = append(groups, gotermboard.Group{
			Key: "empty", Title: gotermboard.Line{gotermboard.C("Nothing waits on a person.", "")}, Still: true,
		})
	}
	return gotermboard.Frame{
		Strip: []gotermboard.Line{
			b.header(h, grouping, unread),
			gotermboard.ReadStatus(b.statuses(), now),
			b.writes.Line(now),
		},
		Charts:  func(width, _ int) []gotermboard.Line { return choiceBar(h, unread, width) },
		Columns: columns,
		Groups:  groups,
	}
}

// header counts what waits and names the grouping.
func (b *Board) header(h held, grouping string, unread bool) gotermboard.Line {
	waiting := "unread"
	if !unread {
		waiting = strconv.Itoa(len(b.shown(h))) + " waiting"
	}
	sort := sorts[b.sortAt]
	return gotermboard.Line{
		gotermboard.C("review ", ""), gotermboard.C(waiting, gotermboard.Bold), gotermboard.C("  group ", ""), gotermboard.C(grouping, gotermboard.Bold),
		gotermboard.C("  sort ", ""), gotermboard.C(sort, gotermboard.Bold), gotermboard.C(", "+sortWords[sort], ""),
	}
}

// choiceBar is a line for each choice: its key, where it sends an item and what it does, then the
// line that names what each digit of the note's menu places, each cut at the screen's width. A
// choice's key places at once, so every key stays on screen. A choice that the set in force gives
// no destination is drawn in yellow, and the queue's choices missing say why none places.
func choiceBar(h held, unread bool, width int) []gotermboard.Line {
	if unread {
		return nil
	}
	if h.choices == nil {
		_, why := queuedChoice(h, route.ChoiceArticles)
		return []gotermboard.Line{{gotermboard.C(cut(why, width), gotermboard.Yellow)}}
	}
	var lines []gotermboard.Line
	for _, c := range h.choices {
		bound, ok := ChoiceBindings[c.Choice]
		if !ok {
			continue
		}
		label, color := " "+choiceLabel(c, h.watchDefault), ""
		if c.Missing != nil {
			color = gotermboard.Yellow
		}
		help := ""
		if r, ok := route.ChoiceOf(c.Choice); ok {
			help = "  " + r.Help
		}
		used := utf8.RuneCountInString(bound.Key + label)
		lines = append(lines, gotermboard.Line{
			gotermboard.C(bound.Key, gotermboard.Bold), gotermboard.C(cut(label, width-1), color), gotermboard.C(cut(help, width-used), dim),
		})
	}
	digits := KeyNote + " note, then by its digit:"
	for i, e := range noteEntries(h) {
		digits += fmt.Sprintf("  %d %s", i+1, e.name())
	}
	return append(lines, gotermboard.Line{gotermboard.C(cut(digits, width), "")})
}

// cut is text cut to at most width runes.
func cut(text string, width int) string {
	if width <= 0 {
		return ""
	}
	if runes := []rune(text); len(runes) > width {
		return string(runes[:width])
	}
	return text
}

// choiceLabel is a choice with where it sends an item, as `learning → learning`, archive and drop
// bare, and the one that picks a playlist with the playlist that enter takes.
func choiceLabel(c server.ReviewChoice, watchDefault *string) string {
	switch {
	case c.Missing != nil:
		return c.Choice + " → no destination"
	case c.Destination == nil:
		return c.Choice
	case c.Playlist && watchDefault != nil:
		return c.Choice + " → " + *c.Destination + ", " + outsider(*watchDefault) + " unless picked"
	}
	return c.Choice + " → " + *c.Destination
}

// destinationLabel is a destination with the side it gives, as `reading-list → personal`.
func destinationLabel(d server.ReviewDestination) string {
	if d.Side == nil {
		return d.Name + " → drops"
	}
	return d.Name + " → " + *d.Side
}

// statuses are the reads the strip names: the queue and the parses, and the playlists once a
// watch asked for them.
func (b *Board) statuses() []gotermboard.Status {
	out := []gotermboard.Status{b.queue.Status(), b.parses.Status()}
	if b.wantPlaylists.Load() {
		out = append(out, b.playlists.Status())
	}
	return out
}

// shown is the queue less each item a write is pending on, since every decision takes the item out
// of the queue. A read begun after a write landed settles its item first. An item whose write
// failed stays, with its error on it.
func (b *Board) shown(h held) []item {
	b.writes.Settle(h.readFrom)
	pending := b.writes.PendingRows()
	if len(pending) == 0 {
		return h.items
	}
	var kept []item
	for _, it := range h.items {
		if p, held := pending[it.Key]; held && p.Fault == "" {
			continue
		}
		kept = append(kept, it)
	}
	return kept
}

// grouped is the items as the grouping draws them, each group in the order of items, which the
// sort in force orders. A channel or connector group with more items leads. Channel groups any
// grouping that the board does not name.
func (b *Board) grouped(items []item, grouping string, now time.Time) []gotermboard.Group {
	unread := b.queue.Status().ReadAt.IsZero()
	switch grouping {
	case store.GroupByConnector:
		return b.groupedBy(items, "connector", connectorsOf, now, unread)
	case store.GroupByNone:
		return b.oneList(items, now)
	}
	return b.groupedBy(items, "channel", func(it item) string { return cmp.Or(channelOf(it), "no channel") }, now, unread)
}

// oneList is every item as one list. Its group draws no header of its own: the first column's
// title sits where a group's name would, beside the other columns' titles, and stays on screen as
// the table scrolls.
func (b *Board) oneList(items []item, now time.Time) []gotermboard.Group {
	if len(items) == 0 {
		return nil
	}
	g := gotermboard.Group{Key: "all", Title: gotermboard.Line{gotermboard.C(columns[0].Title, gotermboard.Bold)}, Still: true, Heading: true}
	for _, it := range items {
		g.Rows = append(g.Rows, b.row(it, now))
	}
	return []gotermboard.Group{g}
}

// groupedBy is a group for each name that nameOf gives an item, the name with the most items first.
func (b *Board) groupedBy(items []item, kind string, nameOf func(item) string, now time.Time, unread bool) []gotermboard.Group {
	counts := map[string]int{}
	for _, it := range items {
		counts[nameOf(it)]++
	}
	names := slices.SortedFunc(maps.Keys(counts), func(a, z string) int { return cmp.Or(counts[z]-counts[a], strings.Compare(a, z)) })
	var groups []gotermboard.Group
	for _, name := range names {
		groups = b.appendGroup(groups, kind+"/"+name, name, items, func(it item) bool { return nameOf(it) == name }, now, unread)
	}
	return groups
}

// appendGroup appends a group of the items keep holds, and nothing where it holds none.
func (b *Board) appendGroup(groups []gotermboard.Group, key, title string, items []item, keep func(item) bool, now time.Time, unread bool) []gotermboard.Group {
	g := gotermboard.Group{Key: key, Title: gotermboard.Line{gotermboard.C(terminalcontrol.Strip(title), gotermboard.Bold)}, Counted: !unread}
	for _, it := range items {
		if keep(it) {
			g.Rows = append(g.Rows, b.row(it, now))
		}
	}
	if len(g.Rows) == 0 {
		return groups
	}
	g.Count = len(g.Rows)
	return append(groups, g)
}

// row is one waiting item: its channel, its title, why it waits, the gate's verdict, quality, fit
// and side guess with their confidences, the source's length, views and likes per view, and how
// long ago it was made at its source. Dim under it are its key, its connectors and the gate's idea.
// A failed write puts its error there in red instead.
func (b *Board) row(it item, now time.Time) gotermboard.Row {
	key := it.Key
	verdict, quality, side := gateCells(it)
	fit, lengthCell, views, likeRatio := dash(), dash(), dash(), dash()
	if it.answer != nil && it.answer.Fit != nil {
		fit = gotermboard.C(strconv.Itoa(*it.answer.Fit), "")
	}
	if it.LengthS != nil {
		lengthCell = gotermboard.C(length(*it.LengthS), "")
	}
	if it.Views != nil {
		views = gotermboard.C(count(*it.Views), "")
	}
	if ratio, ok := likesPerView(it); ok {
		likeRatio = gotermboard.C(percent(ratio), "")
	}
	age := dash()
	if it.SourceTS != nil {
		age = gotermboard.Ticking(gotermboard.Ago(now.Sub(made(it))), "")
	}
	under := gotermboard.C(underLine(it), dim)
	title := gotermboard.Line{gotermboard.C(outsider(orDash(it.Title)), "")}
	if p, held := b.writes.Pending(key); held && p.Fault != "" {
		title = append(gotermboard.Line{gotermboard.C("write failed  ", gotermboard.Red)}, title...)
		under = gotermboard.C(strings.ReplaceAll(strings.TrimSpace(p.Fault), "\n", "; "), gotermboard.Red)
	}
	return gotermboard.Row{
		Key: key,
		Cells: []gotermboard.Cell{
			gotermboard.C(outsider(sourceOf(it)), ""), gotermboard.C(it.Reason, ""), verdict, quality, fit, side,
			lengthCell, views, likeRatio, age,
		},
		Cut:         title,
		Under:       under,
		Text:        filterText(it),
		Document:    func(width int) []gotermboard.Line { return b.renderer.Lines(b.document(key), min(width, readingWidth)) },
		ReaderTitle: outsider(key),
	}
}

// readingWidth caps the reading pane, so prose keeps a readable measure on a wide screen.
const readingWidth = 110

// gateCells are the gate's verdict and its confidence, the item's quality from 1 to 10, and the
// side guess and its confidence. A drop is red, an unsure verdict yellow and a pass green. With no
// answer, each is a dash, and the side falls back on the guess that the queue carries.
func gateCells(it item) (verdict, quality, side gotermboard.Cell) {
	verdict, quality, side = gotermboard.C("—", dim), gotermboard.C("—", dim), gotermboard.C("—", dim)
	if a := it.answer; a != nil {
		code := gotermboard.Yellow
		switch a.Verdict {
		case "drop":
			code = gotermboard.Red
		case "pass":
			code = gotermboard.Green
		}
		verdict = gotermboard.C(a.Verdict+" "+confidence(a.VerdictConfidence), code)
		quality = gotermboard.C(strconv.Itoa(a.Quality), "")
		side = gotermboard.C(a.Side+" "+confidence(a.SideConfidence), "")
		return verdict, quality, side
	}
	if it.answerFault != "" {
		verdict = gotermboard.C("unreadable", gotermboard.Red)
	}
	if g := it.Guess; g != nil {
		side = gotermboard.C(g.Side+" "+confidence(g.Confidence), "")
	}
	return verdict, quality, side
}

func dash() gotermboard.Cell { return gotermboard.C("—", dim) }

// confidence is a confidence as two decimals, as `review list` prints a guess's.
func confidence(c float64) string { return fmt.Sprintf("%.2f", c) }

// underLine is the item's key, its connectors and the gate's idea.
func underLine(it item) string {
	parts := []string{it.Key}
	if len(it.Connectors) > 0 {
		parts = append(parts, strings.Join(it.Connectors, ", "))
	}
	if it.answer != nil && it.answer.Idea != "" {
		parts = append(parts, it.answer.Idea)
	}
	return outsider(strings.Join(parts, " · "))
}

// filterText is what the filter matches beyond the row: the URL, the gate's idea, genre, domains
// and themes, and the sightings' fields and tags.
func filterText(it item) string {
	var words []string
	if it.URL != nil {
		words = append(words, *it.URL)
	}
	if a := it.answer; a != nil {
		words = append(words, a.Idea, a.Genre)
		words = append(words, a.Domains...)
		words = append(words, a.Themes...)
	}
	for _, s := range it.Sightings {
		words = append(words, s.Tags...)
		for _, name := range sortedKeys(s.Fields) {
			words = append(words, name, s.Fields[name])
		}
	}
	return outsider(strings.Join(words, " "))
}

// since is when the item began to wait.
func since(it item) time.Time {
	at, err := time.Parse(store.StampLayout, it.WaitingSinceTS)
	if err != nil {
		return time.Now()
	}
	return at
}

// outsider is text that a stranger may have written, with every terminal control taken out, so a
// title, a body or a comment cannot drive the terminal it is drawn on.
func outsider(text string) string { return terminalcontrol.Strip(text) }

func orDash(s *string) string {
	if s == nil || *s == "" {
		return "—"
	}
	return *s
}

func sortedKeys(m map[string]string) []string { return slices.Sorted(maps.Keys(m)) }

// SchemaVersion is the shape `ingestion tui --view review --json` prints, which
// schemas/tui-review.schema.json holds.
const SchemaVersion = 4

// Snapshot is what --json prints: each waiting item ordered by the board's default grouping and,
// within each group, by the sort in force, beside the group it draws under, the source the row
// names and its rank under the sort, then the key that picks each choice, the digit that the
// note's menu places with by each placing choice or destination, and how each read stands. A
// display left grouped otherwise does not change it.
type Snapshot struct {
	SchemaVersion        int                  `json:"schema_version"`
	TakenAt              time.Time            `json:"taken_at"`
	Grouping             string               `json:"grouping"`
	Sort                 string               `json:"sort"`
	Choices              []ChoiceKey          `json:"choices"`
	WatchDefaultPlaylist *string              `json:"watch_default_playlist"`
	NoteChoices          []NoteChoiceKey      `json:"note_choices"`
	Items                []ItemShown          `json:"items"`
	Sources              []gotermboard.Status `json:"sources"`
	Unread               []string             `json:"unread_sources,omitempty"`
}

// ChoiceKey is the key that picks a choice, and where the choice sends an item.
type ChoiceKey struct {
	Key string `json:"key"`
	server.ReviewChoice
}

// NoteChoiceKey is the digit that the note's menu places with, by a choice or, where no choice
// reaches it, on a destination by name. One of Choice and Destination is set.
type NoteChoiceKey struct {
	Key         string  `json:"key"`
	Choice      *string `json:"choice"`
	Destination *string `json:"destination"`
}

// ItemShown is one row: the queue's item as the service answered it, the group it draws under, and
// the source its first column names.
type ItemShown struct {
	Group  string            `json:"group"`
	Source string            `json:"source"`
	Rank   int               `json:"rank"`
	Item   server.ReviewItem `json:"item"`
}

// Snapshot reads what the board holds now.
func (b *Board) Snapshot(now time.Time) Snapshot {
	h := b.queue.Value()
	grouping := b.Groupings()[0]
	sort := sorts[b.sortAt]
	snap := Snapshot{
		SchemaVersion: SchemaVersion, TakenAt: now.UTC(), Grouping: grouping, Sort: sort, WatchDefaultPlaylist: h.watchDefault,
		Items: []ItemShown{}, Sources: b.statuses(),
	}
	if h.choices != nil {
		snap.Choices, snap.NoteChoices = []ChoiceKey{}, []NoteChoiceKey{}
	}
	for _, c := range h.choices {
		if bound, ok := ChoiceBindings[c.Choice]; ok {
			snap.Choices = append(snap.Choices, ChoiceKey{Key: bound.Key, ReviewChoice: c})
		}
	}
	for i, e := range noteEntries(h) {
		shown := NoteChoiceKey{Key: strconv.Itoa(i + 1)}
		if e.choice != nil {
			shown.Choice = &e.choice.Choice
		} else {
			shown.Destination = &e.destination.Name
		}
		snap.NoteChoices = append(snap.NoteChoices, shown)
	}
	for _, s := range snap.Sources {
		if s.ReadAt.IsZero() && s.Name == sourceQueue {
			snap.Unread = append(snap.Unread, s.Name)
		}
	}
	items := sorted(b.shown(h), sort)
	rank := map[string]int{}
	for i, it := range items {
		rank[it.Key] = i + 1
	}
	for _, g := range b.grouped(items, grouping, now) {
		for _, r := range g.Rows {
			it, _ := h.item(r.Key)
			snap.Items = append(snap.Items, ItemShown{Group: g.Key, Source: sourceOf(it), Rank: rank[r.Key], Item: it.ReviewItem})
		}
	}
	return snap
}

// ordered is every row the grouping draws under the sort in force, in order.
func (b *Board) ordered(grouping string) []gotermboard.Row {
	var rows []gotermboard.Row
	for _, g := range b.grouped(sorted(b.shown(b.queue.Value()), sorts[b.sortAt]), grouping, time.Now()) {
		rows = append(rows, g.Rows...)
	}
	return rows
}
