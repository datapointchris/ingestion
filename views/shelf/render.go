package shelf

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/datapointchris/gotermboard"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const dim = "\033[2m"

// readingWidth caps the panel's text, so a wide screen keeps lines short enough to read.
const readingWidth = 100

// columns are a row's first line: the verb that shelved it, where it stands, how long ago it was
// sorted, and its title last, so the screen's edge cuts the title first.
var columns = []gotermboard.Column{
	{Title: "VERB"},
	{Title: "STATE"},
	{Title: "SORTED", Right: true},
	{Title: "TITLE"},
}

// stateCodes colors each state: what runs in yellow, what is kept in green, what failed in red.
var stateCodes = map[string]string{
	route.ShelfReading: gotermboard.Yellow,
	route.ShelfDone:    gotermboard.Green,
	route.ShelfFailed:  gotermboard.Red,
}

// Frame is gotermboard.Board's.
func (b *Board) Frame(now time.Time, grouping string) gotermboard.Frame {
	b.writes.SayEnded()
	h := b.shelf.Value()
	b.writes.Settle(h.readFrom)
	unread := b.shelf.Status().ReadAt.IsZero()
	count := "unread"
	if !unread {
		count = strconv.Itoa(len(h.items)) + " shelved"
	}
	groups := b.grouped(h.items, grouping, now)
	if len(h.items) == 0 && !unread {
		groups = append(groups, gotermboard.Group{
			Key: "empty", Title: gotermboard.Line{gotermboard.C("Nothing is on the shelf.", "")}, Still: true,
		})
	}
	return gotermboard.Frame{
		Strip: []gotermboard.Line{
			{gotermboard.C("shelf ", ""), gotermboard.C(count, gotermboard.Bold), gotermboard.C("  group ", ""), gotermboard.C(grouping, gotermboard.Bold)},
			gotermboard.ReadStatus([]gotermboard.Status{b.shelf.Status()}, now),
			b.writes.Line(now),
		},
		Columns: columns,
		Groups:  groups,
	}
}

// grouped is the items under the grouping, each group in the order of its first item.
func (b *Board) grouped(items []server.ShelfItem, grouping string, now time.Time) []gotermboard.Group {
	var groups []gotermboard.Group
	at := map[string]int{}
	for _, it := range items {
		name := "shelf"
		switch grouping {
		case GroupVerb:
			name = it.Verb
		case GroupState:
			name = it.State
		}
		i, ok := at[name]
		if !ok {
			i = len(groups)
			at[name] = i
			groups = append(groups, gotermboard.Group{Key: name, Title: gotermboard.Line{gotermboard.C(name, gotermboard.Bold)}, Counted: true, Gap: true})
		}
		groups[i].Rows = append(groups[i].Rows, b.row(it, now))
		groups[i].Count++
	}
	return groups
}

func (b *Board) row(it server.ShelfItem, now time.Time) gotermboard.Row {
	state := it.State
	if pending, held := b.writes.Pending(it.Key); held && pending.Fault == "" {
		state = "opening"
	}
	title := "—"
	if it.Title != nil && *it.Title != "" {
		title = outsider(*it.Title)
	}
	item := it
	return gotermboard.Row{
		Key: it.Key,
		Cells: []gotermboard.Cell{
			gotermboard.C(it.Verb, ""), gotermboard.C(state, stateCodes[state]), {Text: ago(it.SortedTS, now), Ticks: true}, gotermboard.C(title, ""),
		},
		Second:   gotermboard.Line{gotermboard.C(outsider(it.Key), dim)},
		Document: func(width int) []gotermboard.Line { return panel(item, min(width, readingWidth)) },
		Text:     title,
	}
}

// panel is the item's side panel: what it is, where it stands, and its summary once kept.
func panel(it server.ShelfItem, width int) []gotermboard.Line {
	var lines []gotermboard.Line
	add := func(text, code string) {
		for _, l := range wrapped(text, width) {
			lines = append(lines, gotermboard.Line{gotermboard.C(l, code)})
		}
	}
	if it.Title != nil {
		add(outsider(*it.Title), gotermboard.Bold)
	}
	if it.URL != nil {
		add(outsider(*it.URL), dim)
	}
	lines = append(lines, nil)
	how := it.Verb
	if it.Lens != nil {
		how += ", summarized under the " + *it.Lens + " lens"
	}
	add(how, "")
	add(stateWords[it.State], stateCodes[it.State])
	if it.Error != nil {
		add(outsider(*it.Error), gotermboard.Red)
	}
	if read, ok := summaryOf(it); ok {
		if read.Summary != nil {
			lines = append(lines, nil, gotermboard.Line{gotermboard.C("Summary", gotermboard.Bold)})
			add(outsider(*read.Summary), "")
		}
		if read.Suggestion != nil {
			lines = append(lines, nil, gotermboard.Line{gotermboard.C("Suggestion", gotermboard.Bold)})
			add(outsider(*read.Suggestion), "")
		}
	}
	return lines
}

// modelRead is the kept summary as the model read answers it.
type modelRead struct {
	Summary    *string `json:"summary"`
	Suggestion *string `json:"suggestion"`
}

func summaryOf(it server.ShelfItem) (modelRead, bool) {
	var read modelRead
	if len(it.Summary) == 0 || string(it.Summary) == "null" || json.Unmarshal(it.Summary, &read) != nil {
		return read, false
	}
	return read, true
}

// ago is how long before now stamp was, and a dash where it does not read.
func ago(stamp string, now time.Time) string {
	at, err := time.Parse(store.StampLayout, stamp)
	if err != nil {
		return "—"
	}
	return gotermboard.Ago(now.Sub(at))
}

// SchemaVersion is the shape `ingestion tui --view shelf --json` prints, which
// schemas/tui-shelf.schema.json holds.
const SchemaVersion = 1

// Snapshot is what --json prints: each shelved item as the service answered it, in the order the
// frame draws them under the board's default grouping, and how the read stands.
type Snapshot struct {
	SchemaVersion int                  `json:"schema_version"`
	TakenAt       time.Time            `json:"taken_at"`
	Grouping      string               `json:"grouping"`
	Items         []server.ShelfItem   `json:"items"`
	Sources       []gotermboard.Status `json:"sources"`
}

// Snapshot reads what the board holds now.
func (b *Board) Snapshot(now time.Time) Snapshot {
	grouping := b.Groupings()[0]
	snap := Snapshot{SchemaVersion: SchemaVersion, TakenAt: now.UTC(), Grouping: grouping, Items: []server.ShelfItem{}, Sources: []gotermboard.Status{b.shelf.Status()}}
	h := b.shelf.Value()
	byKey := map[string]server.ShelfItem{}
	for _, it := range h.items {
		byKey[it.Key] = it
	}
	for _, g := range b.grouped(h.items, grouping, now) {
		for _, r := range g.Rows {
			snap.Items = append(snap.Items, byKey[r.Key])
		}
	}
	return snap
}
