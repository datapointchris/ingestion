package history

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const dim = "\033[2m"

// columns are a row's first line: its outcome, where it went, who decided and why, its
// connectors, its phase, and how long ago it landed. The title goes last, so the screen's edge
// cuts it rather than the outcome. The item's key and what decided it sit dim under it.
var columns = []gotermboard.Column{
	{Title: "OUTCOME"},
	{Title: "WHERE"},
	{Title: "BY"},
	{Title: "WHY"},
	{Title: "CONNECTOR", Max: 28},
	{Title: "PHASE"},
	{Title: "LANDED", Right: true},
	{Title: "TITLE"},
}

// outcomeCodes color each outcome: a placement green, a drop red, a wait yellow.
var outcomeCodes = map[string]string{
	store.OutcomePlaced:   gotermboard.Green,
	store.OutcomeArchived: gotermboard.Cyan,
	store.OutcomeDropped:  gotermboard.Red,
	store.OutcomeWaiting:  gotermboard.Yellow,
	store.OutcomeSplit:    gotermboard.Blue,
}

// Frame is gotermboard.Board's, one frame for a live table and --once alike.
func (b *Board) Frame(now time.Time, grouping string) gotermboard.Frame {
	b.writes.SayEnded()
	h := b.items.Value()
	unread := b.items.Status().ReadAt.IsZero()
	items := b.shown(h)
	groups := b.grouped(items, grouping, now, unread)
	if len(items) == 0 && !unread {
		groups = append(groups, gotermboard.Group{
			Key: "empty", Title: gotermboard.Line{gotermboard.C("No item has this outcome.", "")}, Still: true,
		})
	}
	return gotermboard.Frame{
		Strip: []gotermboard.Line{
			b.header(h, items, grouping, unread),
			b.filterLine(),
			gotermboard.ReadStatus(b.statuses(), now),
			b.writes.Line(now),
		},
		Columns: columns,
		Groups:  groups,
	}
}

// header counts the items shown under the filter in force, says where the read holds only the
// newest, and names the grouping.
func (b *Board) header(h held, items []server.ItemListed, grouping string, unread bool) gotermboard.Line {
	count := "unread"
	if !unread {
		count = strconv.Itoa(len(items)) + " items"
		if h.more && h.outcome == b.Outcome() {
			count = "the newest " + count
		}
	}
	return gotermboard.Line{
		gotermboard.C("history ", ""), gotermboard.C(count, gotermboard.Bold), gotermboard.C("  outcome ", ""),
		gotermboard.C(filterWords(b.Outcome()), gotermboard.Bold), gotermboard.C("  group ", ""), gotermboard.C(grouping, gotermboard.Bold),
		gotermboard.C(", newest landed first", ""),
	}
}

// filterLine names each outcome filter that l cycles, with the one in force bracketed and bold, so
// a frame printed without color still marks it.
func (b *Board) filterLine() gotermboard.Line {
	line := gotermboard.Line{gotermboard.C(KeyOutcome, gotermboard.Bold), gotermboard.C(" outcome:", "")}
	for _, f := range filters {
		word := gotermboard.C(filterWords(f), dim)
		if f == b.Outcome() {
			word = gotermboard.C("["+filterWords(f)+"]", gotermboard.Bold)
		}
		line = append(line, gotermboard.C(" ", ""), word)
	}
	return line
}

// filterWords names an outcome filter, "every outcome" for none.
func filterWords(outcome string) string { return cmp.Or(outcome, "every outcome") }

// statuses are the reads the strip names: the items, and the playlists once a watch asked for
// them.
func (b *Board) statuses() []gotermboard.Status {
	out := []gotermboard.Status{b.items.Status()}
	if b.wantPlaylists.Load() {
		out = append(out, b.playlists.Status())
	}
	return out
}

// shown is the read's items of the outcome in force. A read under another filter is narrowed to
// it until the read under this one lands. A read begun after a write landed settles its item.
func (b *Board) shown(h held) []server.ItemListed {
	b.writes.Settle(h.readFrom)
	outcome := b.Outcome()
	if outcome == "" || h.outcome == outcome {
		return h.items
	}
	var kept []server.ItemListed
	for _, it := range h.items {
		if it.Outcome == outcome {
			kept = append(kept, it)
		}
	}
	return kept
}

// grouped is the items as the grouping draws them, newest landed first within each group.
func (b *Board) grouped(items []server.ItemListed, grouping string, now time.Time, unread bool) []gotermboard.Group {
	switch grouping {
	case GroupOutcome:
		var groups []gotermboard.Group
		for _, o := range store.Outcomes {
			groups = b.appendGroup(groups, "outcome/"+o, o, items, func(it server.ItemListed) bool { return it.Outcome == o }, now, unread)
		}
		return groups
	case GroupConnector:
		return b.groupedBy(items, "connector", func(it server.ItemListed) string { return cmp.Or(strings.Join(it.Connectors, ", "), "no connector") }, now, unread)
	case GroupDecider:
		return b.groupedBy(items, "decider", func(it server.ItemListed) string {
			if it.Decided == nil {
				return "nobody yet"
			}
			return it.Decided.Decider
		}, now, unread)
	}
	return b.appendGroup(nil, "all", filterWords(b.Outcome())+", newest landed first", items, func(server.ItemListed) bool { return true }, now, unread)
}

// groupedBy is a group for each name that nameOf gives an item, the name with the most items first.
func (b *Board) groupedBy(items []server.ItemListed, kind string, nameOf func(server.ItemListed) string, now time.Time, unread bool) []gotermboard.Group {
	counts := map[string]int{}
	for _, it := range items {
		counts[nameOf(it)]++
	}
	names := slices.SortedFunc(maps.Keys(counts), func(a, z string) int { return cmp.Or(counts[z]-counts[a], strings.Compare(a, z)) })
	var groups []gotermboard.Group
	for _, name := range names {
		groups = b.appendGroup(groups, kind+"/"+name, name, items, func(it server.ItemListed) bool { return nameOf(it) == name }, now, unread)
	}
	return groups
}

// appendGroup appends a group of the items keep holds, and nothing where it holds none.
func (b *Board) appendGroup(groups []gotermboard.Group, key, title string, items []server.ItemListed, keep func(server.ItemListed) bool,
	now time.Time, unread bool,
) []gotermboard.Group {
	g := gotermboard.Group{Key: key, Title: gotermboard.Line{gotermboard.C(outsider(title), gotermboard.Bold)}, Counted: !unread}
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

// row is one item: its outcome, where it went, who decided and why, its connectors, its phase,
// how long ago it landed, and its title. Dim under it are its key, its channel and the decision's
// decided_by. A write in flight or failed says so ahead of the title.
func (b *Board) row(it server.ItemListed, now time.Time) gotermboard.Row {
	key := it.Key
	where, by, why := whereWords(it), byWords(it), whyWords(it)
	age := dash()
	if at, err := time.Parse(store.StampLayout, it.CreatedTS); err == nil {
		age = gotermboard.Ticking(gotermboard.Ago(now.Sub(at)), "")
	}
	under := gotermboard.C(underLine(it), dim)
	title := gotermboard.Line{gotermboard.C(outsider(orDash(it.Title)), "")}
	if p, held := b.writes.Pending(key); held && p.Fault != "" {
		title = append(gotermboard.Line{gotermboard.C("write failed  ", gotermboard.Red)}, title...)
		under = gotermboard.C(strings.ReplaceAll(strings.TrimSpace(p.Fault), "\n", "; "), gotermboard.Red)
	} else if held {
		title = append(gotermboard.Line{gotermboard.C(p.Doing+"  ", gotermboard.Yellow)}, title...)
	}
	return gotermboard.Row{
		Key: key,
		Cells: []gotermboard.Cell{
			gotermboard.C(it.Outcome, outcomeCodes[it.Outcome]), cellOf(where), cellOf(by), cellOf(why),
			cellOf(outsider(strings.Join(it.Connectors, ", "))), gotermboard.C(phaseWords(it), ""), age,
		},
		CutLast: title,
		Under:   under,
		Text:    filterText(it),
		Detail:  detail(it),
	}
}

// whereWords is where the item went: the destination a placement names, else its side, and
// review or the model stage where it waits there.
func whereWords(it server.ItemListed) string {
	switch {
	case it.Decided != nil && it.Decided.Destination != nil:
		return *it.Decided.Destination
	case it.Outcome == store.OutcomePlaced && it.Side != nil:
		return *it.Side
	case it.ReviewReason != nil:
		return "review"
	case it.WaitingAt != nil:
		return "at " + *it.WaitingAt
	}
	return ""
}

// byWords is who decided: a person, a rule by its name, a tag, the gate, a book or a service.
func byWords(it server.ItemListed) string {
	d := it.Decided
	if d == nil {
		return ""
	}
	if name, ok := strings.CutPrefix(d.DecidedBy, store.DecidedByRule("")); ok {
		return server.DeciderRule + " " + name
	}
	return d.Decider
}

// whyWords is why: the decision's cause, or why the item waits in review.
func whyWords(it server.ItemListed) string {
	switch {
	case it.Decided != nil:
		return it.Decided.Cause
	case it.ReviewReason != nil:
		return *it.ReviewReason
	}
	return ""
}

// phaseWords is the item's phase, with the model stage it waits at.
func phaseWords(it server.ItemListed) string {
	if it.WaitingAt != nil {
		return it.Phase + " at " + *it.WaitingAt
	}
	return it.Phase
}

// cellOf is text as a cell, a dim dash where it is empty.
func cellOf(text string) gotermboard.Cell {
	if text == "" {
		return dash()
	}
	return gotermboard.C(text, "")
}

func dash() gotermboard.Cell { return gotermboard.C("—", dim) }

// underLine is the item's key, its channel, and who decided as decided_by spells it.
func underLine(it server.ItemListed) string {
	parts := []string{it.Key}
	if it.Channel != nil {
		parts = append(parts, *it.Channel)
	}
	if it.Decided != nil {
		parts = append(parts, it.Decided.DecidedBy)
	}
	return outsider(strings.Join(parts, " · "))
}

// filterText is what / matches beyond the row: the URL, the channel and decided_by.
func filterText(it server.ItemListed) string {
	words := []string{orDash(it.URL), orDash(it.Channel)}
	if it.Decided != nil {
		words = append(words, it.Decided.DecidedBy)
	}
	return outsider(strings.Join(words, " "))
}

// detail is what the side panel and an opened row show: the title, the URL, the key, the outcome
// in a sentence, the connectors and channel, the phase and when the item landed, and what a
// placement here does.
func detail(it server.ItemListed) []gotermboard.Cell {
	cells := []gotermboard.Cell{gotermboard.C(outsider(orDash(it.Title)), gotermboard.Bold)}
	if it.URL != nil {
		cells = append(cells, gotermboard.C(outsider(*it.URL), ""))
	}
	cells = append(cells,
		gotermboard.C(outsider("key "+it.Key+" · zone "+string(it.Zone)+" · "+it.MediaType), ""),
		gotermboard.C(outsider(outcomeSentence(it)), outcomeCodes[it.Outcome]),
		gotermboard.C(outsider("from "+cmp.Or(strings.Join(it.Connectors, ", "), "no connector")+channelWords(it)), ""),
		gotermboard.C(outsider("phase "+phaseWords(it)+" · landed "+localStamp(it.CreatedTS)), dim),
	)
	if it.Decided != nil && it.Decided.Decider == server.DeciderGate && it.Outcome == store.OutcomeDropped {
		cells = append(cells, gotermboard.C("The gate dropped it. Placing or archiving it here overrules the gate, which returns each of its families to rung 0.", dim))
	}
	return cells
}

// outcomeSentence says where the item's decisions leave it, who decided, why and when.
func outcomeSentence(it server.ItemListed) string {
	d := it.Decided
	switch {
	case d == nil && it.ReviewReason != nil:
		return "waits in review as " + *it.ReviewReason
	case d == nil && it.WaitingAt != nil:
		return "waits at " + *it.WaitingAt
	case d == nil:
		return "waits, " + it.Phase + " so far"
	}
	by := " by " + d.DecidedBy + " for " + d.Cause + " on " + localStamp(d.DecidedTS)
	switch it.Outcome {
	case store.OutcomePlaced:
		return "placed on " + cmp.Or(whereWords(it), "its side") + by
	case store.OutcomeArchived:
		return "archived" + by
	case store.OutcomeDropped:
		return "dropped" + by
	}
	return it.Outcome + by
}

// channelWords is the item's channel after its connectors, where its sightings name one.
func channelWords(it server.ItemListed) string {
	if it.Channel == nil {
		return ""
	}
	return " · channel " + *it.Channel
}

// localStamp is a store stamp in this machine's zone.
func localStamp(stamp string) string {
	at, err := time.Parse(store.StampLayout, stamp)
	if err != nil {
		return stamp
	}
	return at.Local().Format("2006-01-02 15:04 MST")
}

// outsider is text that a stranger may have written, with every terminal control taken out, so a
// title or a channel cannot drive the terminal it is drawn on.
func outsider(text string) string { return terminalcontrol.Strip(text) }

func orDash(s *string) string {
	if s == nil || *s == "" {
		return "—"
	}
	return *s
}

// SchemaVersion is the shape `ingestion tui --view history --json` prints, which
// schemas/tui-history.schema.json holds.
const SchemaVersion = 1

// Snapshot is what --json prints: each item shown under the outcome filter in force, newest
// landed first, beside the group the board's default grouping draws it under, whether the read
// holds only the newest, and how each read stands.
type Snapshot struct {
	SchemaVersion int                  `json:"schema_version"`
	TakenAt       time.Time            `json:"taken_at"`
	Outcome       *string              `json:"outcome"`
	Grouping      string               `json:"grouping"`
	More          bool                 `json:"more"`
	Items         []ItemShown          `json:"items"`
	Sources       []gotermboard.Status `json:"sources"`
	Unread        []string             `json:"unread_sources,omitempty"`
}

// ItemShown is one row: the item as the service listed it, and the group it draws under.
type ItemShown struct {
	Group string            `json:"group"`
	Item  server.ItemListed `json:"item"`
}

// Snapshot reads what the board holds now.
func (b *Board) Snapshot(now time.Time) Snapshot {
	h := b.items.Value()
	grouping := b.Groupings()[0]
	snap := Snapshot{
		SchemaVersion: SchemaVersion, TakenAt: now.UTC(), Grouping: grouping, More: h.more && h.outcome == b.Outcome(),
		Items: []ItemShown{}, Sources: b.statuses(),
	}
	if outcome := b.Outcome(); outcome != "" {
		snap.Outcome = &outcome
	}
	for _, s := range snap.Sources {
		if s.ReadAt.IsZero() && s.Name == sourceItems {
			snap.Unread = append(snap.Unread, s.Name)
		}
	}
	for _, g := range b.grouped(b.shown(h), grouping, now, false) {
		for _, r := range g.Rows {
			it, _ := h.item(r.Key)
			snap.Items = append(snap.Items, ItemShown{Group: g.Key, Item: it})
		}
	}
	return snap
}
