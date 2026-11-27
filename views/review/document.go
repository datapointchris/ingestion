package review

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/comments"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// document is the item's reading pane as markdown: its title, URL and where it came from, the
// note's draft where one was drafted, the quality gate's whole answer, the scan's findings, then
// the parse's text, a section a unit, the
// post first and then each comment. Every value that an item carries is drawn literally, so markdown in
// a stranger's text reads as typed.
func (b *Board) document(key string) string {
	it, ok := b.queue.Value().item(key)
	if !ok {
		return ""
	}
	var d strings.Builder
	fmt.Fprintf(&d, "# %s\n\n", literalLine(orDash(it.Title)))
	if it.URL != nil {
		fmt.Fprintf(&d, "%s\n\n", literalLine(*it.URL))
	}
	fmt.Fprintf(&d, "- **key** %s\n", literalLine(it.Key))
	fmt.Fprintf(&d, "- **zone** %s\n", zoneWords(it.Zone))
	fmt.Fprintf(&d, "- **waits** as %s, since %s\n", literalLine(it.Reason), waitedSince(it))
	for _, s := range it.Sightings {
		fmt.Fprintf(&d, "- **from** %s\n", literalLine(sightingWords(s)))
	}
	d.WriteString(cardSection(it))
	d.WriteString(commentsSection(it))
	d.WriteString(draftSection(it))
	d.WriteString("\n## The quality gate's answer\n\n")
	d.WriteString(answerSection(it))
	d.WriteString(b.parseSections(key))
	return d.String()
}

// cardSection is what a person decides from at a glance: the channel, the length, when it was
// made at its source, the views, likes and comments and the likes per view, then the gate's
// quality, fit, verdict, reason and side guess, and whether the gate would drop it.
func cardSection(it item) string {
	rows := []string{"- **channel** " + literalLine(cmp.Or(channelOf(it), "none named"))}
	if it.LengthS != nil {
		rows = append(rows, "- **length** "+length(*it.LengthS))
	}
	if it.SourceTS != nil {
		at := made(it)
		rows = append(rows, "- **made** "+literalLine(at.Local().Format("2006-01-02"))+", "+gotermboard.Ago(time.Since(at))+" ago")
	}
	for _, m := range []struct {
		n    *int64
		word string
	}{{it.Views, "views"}, {it.Likes, "likes"}, {it.CommentCount, "comments"}} {
		if m.n != nil {
			rows = append(rows, fmt.Sprintf("- **%s** %d", m.word, *m.n))
		}
	}
	if ratio, ok := likesPerView(it); ok {
		rows = append(rows, "- **likes per view** "+literalLine(percent(ratio)))
	}
	if a := it.answer; a != nil {
		rows = append(rows,
			fmt.Sprintf("- **quality** %d of 10", a.Quality),
			"- **fit** "+fitWords(a.Fit),
			fmt.Sprintf("- **verdict** %s at %s confidence", literalLine(a.Verdict), confidence(a.VerdictConfidence)),
			"- **reason** "+literalLine(cmp.Or(a.Reason, "none given")),
			fmt.Sprintf("- **side hint** %s at %s confidence", literalLine(a.Side), confidence(a.SideConfidence)))
	} else if g := it.Guess; g != nil {
		rows = append(rows, fmt.Sprintf("- **side hint** %s at %s confidence", literalLine(g.Side), confidence(g.Confidence)))
	}
	if it.Reason == route.CauseGateWouldDrop {
		rows = append(rows, "- **"+literalLine(route.CauseGateWouldDrop)+"** the gate would drop it, and its family has not earned hiding the drop")
	}
	if it.Comments != nil {
		rows = append(rows, "- **comments read** "+literalLine(commentsWords(it.Comments)))
	}
	return "\n## The card\n\n" + strings.Join(rows, "\n") + "\n"
}

// fitWords is the gate's fit to the owner's current work, or why there is none.
func fitWords(fit *int) string {
	if fit == nil {
		return "none, since the gate read no record of the current work"
	}
	return strconv.Itoa(*fit) + " of 10"
}

// commentsSection is what a model read in the item's top comments, once the read is done. A model
// wrote it over strangers' comments, so every field is drawn literally.
func commentsSection(it item) string {
	c := it.Comments
	if c == nil || c.State != server.CommentsDone {
		return ""
	}
	a, err := comments.DecodeAnswer(c.Answer)
	if err != nil {
		return "\n## What the comments say\n\nThe answer the store keeps could not be read: " + literalLine(err.Error()) + "\n"
	}
	rows := []string{"- **tone** " + literalLine(a.Tone), "- **summary** " + literalLine(a.Summary)}
	for _, p := range a.Praise {
		rows = append(rows, "- **praise** "+literalLine(p))
	}
	for _, p := range a.Criticism {
		rows = append(rows, "- **criticism** "+literalLine(p))
	}
	return "\n## What the comments say\n\nA model read this from the video's top comments, which strangers wrote.\n\n" + strings.Join(rows, "\n") + "\n"
}

// zoneWords says who wrote the item's content.
func zoneWords(zone store.Zone) string {
	if zone == store.Outside {
		return "outside, written by a stranger"
	}
	return string(zone) + ", the owner's own words"
}

// waitedSince is when the item began to wait, in this machine's zone, and how long ago that was.
func waitedSince(it item) string {
	at := since(it)
	return literalLine(at.Local().Format("2006-01-02 15:04 MST")) + ", " + gotermboard.Ago(time.Since(at)) + " ago"
}

// sightingWords is a connector and what its sighting carries: each field, each metric and each tag.
func sightingWords(s server.ReviewSighting) string {
	parts := []string{s.Connector}
	for _, name := range sortedKeys(s.Fields) {
		parts = append(parts, name+" "+s.Fields[name])
	}
	for _, name := range slices.Sorted(maps.Keys(s.Metrics)) {
		parts = append(parts, name+" "+strconv.FormatInt(s.Metrics[name], 10))
	}
	if len(s.Tags) > 0 {
		parts = append(parts, "tags "+strings.Join(s.Tags, ", "))
	}
	return strings.Join(parts, " · ")
}

// draftSection is the note's draft as a list: how sure it is, its summary, how it bears on the
// owner's work, its idea, why the item was likely saved and its questions. A model drafted it from
// the item, so every field is drawn literally. It is empty where no draft was made.
func draftSection(it item) string {
	switch {
	case it.draftFault != "":
		return "\n## The note's draft\n\nThe draft the store keeps could not be read: " + literalLine(it.draftFault) + "\n"
	case it.draft == nil:
		return ""
	}
	a := it.draft
	rows := []string{
		fmt.Sprintf("- **confidence** %s", confidence(a.Confidence)),
		"- **summary** " + literalLine(a.Summary),
		"- **relation** " + literalLine(a.Relation),
		"- **idea** " + literalLine(a.Idea),
		"- **why saved** " + literalLine(a.WhySaved),
	}
	for _, q := range a.Questions {
		rows = append(rows, "- **question** "+literalLine(q))
	}
	return "\n## The note's draft\n\nA model drafted this from the item's opening. Placing the item with no note takes it.\n\n" +
		strings.Join(rows, "\n") + "\n"
}

// answerSection is the gate's whole answer as a list, or why there is none. With no answer, the
// guess that the queue carries still shows.
func answerSection(it item) string {
	a := it.answer
	if a == nil {
		var d strings.Builder
		switch {
		case it.answerFault != "":
			fmt.Fprintf(&d, "The answer the store keeps could not be read: %s\n", literalLine(it.answerFault))
		default:
			d.WriteString("The quality gate gave no answer.\n")
		}
		if g := it.Guess; g != nil {
			fmt.Fprintf(&d, "\n- **side** %s at %s confidence\n", literalLine(g.Side), confidence(g.Confidence))
		}
		return d.String()
	}
	rows := []string{
		fmt.Sprintf("- **verdict** %s at %s confidence", literalLine(a.Verdict), confidence(a.VerdictConfidence)),
		fmt.Sprintf("- **quality** %d of 10", a.Quality),
		fmt.Sprintf("- **side** %s at %s confidence", literalLine(a.Side), confidence(a.SideConfidence)),
		"- **reason** " + literalLine(a.Reason),
		"- **fit** " + fitWords(a.Fit),
		"- **genre** " + literalLine(a.Genre),
		"- **domains** " + literalLine(strings.Join(a.Domains, ", ")),
		"- **themes** " + literalLine(strings.Join(a.Themes, "; ")),
		"- **idea** " + literalLine(a.Idea),
	}
	return strings.Join(rows, "\n") + "\n"
}

// parseSections are the scan's findings and the parse's text, or what the read of the parse says
// while it has not landed.
func (b *Board) parseSections(key string) string {
	p, read := b.parses.Value()[key]
	switch {
	case !read && b.parses.Status().Fault != "":
		return "\n## Text\n\n" + literalLine(b.parses.Status().Fault) + ". Moving to the row again, or R, reads it again.\n"
	case !read:
		return "\n## Text\n\nReading the parse.\n"
	case !p.found:
		return "\n## Text\n\nThe item holds no usable parse.\n"
	}
	var d strings.Builder
	d.WriteString("\n## The scan's findings\n\n")
	d.WriteString(findingsSection(p.answer))
	units := p.answer.Units
	if len(units) == 0 {
		d.WriteString("\n## Text\n\n" + literalBlock(p.answer.Text) + "\n")
		return d.String()
	}
	for _, u := range units {
		heading := strconv.Itoa(u.N)
		if u.Label != nil && *u.Label != "" {
			heading += " · " + literalLine(*u.Label)
		}
		d.WriteString("\n## " + heading + "\n\n" + literalBlock(u.Text) + "\n")
	}
	return d.String()
}

// findingsSection lists each finding of the newest scan over the parse, with the unit it sits in
// and the text it matched.
func findingsSection(p server.ParseAnswer) string {
	if p.RulesVersion == nil {
		return "No scan has read this parse.\n"
	}
	if len(p.Findings) == 0 {
		return "None, under rules " + literalLine(*p.RulesVersion) + ".\n"
	}
	var d strings.Builder
	for _, f := range p.Findings {
		where := ""
		if f.Unit != nil {
			where = " in unit " + strconv.Itoa(*f.Unit)
		}
		fmt.Fprintf(&d, "- **%s** %s%s: %s\n", literalLine(f.Rule), literalLine(f.Severity), where, literalLine(f.Span))
	}
	return d.String()
}

// literalLine is text as one line of markdown that reads as typed: every terminal control taken
// out, its line breaks made spaces, and every ASCII punctuation mark escaped, so no emphasis,
// link, heading, list, table, HTML or entity forms from it.
func literalLine(text string) string {
	return escapePunctuation(strings.Join(strings.Fields(terminalcontrol.Strip(text)), " "))
}

// literalBlock is text as markdown paragraphs that read as typed. A blank line ends a paragraph,
// and every other line break is kept as a hard break of two trailing spaces. A backslash break
// would follow a line's own escaped closing backslash, and goldmark then draws both and joins the
// lines. A line's leading spaces become no-break spaces, so an indented line keeps its indent
// rather than opening a code block.
func literalBlock(text string) string {
	var paragraphs []string
	var lines []string
	flush := func() {
		if len(lines) > 0 {
			paragraphs = append(paragraphs, strings.Join(lines, "  \n"))
			lines = nil
		}
	}
	for _, line := range strings.Split(terminalcontrol.Strip(text), "\n") {
		line = strings.TrimRight(line, " \t\r")
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		body := strings.TrimLeft(line, " \t")
		indent := strings.ReplaceAll(line[:len(line)-len(body)], "\t", "    ")
		lines = append(lines, strings.Repeat(" ", len(indent))+escapePunctuation(body))
	}
	flush()
	if len(paragraphs) == 0 {
		return "(empty)"
	}
	return strings.Join(paragraphs, "\n\n")
}

// escapePunctuation puts a backslash before each ASCII punctuation mark, which CommonMark reads
// as that mark and nothing more.
func escapePunctuation(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, r := range text {
		if r < 0x80 && strings.ContainsRune("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
