package review

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/terminalcontrol"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// document is the item's reading pane as markdown: its title, URL and where it came from, the
// quality gate's whole answer, the scan's findings, then the parse's text, a section a unit, the
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
	d.WriteString("\n## The quality gate's answer\n\n")
	d.WriteString(answerSection(it))
	d.WriteString(b.parseSections(key))
	return d.String()
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

// sightingWords is a connector and what its sighting carries: each field and each tag.
func sightingWords(s server.ReviewSighting) string {
	parts := []string{s.Connector}
	for _, name := range sortedKeys(s.Fields) {
		parts = append(parts, name+" "+s.Fields[name])
	}
	if len(s.Tags) > 0 {
		parts = append(parts, "tags "+strings.Join(s.Tags, ", "))
	}
	return strings.Join(parts, " · ")
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
// and every other line break is kept as a hard break. A line's leading spaces become no-break
// spaces, so an indented line keeps its indent rather than opening a code block.
func literalBlock(text string) string {
	var paragraphs []string
	var lines []string
	flush := func() {
		if len(lines) > 0 {
			paragraphs = append(paragraphs, strings.Join(lines, "\\\n"))
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
