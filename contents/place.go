package contents

import (
	"strconv"
	"strings"

	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/store"
)

const (
	// searchPages is how many PDF pages either side of an offset's mapped
	// page the search for its title reads. A scanned book's offset drifts
	// through it, so the printed page alone can miss by several pages. A
	// page label is the book's own number for its page, so a label's mapped
	// page is searched around not at all.
	searchPages = 20
	// headWords is how many words at the top of a page are read for a title.
	headWords = 40
)

// How an entry's start page was found.
const (
	// PlacedByLabel is a printed page that a page label names.
	PlacedByLabel = "label"
	// PlacedByOffset is a printed page less the book's measured offset.
	PlacedByOffset = "offset"
	// PlacedByTitle is the page that opens on the entry's title, found near
	// the page that the label or offset gave, or after the entry before it.
	PlacedByTitle = "title"
	// PlacedByNextEntry is an entry with no printed page, such as a part's
	// heading, placed where the next placed entry starts.
	PlacedByNextEntry = "next-entry"
)

// Place is each of answer's entries on a PDF page of units, a positional
// parse, in answer's order, and the entries with a printed page that it left
// out. A printed page maps to the PDF page whose label is it, else to
// the page less the book's offset where one is measured. An offset drifts
// through a scanned book, so the page near the offset's whose top holds the
// entry's title then places it. That is the start of the run of such pages,
// because running heads repeat a chapter's title on its later pages. A
// printed page that maps to none places its entry at the first page after the
// entry before it that opens on its title. An entry with no printed page
// takes the start of the next placed entry. An entry placed none of these
// ways is left out.
func Place(answer []AnswerEntry, units []store.Unit) ([]sections.Entry, []AnswerEntry) {
	offset, measured := sections.Offset(units)
	skip := map[int]bool{}
	last := 0
	for _, page := range ContentsPages(units) {
		skip[page], last = true, page
	}
	labels := pageLabels(units, last)
	heads := make([]string, len(units))
	for i, u := range units {
		if !skip[i+1] {
			heads[i] = head(u.Text)
		}
	}
	var out, unpaged []sections.Entry
	var left []AnswerEntry
	var open []opened
	after := 0
	for _, a := range answer {
		e := sections.Entry{Level: a.Level, Title: strings.TrimSpace(a.Title)}
		s := search{heads: heads, key: sections.TitleKey(e.Title), after: after}
		for len(open) > 0 && open[len(open)-1].level >= e.Level {
			if open[len(open)-1].level == e.Level {
				s.floor = open[len(open)-1].page
			}
			open = open[:len(open)-1]
		}
		for _, o := range open {
			s.enclosing = append(s.enclosing, o.key)
		}
		if s.floor == 0 && len(open) > 0 {
			s.floor = open[len(open)-1].page
		}
		open = append(open, opened{level: e.Level, key: s.key})
		if a.Page == nil || strings.TrimSpace(*a.Page) == "" {
			unpaged = append(unpaged, e)
			continue
		}
		printed := strings.ToLower(strings.TrimSpace(*a.Page))
		switch page, ok := labels[printed]; {
		case ok:
			e.Page, e.PlacedBy = page, PlacedByLabel
		case measured:
			if n, err := strconv.Atoi(printed); err == nil && n+offset >= 1 && n+offset <= len(units) {
				e.Page, e.PlacedBy = n+offset, PlacedByOffset
			}
		}
		if e.PlacedBy == PlacedByOffset {
			if found := s.near(e.Page); found > 0 && found != e.Page {
				e.Page, e.PlacedBy = found, PlacedByTitle
			}
		} else if e.Page == 0 {
			if found := s.firstAfter(); found > 0 {
				e.Page, e.PlacedBy = found, PlacedByTitle
			}
		}
		if e.Page == 0 {
			left = append(left, a)
			continue
		}
		for _, u := range unpaged {
			u.Page, u.PlacedBy = e.Page, PlacedByNextEntry
			out = append(out, u)
		}
		unpaged = nil
		out = append(out, e)
		after, open[len(open)-1].page = e.Page, e.Page
	}
	return out, left
}

// opened is an entry that holds the entries after it at a deeper level, and
// the page it was placed on, 0 where it is not.
type opened struct {
	level int
	key   string
	page  int
}

// search looks for one entry's title in a book's page heads.
type search struct {
	heads []string
	key   string
	// enclosing are the keys of the entries that hold this one, such as its
	// chapter's. A running head may carry one, and it may hold the key.
	enclosing []string
	// after is the page of the entry placed before this one.
	after int
	// floor is the page of the entry before this one at its level, else of
	// the entry that holds it, and 0 where neither is placed.
	floor int
}

// holds says whether page p's head holds the key outside every enclosing key
// that holds the key, so a section titled Errors is not found in a running
// head of its chapter, Errors and Exceptions.
func (s search) holds(p int) bool {
	if p < 1 || p > len(s.heads) {
		return false
	}
	h := s.heads[p-1]
	for _, k := range s.enclosing {
		if strings.Contains(k, s.key) {
			h = strings.ReplaceAll(h, k, "")
		}
	}
	return strings.Contains(h, s.key)
}

// near is the page within searchPages of page whose head holds the key,
// nearest page and the earlier on a tie, moved back to the start of the run
// of pages around it whose heads hold it, a page apart at most, and never
// before floor. It is 0 where none does.
func (s search) near(page int) int {
	if len(s.key) < sections.TitleLeast {
		return 0
	}
	found := 0
	for d := 0; d <= searchPages && found == 0; d++ {
		switch {
		case s.holds(page - d):
			found = page - d
		case s.holds(page + d):
			found = page + d
		}
	}
	for found > 0 {
		switch {
		case found-1 >= s.floor && s.holds(found-1):
			found--
		case found-2 >= s.floor && s.holds(found-2):
			found -= 2
		default:
			return found
		}
	}
	return 0
}

// firstAfter is the first page after the entry placed before whose head
// holds the key, and 0 where none does.
func (s search) firstAfter() int {
	if len(s.key) < sections.TitleLeast {
		return 0
	}
	for p := s.after + 1; p <= len(s.heads); p++ {
		if s.holds(p) {
			return p
		}
	}
	return 0
}

// pageLabels maps each label that sections.PageLabels reads of units,
// lowercased, to its PDF page. Where a label repeats, as one that a book's
// front matter and its body both use does, it maps to its first page after
// page after, else to its first page.
func pageLabels(units []store.Unit, after int) map[string]int {
	labels := map[string]int{}
	for i, l := range sections.PageLabels(units) {
		if l == "" {
			continue
		}
		l = strings.ToLower(l)
		if held, ok := labels[l]; !ok || (held <= after && i+1 > after) {
			labels[l] = i + 1
		}
	}
	return labels
}

// head is sections.Squashed of the first headWords words of a page.
func head(text string) string {
	words := strings.Fields(text)
	return sections.Squashed(strings.Join(words[:min(len(words), headWords)], " "))
}
