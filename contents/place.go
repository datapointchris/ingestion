package contents

import (
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"

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
	// titleRunes bounds how much of a title the search matches, so a long
	// title that the page breaks differently still matches by its opening.
	titleRunes = 30
	// identityShare is the share of a book's pages whose label is their PDF
	// page at or above which its labels are not read.
	identityShare = 0.9
	// titleLeast is the fewest letters and digits of a title that the search
	// looks for.
	titleLeast = 4
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

// numbering is a title's numbering before its words, such as Chapter 3, 2.1,
// Part IV or A.
var numbering = regexp.MustCompile(`^(?:(?:chapter|part|appendix|section|lecture|lesson|unit|module|book)\s+)?(?:\d+(?:\.\d+)*|[ivxlcdm]+|[a-z])(?:[.:)\-–—]|\s|$)\s*`)

// Place is each of answer's entries on a PDF page of units, a positional
// parse, in answer's order. A printed page maps to the PDF page whose label
// is it, else to the page less the book's offset where one is measured. An
// offset drifts through a scanned book, so the page near the offset's whose
// top holds the entry's title then places it, the start of a run of such
// pages, because running heads repeat a chapter's title on its later pages. A printed page that maps to none places its entry at the first
// page after the entry before it that opens on its title. An entry with no
// printed page takes the start of the next placed entry. An entry placed none
// of these ways is left out.
func Place(answer []AnswerEntry, units []store.Unit) []sections.Entry {
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
	var out []sections.Entry
	var unpaged []sections.Entry
	after := 0
	for _, a := range answer {
		e := sections.Entry{Level: a.Level, Title: strings.TrimSpace(a.Title)}
		if a.Page == nil || strings.TrimSpace(*a.Page) == "" {
			unpaged = append(unpaged, e)
			continue
		}
		printed := strings.ToLower(strings.TrimSpace(*a.Page))
		key := titleKey(e.Title)
		switch page, ok := labels[printed]; {
		case ok:
			e.Page, e.PlacedBy = page, PlacedByLabel
		case measured:
			if n, err := strconv.Atoi(printed); err == nil && n+offset >= 1 && n+offset <= len(units) {
				e.Page, e.PlacedBy = n+offset, PlacedByOffset
			}
		}
		if e.PlacedBy == PlacedByOffset {
			if found := near(heads, key, e.Page); found > 0 && found != e.Page {
				e.Page, e.PlacedBy = found, PlacedByTitle
			}
		} else if e.Page == 0 {
			if found := firstAfter(heads, key, after); found > 0 {
				e.Page, e.PlacedBy = found, PlacedByTitle
			}
		}
		if e.Page == 0 {
			continue
		}
		for _, u := range unpaged {
			u.Page, u.PlacedBy = e.Page, PlacedByNextEntry
			out = append(out, u)
		}
		unpaged = nil
		out = append(out, e)
		after = e.Page
	}
	return out
}

// pageLabels maps each page label of units, lowercased, to its PDF page: the
// first after page after where the label repeats, as one that a book's front
// matter and its body both use does. It is empty where the labels are each
// page's PDF page on identityShare of the pages, because such labels say
// nothing of the numbers that the book prints.
func pageLabels(units []store.Unit, after int) map[string]int {
	labels := map[string]int{}
	identity := 0
	for i, u := range units {
		if u.Label == nil {
			continue
		}
		l := strings.ToLower(strings.TrimSpace(*u.Label))
		if l == "" {
			continue
		}
		if l == strconv.Itoa(i+1) {
			identity++
		}
		if held, ok := labels[l]; !ok || (held <= after && i+1 > after) {
			labels[l] = i + 1
		}
	}
	if float64(identity) >= identityShare*float64(len(units)) {
		return map[string]int{}
	}
	return labels
}

// near is the page within searchPages of page whose head holds key, nearest
// page and the earlier on a tie, moved back to the start of the run of pages
// around it whose heads hold key, a page apart at most. It is 0 where none
// does.
func near(heads []string, key string, page int) int {
	if len(key) < titleLeast {
		return 0
	}
	holds := func(p int) bool { return p >= 1 && p <= len(heads) && strings.Contains(heads[p-1], key) }
	found := 0
	for d := 0; d <= searchPages && found == 0; d++ {
		switch {
		case holds(page - d):
			found = page - d
		case holds(page + d):
			found = page + d
		}
	}
	for found > 0 {
		switch {
		case holds(found - 1):
			found--
		case holds(found - 2):
			found -= 2
		default:
			return found
		}
	}
	return 0
}

// firstAfter is the first page after page whose head holds key, and 0 where
// none does.
func firstAfter(heads []string, key string, page int) int {
	if len(key) < titleLeast {
		return 0
	}
	for p := page + 1; p <= len(heads); p++ {
		if strings.Contains(heads[p-1], key) {
			return p
		}
	}
	return 0
}

// titleKey is what the search matches of title: its letters and digits,
// lowercased, after any numbering before its words, up to titleRunes of
// them. A title that is its numbering alone, such as Chapter 3, is matched
// whole.
func titleKey(title string) string {
	t := strings.ToLower(strings.Join(strings.Fields(title), " "))
	key := squashed(numbering.ReplaceAllString(t, ""))
	if len(key) < titleLeast {
		key = squashed(t)
	}
	if r := []rune(key); len(r) > titleRunes {
		key = string(r[:titleRunes])
	}
	return key
}

// head is the letters and digits of the first headWords words of a page,
// lowercased.
func head(text string) string {
	words := strings.Fields(text)
	return squashed(strings.Join(words[:min(len(words), headWords)], " "))
}

// squashed is s folded by NFKC and lowercased, with every rune but its
// letters and digits left out, so a ligature such as ﬁ reads as fi.
func squashed(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(norm.NFKC.String(s)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
