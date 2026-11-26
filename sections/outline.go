package sections

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/datapointchris/ingestion/store"
)

const (
	// RuleVersion names the outline rule and the offset, so a changed rule
	// writes its sections beside the old.
	RuleVersion = "2"
	// WholeFilePages is the page count under which a PDF with no usable
	// outline takes one section over the whole file. A longer one gets no
	// sections from the outline rule.
	WholeFilePages = 50
	// minEntries and minPages are the fewest entries, on the fewest distinct
	// pages, of a usable outline level. minSpan is the least share of the book
	// between its first and last entries.
	minEntries = 3
	minPages   = 3
	minSpan    = 0.5
	// offsetCover is the least share of a book's pages that one offset
	// explains for it to count.
	offsetCover = 0.3
	// edgeLines is how many lines at the top and at the bottom of a page are
	// read for its printed number.
	edgeLines = 3
	// bookPrefix is the fewest letters and digits that an entry's title and
	// the book's must share at the start to name the same book, and
	// bookWordShare the least share of its words that names it otherwise.
	bookPrefix    = 6
	bookWordShare = 0.6
	// identityShare is the share of a book's pages whose label is their PDF
	// page at or above which PageLabels reads none of its labels.
	identityShare = 0.9
)

// How a section's start page was found.
const (
	PlacedByOutline   = "outline"
	PlacedByWholeFile = "whole-file"
)

var (
	// partTitle is a title such as Part I, Part 2 or Part One.
	partTitle = regexp.MustCompile(`^part\s+([ivxlcdm]+|\d+|one|two|three|four|five|six|seven|eight|nine|ten|[a-z])\b`)
	// frontMatter and backMatter are the titles of the entries that are no
	// chapter, as a book's front and back matter name them.
	frontMatter = matterTitle(
		`front cover|title page|half[- ]title|copyright page|cover page|(?:brief|detailed|short) contents|table of contents|`+
			`contents at a glance|list of (?:figures|tables|illustrations|listings|examples|abbreviations|symbols)|`+
			`about (?:this|the) book|about the (?:authors?|editors?|cover|technical reviewers?)|front ?matter|`+
			`how to (?:use|read) this book|who should read this book|conventions used|dear reader|also by|other books`,
		`cover|copyright|dedication|epigraph|contents|foreword|preface|prologue|acknowledge?ments?|praise|endorsements|conventions|welcome`,
		`title`)
	backMatter = matterTitle(
		`subject index|author index|appendix(?:es)?|appendices|further reading|back cover|back ?matter|about the (?:authors?|editors?)`,
		`index|bibliography|references|glossary|afterword|epilogue|colophon|(?:end)?notes|credits|permissions|answers|solutions`,
		``)
	// pageNumber is a line holding a page number alone.
	pageNumber = regexp.MustCompile(`^\d{1,4}$`)
	// leadingNumber is the numbering before a title, such as 1. or A.
	leadingNumber = regexp.MustCompile(`^([0-9]+|[ivxlc]+|[a-z])[.:)]\s+`)
)

// matterTail is what may follow a matter word in a matter title: nothing,
// punctuation, a number, a word such as of or to, or and before another
// matter word. So Index of Notation and Foreword 2 are matter, and Index
// Structures or References and Pointers is a chapter.
const matterTail = `(?:$|\s*[:(\[—–.!?-]|\s+(?:\d{1,3}|[ivxlc]{1,6})$|\s+(?:of|to|for|by|in|or|page)\b|` +
	`\s+and\s+(?:further reading|credits|outline|notation|bibliography|references|glossary|index|permissions|acknowledg))`

// matterTitle matches a normalized title that opens on one of phrases,
// whatever follows it, on one of words before a matterTail, or that is one of
// bare alone, as the words that open many chapter titles are.
func matterTitle(phrases, words, bare string) *regexp.Regexp {
	pattern := `^(?:(?:` + phrases + `)\b|(?:` + words + `)` + matterTail
	if bare != "" {
		pattern += `|(?:` + bare + `)[\s!.:]*$`
	}
	return regexp.MustCompile(pattern + `)`)
}

// Outline is the sections that serve writes from p and the units of the
// PDF's positional parse, under producer version version. title is the
// book's title as its item holds it. Where the outline is usable it is one
// row per entry, each with its kind as FromEntries decides it. Where the
// outline is not usable and the PDF is under WholeFilePages pages, it is one
// section over the whole file. Otherwise it is none.
func Outline(p Profile, units []store.Unit, title, version string) []store.Section {
	var entries []Entry
	for _, e := range p.Outline {
		if e.Page == nil || *e.Page < 1 || *e.Page > p.Pages || e.Level < 1 {
			continue
		}
		entries = append(entries, Entry{Level: e.Level, Title: e.Title, Page: *e.Page, PlacedBy: PlacedByOutline})
	}
	book := Book{Pages: p.Pages, Titles: []string{title, deref(p.Title)}, Units: units}
	if rows := FromEntries(entries, book, store.SectionsFromOutline, version); rows != nil || p.Pages >= WholeFilePages {
		return rows
	}
	printed := printedPages(units)
	return []store.Section{{
		Producer: store.SectionsFromOutline, ProducerVersion: version, N: 1, Level: 1, Kind: store.SectionSection,
		PDFPageStart: 1, PDFPageEnd: p.Pages, PrintedPageStart: printed(1), PrintedPageEnd: printed(p.Pages),
		StartPlacedBy: PlacedByWholeFile,
	}}
}

// Entry is one entry that opens a page of a book, as its outline or its
// printed contents names it, in the book's order.
type Entry struct {
	// Level is the entry's depth, from 1 at the top.
	Level int
	Title string
	// Page is the PDF page that the entry opens, from 1.
	Page int
	// PlacedBy says how Page was found, such as outline.
	PlacedBy string
}

// Book is what the rule reads of a book beside its entries.
type Book struct {
	Pages int
	// Titles are the names the book goes by, such as its item's title and
	// its PDF's own, any of them empty.
	Titles []string
	// Units are the PDF's positional parse, unit n being PDF page n.
	Units []store.Unit
}

// FromEntries is the sections that producer writes from entries under
// version: one row per entry, each with its kind as kindOf decides it, and
// nil where no level of them makes a usable outline of book. An entry on PDF
// page 1 whose title names the book is front matter, as a cover or title page
// is.
func FromEntries(entries []Entry, book Book, producer, version string) []store.Section {
	es := make([]entry, len(entries))
	for i, e := range entries {
		es[i] = entry{level: e.Level, title: e.Title, page: e.Page, placedBy: e.PlacedBy, matter: matterOf(e, book.Titles)}
	}
	level, usable := chapterLevel(es, book.Pages)
	if !usable {
		return nil
	}
	printed := printedPages(book.Units)
	out := make([]store.Section, len(es))
	var parents []int
	for i, e := range es {
		for len(parents) > 0 && es[parents[len(parents)-1]].level >= e.level {
			parents = parents[:len(parents)-1]
		}
		s := store.Section{
			Producer: producer, ProducerVersion: version, N: i + 1, Level: e.level, Kind: kindOf(es, i, level),
			PDFPageStart: e.page, PDFPageEnd: endOf(es, i, book.Pages), StartPlacedBy: e.placedBy,
		}
		if title := strings.TrimSpace(e.title); title != "" {
			s.Title = &title
		}
		if len(parents) > 0 {
			n := parents[len(parents)-1] + 1
			s.ParentN = &n
		}
		s.PrintedPageStart, s.PrintedPageEnd = printed(s.PDFPageStart), printed(s.PDFPageEnd)
		out[i] = s
		parents = append(parents, i)
	}
	return out
}

// entry is one Entry as the rule reads it.
type entry struct {
	level    int
	title    string
	page     int
	placedBy string
	// matter is the entry's kind where it is front or back matter, and empty
	// otherwise.
	matter string
}

// matterOf is e's kind where its title names front or back matter, or where
// it opens PDF page 1 and names the book, and empty otherwise.
func matterOf(e Entry, titles []string) string {
	title := normalized(e.Title)
	switch {
	case frontMatter.MatchString(title):
		return store.SectionFrontMatter
	case backMatter.MatchString(title):
		return store.SectionBackMatter
	case e.Page == 1 && slices.ContainsFunc(titles, func(book string) bool { return namesBook(e.Title, book) }):
		return store.SectionFrontMatter
	}
	return ""
}

// namesBook says whether title names the book called book: one's letters
// and digits open the other's, the shorter holding at least bookPrefix of
// them, or at least bookWordShare of title's words, two at least, are book's.
// A file's name such as gobookname reads the same as Go Book Name.
func namesBook(title, book string) bool {
	a, b := Squashed(title), Squashed(book)
	if len(a) > len(b) {
		a, b = b, a
	}
	if len(a) >= bookPrefix && strings.HasPrefix(b, a) {
		return true
	}
	words, theirs := wordsOf(title), wordsOf(book)
	shared := 0
	for w := range words {
		if theirs[w] {
			shared++
		}
	}
	return shared >= 2 && float64(shared) >= bookWordShare*float64(len(words))
}

// wordsOf is the set of s's lowercased words of three letters or digits or
// more.
func wordsOf(s string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if utf8.RuneCountInString(w) >= 3 {
			out[w] = true
		}
	}
	return out
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// normalized is title lowercased, its spaces collapsed, and any numbering
// before it dropped, so Appendix and A. Appendix read alike. It keeps the Part
// or Appendix word that TitleKey drops, because the rule reads that word.
func normalized(title string) string {
	t := strings.ToLower(strings.Join(strings.Fields(title), " "))
	return leadingNumber.ReplaceAllString(t, "")
}

// chapterLevel is the shallowest level whose body entries make a usable
// outline, skipping a level whose body entries are mostly parts. It is false
// where no level does.
func chapterLevel(entries []entry, pages int) (int, bool) {
	deepest := 0
	for _, e := range entries {
		deepest = max(deepest, e.level)
	}
	for level := 1; level <= deepest; level++ {
		var body []entry
		parts := 0
		for _, e := range entries {
			if e.level != level || e.matter != "" {
				continue
			}
			body = append(body, e)
			if partTitle.MatchString(normalized(e.title)) {
				parts++
			}
		}
		if parts*2 > len(body) {
			continue
		}
		if usable(body, pages) {
			return level, true
		}
	}
	return 0, false
}

// usable says whether body makes a usable outline of a book of pages: at
// least minEntries entries on at least minPages distinct pages, whose first
// and last span at least minSpan of the book.
func usable(body []entry, pages int) bool {
	if len(body) < minEntries {
		return false
	}
	distinct := map[int]bool{}
	first, last := pages, 1
	for _, e := range body {
		distinct[e.page] = true
		first, last = min(first, e.page), max(last, e.page)
	}
	return len(distinct) >= minPages && float64(last-first+1) >= minSpan*float64(pages)
}

// kindOf is entries[i]'s kind where the chapter level is level: a section
// below it, front or back matter by title at or above it, and a chapter at
// it. Above it, an entry is a part where its title names one or it holds an
// entry at the chapter level, and otherwise a chapter, such as an
// Introduction before Part I.
func kindOf(entries []entry, i, level int) string {
	e := entries[i]
	switch {
	case e.level > level:
		return store.SectionSection
	case e.matter != "":
		return e.matter
	case e.level == level || !partTitle.MatchString(normalized(e.title)) && !holds(entries, i, level):
		return store.SectionChapter
	default:
		return store.SectionPart
	}
}

// holds says whether entries[i] holds an entry at level, before the next
// entry at its own level or above.
func holds(entries []entry, i, level int) bool {
	for _, next := range entries[i+1:] {
		if next.level <= entries[i].level {
			return false
		}
		if next.level == level {
			return true
		}
	}
	return false
}

// endOf is the last PDF page of entries[i]: the page before the next entry at
// its level or above begins, or its own start where that one begins on the
// same page or before. The last such entry ends at the book's last page.
func endOf(entries []entry, i, pages int) int {
	for _, next := range entries[i+1:] {
		if next.level <= entries[i].level {
			return max(entries[i].page, next.page-1)
		}
	}
	return max(entries[i].page, pages)
}

// PageLabels is each page's label in units, trimmed, unit n's at index n-1,
// and empty where it has none. Every one is empty where the labels are their
// pages' PDF pages on identityShare of the pages, because such labels say
// nothing of the numbers that the book prints.
func PageLabels(units []store.Unit) []string {
	labels := make([]string, len(units))
	identity := 0
	for i, u := range units {
		if u.Label == nil {
			continue
		}
		labels[i] = strings.TrimSpace(*u.Label)
		if labels[i] == strconv.Itoa(i+1) {
			identity++
		}
	}
	if len(units) > 0 && float64(identity) >= identityShare*float64(len(units)) {
		return make([]string, len(units))
	}
	return labels
}

// printedPages answers each PDF page's printed page: its label as PageLabels
// reads it, else the PDF page less the book's offset where one is measured
// and the result is a page, and nil otherwise.
func printedPages(units []store.Unit) func(page int) *string {
	offset, measured := Offset(units)
	labels := PageLabels(units)
	return func(page int) *string {
		if page >= 1 && page <= len(units) && labels[page-1] != "" {
			l := labels[page-1]
			return &l
		}
		if !measured || page-offset < 1 {
			return nil
		}
		n := strconv.Itoa(page - offset)
		return &n
	}
}

// Offset is the most common difference between a page's PDF page and a
// number standing alone on one of its first or last edgeLines lines, and true
// where that difference holds on at least offsetCover of the pages. A tie
// goes to the difference nearer 0.
func Offset(units []store.Unit) (int, bool) {
	counts := map[int]int{}
	for i, u := range units {
		seen := map[int]bool{}
		for _, line := range edges(u.Text) {
			if !pageNumber.MatchString(line) {
				continue
			}
			n, _ := strconv.Atoi(line)
			if n >= 1 && !seen[i+1-n] {
				seen[i+1-n] = true
				counts[i+1-n]++
			}
		}
	}
	best, most := 0, 0
	for d, n := range counts {
		if n > most || (n == most && (abs(d) < abs(best) || (abs(d) == abs(best) && d < best))) {
			best, most = d, n
		}
	}
	return best, most > 0 && float64(most) >= offsetCover*float64(len(units))
}

// edges is the first and last edgeLines non-blank lines of text, each
// trimmed, and each line once where the page holds fewer.
func edges(text string) []string {
	var lines []string
	for line := range strings.SplitSeq(text, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) <= 2*edgeLines {
		return lines
	}
	return append(lines[:edgeLines:edgeLines], lines[len(lines)-edgeLines:]...)
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
