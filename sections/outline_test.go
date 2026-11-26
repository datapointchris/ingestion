package sections

import (
	"strconv"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/store"
)

// A book with no page labels takes its printed pages from the numbers that
// stand alone at its pages' foot. A short book with no usable outline is one
// section over the whole file, and a long one has none.
func TestAnUnlabeledBookPrintsByItsOffsetAndAnOutlinelessOneByItsLength(t *testing.T) {
	page := func(n int) *int { return &n }
	units := func(pages int) []store.Unit {
		out := make([]store.Unit, pages)
		for i := range out {
			out[i] = store.Unit{N: i + 1, Text: "Running head\nbody text"}
			if i+1 > 4 {
				out[i].Text += "\n" + strconv.Itoa(i+1-4)
			}
		}
		return out
	}
	outlined := Profile{Pages: 60, Outline: []OutlineEntry{
		{Level: 1, Title: "1. Start", Page: page(5)},
		{Level: 1, Title: "2. Middle", Page: page(20)},
		{Level: 1, Title: "3. Late", Page: page(35)},
		{Level: 1, Title: "4. End", Page: page(50)},
	}}
	got := Outline(outlined, units(60), "v")
	if len(got) != 4 {
		t.Fatalf("the outline made %d sections, want 4", len(got))
	}
	for i, want := range [][2]string{{"1", "15"}, {"16", "30"}, {"31", "45"}, {"46", "56"}} {
		s := got[i]
		if s.Kind != store.SectionChapter || s.PrintedPageStart == nil || *s.PrintedPageStart != want[0] || *s.PrintedPageEnd != want[1] {
			t.Errorf("section %d is a %s printed %v-%v, want a chapter printed %s-%s", i+1, s.Kind, s.PrintedPageStart, s.PrintedPageEnd, want[0], want[1])
		}
	}

	short := Profile{Pages: 40, Outline: outlined.Outline[:2]}
	if got := Outline(short, units(40), "v"); len(got) != 1 || got[0].StartPlacedBy != PlacedByWholeFile || got[0].PDFPageEnd != 40 {
		t.Errorf("a 40-page book with two outline entries made %+v, want one section over the whole file", got)
	}
	if got := Outline(Profile{Pages: WholeFilePages}, units(WholeFilePages), "v"); got != nil {
		t.Errorf("a %d-page book with no outline made %+v, want none", WholeFilePages, got)
	}
}

// A book in parts keeps the Introduction and Conclusion beside its parts as
// chapters, and a chapter whose title opens on a matter word stays a chapter.
func TestAnEntryBesideTheBooksPartsIsAChapterAndSoIsOneTitledLikeMatter(t *testing.T) {
	page := func(n int) *int { return &n }
	book := Profile{Pages: 300, Outline: []OutlineEntry{
		{Level: 1, Title: "Preface", Page: page(5)},
		{Level: 1, Title: "Introduction", Page: page(10)},
		{Level: 1, Title: "Part I", Page: page(20)},
		{Level: 2, Title: "1. References and Pointers", Page: page(21)},
		{Level: 1, Title: "Part II", Page: page(100)},
		{Level: 2, Title: "2. Index Structures", Page: page(101)},
		{Level: 1, Title: "Part III", Page: page(200)},
		{Level: 2, Title: "3. Notes on Style", Page: page(201)},
		{Level: 1, Title: "Conclusion", Page: page(280)},
		{Level: 1, Title: "Index of Notation", Page: page(290)},
	}}
	var kinds []string
	for _, s := range Outline(book, nil, "v") {
		kinds = append(kinds, s.Kind)
	}
	if got, want := strings.Join(kinds, " "), "front-matter chapter part chapter part chapter part chapter chapter back-matter"; got != want {
		t.Errorf("the book's sections are %s, want %s", got, want)
	}
}
