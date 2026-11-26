package sections

import (
	"strconv"
	"testing"

	"github.com/datapointchris/ingestion/store"
)

// A book with no page labels takes its printed pages from the numbers that
// stand alone at its pages' foot. A short book with no usable outline is one
// section over the whole file, and a long one has none until its contents
// are read.
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
		t.Errorf("a %d-page book with no outline made %+v, want none until its contents are read", WholeFilePages, got)
	}
}
