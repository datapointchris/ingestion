package contents

import (
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/store"
)

// A book that prints no page numbers measures no offset. Its chapters' titles
// open their pages, and they vote the offset that places a section opening
// below the top of its page.
func TestABookWithNoPageNumbersPlacesAMidPageSectionByItsPrintedPage(t *testing.T) {
	filler := strings.Repeat("filler ", 45)
	pages := []string{
		"Chess Notes",
		"Contents\nOpening Moves 3\nMiddle Game 6\nPawn Structures 7\nEndgame Technique 9",
		"Opening Moves\n" + filler, filler, filler,
		"Middle Game\n" + filler,
		filler + "\nPawn Structures\n" + filler,
		filler,
		"Endgame Technique\n" + filler, filler, filler, filler,
	}
	units := make([]store.Unit, len(pages))
	for i, text := range pages {
		units[i] = store.Unit{N: i + 1, Text: text}
	}
	page := func(p string) *string { return &p }
	answer := []AnswerEntry{
		{Title: "Opening Moves", Page: page("3"), Level: 1},
		{Title: "Middle Game", Page: page("6"), Level: 1},
		{Title: "Pawn Structures", Page: page("7"), Level: 2},
		{Title: "Endgame Technique", Page: page("9"), Level: 1},
	}
	placed, left := Place(answer, units)
	if len(left) != 0 || len(placed) != len(answer) {
		t.Fatalf("placed %d and left out %d of %d entries", len(placed), len(left), len(answer))
	}
	for i, want := range []int{3, 6, 7, 9} {
		if placed[i].Page != want {
			t.Errorf("%s placed on page %d by %s, want page %d", placed[i].Title, placed[i].Page, placed[i].PlacedBy, want)
		}
	}
	if placed[2].PlacedBy != PlacedByOffset {
		t.Errorf("the section was placed by %s, want %s", placed[2].PlacedBy, PlacedByOffset)
	}
}
