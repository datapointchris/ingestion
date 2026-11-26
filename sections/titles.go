package sections

import (
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const (
	// TitleLeast is the fewest letters and digits of a title's key that a
	// search for the title looks for. A title whose words after its
	// numbering hold fewer is keyed whole.
	TitleLeast = 4
	// titleRunes bounds a title's key, so a long title that a page breaks
	// differently still matches by its opening.
	titleRunes = 30
)

// numbering is a title's numbering before its words, such as Chapter 3, 2.1,
// Part IV or A.
var numbering = regexp.MustCompile(`^(?:(?:chapter|part|appendix|section|lecture|lesson|unit|module|book)\s+)?(?:\d+(?:\.\d+)*|[ivxlcdm]+|[a-z])(?:[.:)\-–—]|\s|$)\s*`)

// Squashed is s folded by NFKC and lowercased, with every rune but its letters
// and digits left out, so a ligature such as ﬁ reads as fi.
func Squashed(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(norm.NFKC.String(s)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// TitleKey is a title as a search for it on a page, and a later version's
// chapter, match it: Squashed after any numbering before its words, up to
// titleRunes runes. So 1 Start and Chapter 1: Start share a key.
func TitleKey(title string) string {
	t := strings.ToLower(strings.Join(strings.Fields(title), " "))
	key := Squashed(numbering.ReplaceAllString(t, ""))
	if len(key) < TitleLeast {
		key = Squashed(t)
	}
	if r := []rune(key); len(r) > titleRunes {
		key = string(r[:titleRunes])
	}
	return key
}
