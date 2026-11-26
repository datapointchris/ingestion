// Package sections is the structure stage and the sections that serve writes
// from it. A worker runs pdf-structure over a PDF's bytes, through
// isolatedprocess, and reports the Profile it writes as the structure
// output's answer. serve then applies the outline rule in the write that ends
// the structure task. It writes one sections row per outline entry, a child
// item per body chapter, and the book's source time from its creation date.
package sections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/datapointchris/ingestion/store"
)

// profileMax bounds the document that pdf-structure writes. An outline of
// 10,000 entries with 200-byte titles is about 3 MiB.
const profileMax = 32 << 20

// Profile is what pdf-structure finds in one PDF.
type Profile struct {
	Pages int `json:"pages"`
	// Outline is every outline entry, in the outline's order.
	Outline    []OutlineEntry `json:"outline"`
	PageLabels []PageLabel    `json:"page_labels"`
	// CreationDate is the metadata's creationDate as the PDF writes it, such
	// as D:20190415093021+02'00', and nil where it holds none.
	CreationDate *string `json:"creation_date"`
	// Title is the metadata's title, and nil where it holds none.
	Title      *string    `json:"title"`
	FontShares FontShares `json:"font_shares"`
}

// OutlineEntry is one entry of a PDF's outline.
type OutlineEntry struct {
	// Level is the entry's depth, from 1 at the top.
	Level int    `json:"level"`
	Title string `json:"title"`
	// Page is the entry's target, as a PDF page from 1, and nil where the
	// target lies outside the document.
	Page *int `json:"page"`
}

// PageLabel is one of a PDF's page-label rules. It labels each page from
// StartPage until the next rule's start.
type PageLabel struct {
	// StartPage is the first PDF page that the rule labels, from 1.
	StartPage int    `json:"start_page"`
	Prefix    string `json:"prefix"`
	// Style is D for decimal, r and R for roman, a and A for letters, and
	// empty for the prefix alone.
	Style       string `json:"style"`
	FirstNumber int    `json:"first_number"`
}

// FontShares are the shares of the sampled pages, every fifth from the first,
// whose fonts include a monospace one or a TeX or math one. Code is set in
// monospace, and formulas in TeX and math fonts.
type FontShares struct {
	SampledPages int     `json:"sampled_pages"`
	Monospace    float64 `json:"monospace"`
	TeXOrMath    float64 `json:"tex_or_math"`
}

// readProfile reads the one document that pdf-structure writes on r. It
// refuses a field that Profile does not hold, and a profile of no pages.
func readProfile(r io.Reader) (Profile, error) {
	raw, err := io.ReadAll(io.LimitReader(r, profileMax+1))
	if err != nil {
		return Profile{}, fmt.Errorf("a stdout it could not read: %w", err)
	}
	if len(raw) > profileMax {
		return Profile{}, fmt.Errorf("a profile past %d bytes", profileMax)
	}
	return decodeProfile(raw)
}

// NewestProfile is the profile of the newest done structure output among
// outputs, and false where none is done.
func NewestProfile(outputs []store.Output) (Profile, bool, error) {
	structure := store.NewestDone(outputs, store.StageStructure)
	if structure == nil {
		return Profile{}, false, nil
	}
	p, err := decodeProfile(structure.Answer)
	if err != nil {
		return Profile{}, false, fmt.Errorf("read %s's profile from %s %s: %w", structure.ItemKey, structure.Producer, structure.ProducerVersion, err)
	}
	return p, true, nil
}

// decodeProfile reads raw as a profile, refusing an unknown field and a
// profile of no pages.
func decodeProfile(raw []byte) (Profile, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var p Profile
	if err := decoder.Decode(&p); err != nil {
		return Profile{}, fmt.Errorf("a stdout that is not a profile: %w", err)
	}
	if p.Pages < 1 {
		return Profile{}, errors.New("a profile of no pages")
	}
	return p, nil
}
