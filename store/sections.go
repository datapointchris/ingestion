package store

import (
	"context"
	"fmt"
	"slices"
)

// Producers of an item's sections.
const (
	// SectionsFromOutline are sections read from a PDF's own outline, or one
	// section over the whole of a short PDF that has none.
	SectionsFromOutline = "outline"
	// SectionsFromContents are sections read from a book's printed contents
	// pages.
	SectionsFromContents = "contents"
)

// Kinds of section.
const (
	SectionPart        = "part"
	SectionChapter     = "chapter"
	SectionSection     = "section"
	SectionFrontMatter = "front-matter"
	SectionBackMatter  = "back-matter"
)

// Section is one entry of an item's sections, as sections holds it.
type Section struct {
	ItemKey         string `json:"item_key"`
	Producer        string `json:"producer"`
	ProducerVersion string `json:"producer_version"`
	N               int    `json:"n"`
	// ParentN is the n of the entry that holds this one, and nil at the top.
	ParentN *int    `json:"parent_n"`
	Level   int     `json:"level"`
	Kind    string  `json:"kind"`
	Title   *string `json:"title"`
	// PDFPageStart and PDFPageEnd are the section's first and last PDF pages,
	// from 1. A positional parse holds PDF page n as unit n, so the range
	// names the section's units.
	PDFPageStart int `json:"pdf_page_start"`
	PDFPageEnd   int `json:"pdf_page_end"`
	// PrintedPageStart and PrintedPageEnd are the pages as the book prints
	// them, and nil where it gives no way to tell.
	PrintedPageStart *string `json:"printed_page_start"`
	PrintedPageEnd   *string `json:"printed_page_end"`
	// StartPlacedBy says how the start page was found, such as outline.
	StartPlacedBy string `json:"start_placed_by"`
	// ChildKey is the key of the item made for a chapter, and nil where none
	// was.
	ChildKey  *string `json:"child_key"`
	CreatedTS string  `json:"created_ts"`
}

// PutSections writes one producer version's sections of an item, once. Where
// that version already wrote sections, they stay as they are, and PutSections
// answers false.
func (t *Tx) PutSections(ctx context.Context, sections []Section) (bool, error) {
	if len(sections) == 0 {
		return false, nil
	}
	first := sections[0]
	var held int
	if err := t.tx.QueryRowContext(ctx, `SELECT count(*) FROM sections WHERE item_key = ? AND producer = ? AND producer_version = ?`,
		first.ItemKey, first.Producer, first.ProducerVersion).Scan(&held); err != nil {
		return false, fmt.Errorf("count %s's sections from %s %s: %w", first.ItemKey, first.Producer, first.ProducerVersion, err)
	}
	if held > 0 {
		return false, nil
	}
	for i, s := range sections {
		if s.ItemKey != first.ItemKey || s.Producer != first.Producer || s.ProducerVersion != first.ProducerVersion || s.N != i+1 {
			return false, fmt.Errorf("one write holds one producer version's sections of one item, numbered from 1, not %s %s %s #%d",
				s.ItemKey, s.Producer, s.ProducerVersion, s.N)
		}
		s.CreatedTS = t.stamp()
		if err := t.put(ctx, table("sections"), s); err != nil {
			return false, err
		}
	}
	return true, nil
}

// Sections reads every section of the item under key, each version's in
// order.
func (t *Tx) Sections(ctx context.Context, key string) ([]Section, error) {
	return readInto[Section](ctx, t.tx, table("sections"), `item_key = ? ORDER BY created_ts, producer, producer_version, n`, key)
}

// Chapters reads the chapter items made from the book under key.
func (t *Tx) Chapters(ctx context.Context, key string) ([]Item, error) {
	children, err := t.Children(ctx, key)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(children, func(c Item) bool { return !c.PlacedThroughParent() }), nil
}

// ChapterSection reads the newest section that names the chapter item under
// key, and false where none does.
func (t *Tx) ChapterSection(ctx context.Context, key string) (Section, bool, error) {
	return first[Section](ctx, t.tx, table("sections"), `child_key = ? AND kind = ? ORDER BY created_ts DESC, rowid DESC`, key, SectionChapter)
}

// Parse reads the done parse of the item under key that pick names, with its
// units, as this write sees it. It answers ErrNoItem and ErrNoParse as
// Store.Parse does.
func (t *Tx) Parse(ctx context.Context, key string, pick ParsePick) (Parse, error) {
	return parseOf(ctx, t.tx, key, pick)
}
