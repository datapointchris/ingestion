package store

import (
	"context"
	"strings"
)

// shelfCandidates selects every item that a read decision ever put on the
// shelf, as a superset that a reader of the shelf narrows. A book's chapter
// takes its book's decisions, so it is left out.
const shelfCandidates = `
SELECT DISTINCT d.item_key FROM decisions d WHERE d.gate = 'read' AND d.verdict = '` + VerdictShelf + `'
	AND d.item_key NOT IN (SELECT c.key FROM items c WHERE c.media_type = '` + MediaTypeChapter + `')`

// ShelfCandidates reads every item that a read decision ever put on the
// shelf, with its records, the oldest landed first. A later decision may have
// moved one off the shelf since, which the reader decides from the records.
func (s *Store) ShelfCandidates(ctx context.Context) ([]ItemRecords, error) {
	keys, err := s.selectKeys(ctx, shelfCandidates, "select the items put on the shelf")
	if err != nil || len(keys) == 0 {
		return []ItemRecords{}, err
	}
	tx, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	items, err := readInto[Item](ctx, tx, table("items"), `key IN (?`+strings.Repeat(", ?", len(keys)-1)+`) ORDER BY created_ts, key`, keys...)
	if err != nil {
		return nil, err
	}
	return recordsOf(ctx, tx, items)
}
