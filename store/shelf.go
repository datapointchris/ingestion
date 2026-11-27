package store

import (
	"context"
	"strings"
)

// shelfCandidates selects each item whose newest read decision keeps it on
// the shelf: a shelf that no removal caused, or an opening. ShelfStanding
// also reads the side decision, so this is a superset that a reader narrows.
// A book's chapter takes its book's decisions, so it is left out.
const shelfCandidates = `
SELECT d.item_key FROM decisions d
WHERE d.gate = 'read'
	AND ((d.verdict = '` + VerdictShelf + `' AND d.cause != '` + CauseShelfRemove + `') OR d.cause = '` + CauseShelfOpen + `')
	AND d.sequence = (SELECT max(n.sequence) FROM decisions n WHERE n.item_key = d.item_key AND n.gate = 'read')
	AND d.item_key NOT IN (SELECT c.key FROM items c WHERE c.media_type = '` + MediaTypeChapter + `')`

// ShelfCandidates reads each item whose newest read decision keeps it on the
// shelf, with its records, the oldest landed first. The reader decides from
// the records where each stands.
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
