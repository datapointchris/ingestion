package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// phaseBatch bounds how many items one read settles, well under SQLite's limit
// on a statement's parameters.
const phaseBatch = 500

// itemColumn is a logged table's column that joins its rows to items, the
// same way that recordsOf joins them.
type itemColumn struct {
	table  Table
	column string
	// blob says the column holds a blob's SHA-256, which joins a row to each
	// item whose blob_sha256 it is.
	blob bool
}

// itemColumns reads how each logged table joins its rows to items, from db's
// columns: items by its key, blobs by its SHA-256, and every other table by
// its item_key column, or by its blob_sha256 where it has none. When a stage
// adds a table with either column, this finds it unchanged.
func itemColumns(ctx context.Context, db *sql.DB) ([]itemColumn, error) {
	var out []itemColumn
	for _, t := range Tables {
		switch t.Name {
		case "items":
			out = append(out, itemColumn{table: t, column: "key"})
			continue
		case "blobs":
			out = append(out, itemColumn{table: t, column: "sha256", blob: true})
			continue
		}
		columns, err := tableColumns(ctx, db, t.Name)
		if err != nil {
			return nil, err
		}
		names := map[string]bool{}
		for _, c := range columns {
			names[c.name] = true
		}
		switch {
		case names["item_key"]:
			out = append(out, itemColumn{table: t, column: "item_key"})
		case names["blob_sha256"]:
			out = append(out, itemColumn{table: t, column: "blob_sha256", blob: true})
		}
	}
	return out, nil
}

// settleTouched settles the phase of each item that a row this write stamped
// joins to. The guard refuses a row stamped other than by the write in flight,
// so the stamp finds each row that the write added or changed, by put or by
// SQL. The store deletes no row, so no change goes unstamped.
func (s *Store) settleTouched(ctx context.Context, tx *Tx) error {
	selects := make([]string, len(s.itemColumns))
	args := make([]any, len(s.itemColumns))
	for i, ic := range s.itemColumns {
		touched := `SELECT ` + quote(ic.column) + ` FROM ` + ic.table.Name +
			` WHERE ` + ic.table.StampSQL("") + ` = ? AND ` + quote(ic.column) + ` IS NOT NULL`
		if ic.blob {
			touched = `SELECT key FROM items WHERE blob_sha256 IN (` + touched + `)`
		}
		selects[i] = touched
		args[i] = tx.stamp()
	}
	rows, err := tx.tx.QueryContext(ctx, strings.Join(selects, ` UNION `), args...)
	if err != nil {
		return fmt.Errorf("read the items a write touched: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = tx.settlePhases(ctx, keys)
	return err
}

// settlePhases writes the phase that Phase decides on each item under keys
// whose row holds another, and answers how many it moved. The write stamps
// the row, so the outbox ships the item again with its new phase.
func (t *Tx) settlePhases(ctx context.Context, keys []string) (int, error) {
	moved := 0
	for start := 0; start < len(keys); start += phaseBatch {
		batch := keys[start:min(start+phaseBatch, len(keys))]
		args := make([]any, len(batch))
		for i, k := range batch {
			args[i] = k
		}
		items, err := readInto[Item](ctx, t.tx, table("items"), `key IN (?`+strings.Repeat(", ?", len(batch)-1)+`)`, args...)
		if err != nil {
			return moved, err
		}
		records, err := recordsOf(ctx, t.tx, items)
		if err != nil {
			return moved, err
		}
		for _, r := range records {
			phase := r.Phase()
			if r.Item.Phase != nil && *r.Item.Phase == phase {
				continue
			}
			if _, err := t.tx.ExecContext(ctx, `UPDATE items SET phase = ?, updated_ts = ? WHERE key = ?`,
				phase, t.stamp(), r.Item.Key); err != nil {
				return moved, fmt.Errorf("keep %s's phase: %w", r.Item.Key, err)
			}
			moved++
		}
	}
	return moved, nil
}

// SettlePhases settles every item's phase, a batch to a write, and answers how
// many items it read and how many it moved. A store that an earlier release
// wrote holds no phase, or one that an earlier Phase decided, and each item
// it moves ships again. Other writes run between its batches.
func (s *Store) SettlePhases(ctx context.Context) (items, moved int, err error) {
	after := ""
	for {
		var keys []string
		var n int
		err := s.Update(ctx, func(tx *Tx) error {
			rows, err := tx.tx.QueryContext(ctx, `SELECT key FROM items WHERE key > ? ORDER BY key LIMIT ?`, after, phaseBatch)
			if err != nil {
				return err
			}
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var key string
				if err := rows.Scan(&key); err != nil {
					return err
				}
				keys = append(keys, key)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			n, err = tx.settlePhases(ctx, keys)
			return err
		})
		if err != nil {
			return items, moved, fmt.Errorf("settle each item's phase: %w", err)
		}
		items, moved = items+len(keys), moved+n
		if len(keys) < phaseBatch {
			return items, moved, nil
		}
		after = keys[len(keys)-1]
	}
}
