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

// itemColumn is a logged table's column that names the item its row is about.
type itemColumn struct {
	table  Table
	column string
}

// itemColumns reads which logged tables name an item, from db's columns: items
// by its key, and every other table by an item_key column. When a stage adds a
// table with an item_key column, this finds it unchanged.
func itemColumns(ctx context.Context, db *sql.DB) ([]itemColumn, error) {
	var out []itemColumn
	for _, t := range Tables {
		if t.Name == "items" {
			out = append(out, itemColumn{table: t, column: "key"})
			continue
		}
		columns, err := tableColumns(ctx, db, t.Name)
		if err != nil {
			return nil, err
		}
		for _, c := range columns {
			if c.name == "item_key" {
				out = append(out, itemColumn{table: t, column: c.name})
			}
		}
	}
	return out, nil
}

// settleTouched settles the phase of each item that a row this write stamped
// names. The guard refuses a row stamped other than by the write in flight, so
// the stamp finds each row that the write added or changed, by put or by SQL.
func (s *Store) settleTouched(ctx context.Context, tx *Tx) error {
	selects := make([]string, len(s.itemColumns))
	args := make([]any, len(s.itemColumns))
	for i, ic := range s.itemColumns {
		selects[i] = `SELECT ` + quote(ic.column) + ` FROM ` + ic.table.Name +
			` WHERE ` + ic.table.StampSQL("") + ` = ? AND ` + quote(ic.column) + ` IS NOT NULL`
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
	return tx.settlePhases(ctx, keys)
}

// settlePhases writes the phase that Phase decides on each item under keys
// whose row holds another. The write stamps the row, so the outbox ships the
// item again with its new phase.
func (t *Tx) settlePhases(ctx context.Context, keys []string) error {
	for start := 0; start < len(keys); start += phaseBatch {
		batch := keys[start:min(start+phaseBatch, len(keys))]
		args := make([]any, len(batch))
		for i, k := range batch {
			args[i] = k
		}
		items, err := readInto[Item](ctx, t.tx, table("items"), `key IN (?`+strings.Repeat(", ?", len(batch)-1)+`)`, args...)
		if err != nil {
			return err
		}
		records, err := recordsOf(ctx, t.tx, items)
		if err != nil {
			return err
		}
		for _, r := range records {
			phase := r.Phase()
			if r.Item.Phase != nil && *r.Item.Phase == phase {
				continue
			}
			if _, err := t.tx.ExecContext(ctx, `UPDATE items SET phase = ?, updated_ts = ? WHERE key = ?`,
				phase, t.stamp(), r.Item.Key); err != nil {
				return fmt.Errorf("keep %s's phase: %w", r.Item.Key, err)
			}
		}
	}
	return nil
}

// convergePhases settles every item's phase, a batch to a write. A store that
// an earlier release wrote holds no phase, or one that an earlier Phase
// decided, and each item it moves ships again.
func (s *Store) convergePhases(ctx context.Context) error {
	after := ""
	for {
		var keys []string
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
			return tx.settlePhases(ctx, keys)
		})
		if err != nil {
			return fmt.Errorf("settle each item's phase: %w", err)
		}
		if len(keys) < phaseBatch {
			return nil
		}
		after = keys[len(keys)-1]
	}
}
