package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ErrNoItem is a write or read naming an item the store does not hold.
var ErrNoItem = errors.New("no such item")

// Tx is one writing transaction. Every row it writes carries its stamp.
type Tx struct {
	tx *sql.Tx
	at time.Time
}

// Now is the transaction's stamp, later than every write committed before it.
func (t *Tx) Now() time.Time { return t.at }

func (t *Tx) stamp() string { return Stamp(t.at) }

// PutItem writes item, keeping the created stamp and flow of a row already
// there. An item new to the store starts a fresh flow where it names none.
func (t *Tx) PutItem(ctx context.Context, item Item) error {
	item.CreatedTS, item.UpdatedTS = t.stamp(), t.stamp()
	if item.FlowID == "" {
		item.FlowID = NewFlowID()
	}
	return t.put(ctx, table("items"), item)
}

// PutSighting writes one connector's arrival of an item, keeping when it was
// first seen.
func (t *Tx) PutSighting(ctx context.Context, s Sighting) error {
	if s.Tags == nil {
		s.Tags = []string{}
	}
	s.FirstSeenTS, s.LastSeenTS = t.stamp(), t.stamp()
	return t.put(ctx, table("sightings"), s)
}

// PutBlob records landed bytes. A blob already recorded is left as it is.
func (t *Tx) PutBlob(ctx context.Context, b Blob) error {
	b.CreatedTS = t.stamp()
	return t.put(ctx, table("blobs"), b)
}

// PutHome records where a file lives. gone marks it gone from its root, and
// the item stays.
func (t *Tx) PutHome(ctx context.Context, h Home, gone bool) error {
	now := t.stamp()
	h.FoundTS, h.GoneTS = now, nil
	if gone {
		// A gone home keeps when it was found, and is stamped when it went.
		err := t.tx.QueryRowContext(ctx, `SELECT found_ts FROM homes WHERE blob_sha256 = ? AND root = ? AND path_sha256 = ?`,
			h.BlobSHA256, h.Root, h.PathSHA256).Scan(&h.FoundTS)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read home %s:%s: %w", h.Root, h.PathSHA256, err)
		}
		h.GoneTS = &now
	}
	return t.put(ctx, table("homes"), h)
}

// PutOutput writes one stage's output, keeping when it was first written.
func (t *Tx) PutOutput(ctx context.Context, o Output) error {
	o.CreatedTS, o.UpdatedTS = t.stamp(), t.stamp()
	return t.put(ctx, table("outputs"), o)
}

// AddDecision appends a gate's decision on an item, numbered after the
// item's last at that gate, and answers it as written.
func (t *Tx) AddDecision(ctx context.Context, d Decision) (Decision, error) {
	var last sql.NullInt64
	if err := t.tx.QueryRowContext(ctx, `SELECT max(sequence) FROM decisions WHERE item_key = ? AND gate = ?`,
		d.ItemKey, d.Gate).Scan(&last); err != nil {
		return d, fmt.Errorf("number a %s decision on %s: %w", d.Gate, d.ItemKey, err)
	}
	d.Sequence = int(last.Int64) + 1
	d.DecidedTS = t.stamp()
	return d, t.put(ctx, table("decisions"), d)
}

// PutDelivery writes one item's delivery to one destination.
func (t *Tx) PutDelivery(ctx context.Context, d Delivery) error {
	d.UpdatedTS = t.stamp()
	return t.put(ctx, table("deliveries"), d)
}

// PutCursor writes one connector's cursor.
func (t *Tx) PutCursor(ctx context.Context, c Cursor) error {
	c.UpdatedTS = t.stamp()
	return t.put(ctx, table("cursors"), c)
}

// AddDeclarations records one applied set of declarations. A commit already
// applied is left as it is.
func (t *Tx) AddDeclarations(ctx context.Context, d Declarations) error {
	if d.Connectors == nil {
		d.Connectors = []json.RawMessage{}
	}
	d.AppliedTS = t.stamp()
	return t.put(ctx, table("declarations"), d)
}

// put writes record's fields as tab's columns. An upserted table writes the
// row again in place, keeping its key and its Kept columns. An appended table
// keeps the row already there.
func (t *Tx) put(ctx context.Context, tab Table, record any) error {
	values, err := columnsOf(tab, record)
	if err != nil {
		return fmt.Errorf("write %s: %w", tab.Name, err)
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	columns := make([]string, len(names))
	args := make([]any, len(names))
	var updates []string
	for i, name := range names {
		columns[i] = quote(name)
		args[i] = values[name]
		if !contains(tab.Key, name) && !contains(tab.Kept, name) {
			updates = append(updates, quote(name)+` = excluded.`+quote(name))
		}
	}
	keys := make([]string, len(tab.Key))
	for i, k := range tab.Key {
		keys[i] = quote(k)
	}
	conflict := `DO NOTHING`
	if tab.Writes == Upserts {
		conflict = `DO UPDATE SET ` + strings.Join(updates, ", ")
	}
	statement := `INSERT INTO ` + tab.Name + ` (` + strings.Join(columns, ", ") + `) VALUES (?` + strings.Repeat(", ?", len(names)-1) +
		`) ON CONFLICT (` + strings.Join(keys, ", ") + `) ` + conflict
	if _, err := t.tx.ExecContext(ctx, statement, args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY") {
			return fmt.Errorf("write %s: %w", tab.Name, ErrNoItem)
		}
		return fmt.Errorf("write %s: %w", tab.Name, err)
	}
	return nil
}

// columnsOf is record's JSON fields as tab's column values: a JSON column as
// its JSON text, a number as an integer where it is whole, and null as nil.
func columnsOf(tab Table, record any) (map[string]any, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	values := make(map[string]any, len(fields))
	for name, raw := range fields {
		if bytes.Equal(raw, []byte("null")) {
			values[name] = nil
			continue
		}
		if tab.IsJSON(name) {
			values[name] = string(raw)
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var v any
		if err := decoder.Decode(&v); err != nil {
			return nil, err
		}
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				v = i
			} else if v, err = n.Float64(); err != nil {
				return nil, err
			}
		}
		values[name] = v
	}
	return values, nil
}

// Row is one row as its table holds it: each column by name, a JSON column as
// json.RawMessage, and its rowid.
type Row struct {
	RowID  int64
	Values map[string]any
}

// Text is column's value as text, and "" for NULL.
func (r Row) Text(column string) string {
	switch v := r.Values[column].(type) {
	case nil:
		return ""
	case string:
		return v
	case json.RawMessage:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// querier is a handle or a transaction.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ReadRows reads tab's rows matching where, which may carry ORDER BY and
// LIMIT, with each one's rowid. It reads every row before answering, so no
// open cursor holds a reader while the caller works.
func ReadRows(ctx context.Context, db querier, tab Table, where string, args ...any) ([]Row, error) {
	rows, err := db.QueryContext(ctx, `SELECT rowid AS store_rowid, * FROM `+tab.Name+` WHERE `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", tab.Name, err)
	}
	defer func() { _ = rows.Close() }()
	names, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("read %s's columns: %w", tab.Name, err)
	}
	var out []Row
	for rows.Next() {
		values := make([]any, len(names))
		into := make([]any, len(names))
		for i := range values {
			into[i] = &values[i]
		}
		if err := rows.Scan(into...); err != nil {
			return nil, fmt.Errorf("read %s: %w", tab.Name, err)
		}
		rowid, ok := values[0].(int64)
		if !ok {
			return nil, fmt.Errorf("%s's rowid is %T", tab.Name, values[0])
		}
		row := Row{RowID: rowid, Values: make(map[string]any, len(names)-1)}
		for i, name := range names[1:] {
			v := values[i+1]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			if text, ok := v.(string); ok && tab.IsJSON(name) && json.Valid([]byte(text)) {
				v = json.RawMessage(text)
			}
			row.Values[name] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// readInto reads tab's rows matching where into records of type T, by their
// JSON names.
func readInto[T any](ctx context.Context, db querier, tab Table, where string, args ...any) ([]T, error) {
	rows, err := ReadRows(ctx, db, tab, where, args...)
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		encoded, err := json.Marshal(row.Values)
		if err != nil {
			return nil, err
		}
		var record T
		if err := json.Unmarshal(encoded, &record); err != nil {
			return nil, fmt.Errorf("read a %s row: %w", tab.Name, err)
		}
		out = append(out, record)
	}
	return out, nil
}
