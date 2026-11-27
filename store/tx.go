package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
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

// Item reads the item under key as this write sees it, and false where the
// store holds none.
func (t *Tx) Item(ctx context.Context, key string) (Item, bool, error) {
	return first[Item](ctx, t.tx, table("items"), `key = ?`, key)
}

// Children reads every item whose parent is key.
func (t *Tx) Children(ctx context.Context, key string) ([]Item, error) {
	return readInto[Item](ctx, t.tx, table("items"), `parent_key = ? ORDER BY length(key), key`, key)
}

// Sighting reads connector's sighting of the item under key, and false where
// the store holds none.
func (t *Tx) Sighting(ctx context.Context, key, connector string) (Sighting, bool, error) {
	return first[Sighting](ctx, t.tx, table("sightings"), `item_key = ? AND connector = ?`, key, connector)
}

// Sightings reads every sighting of the item under key, the first seen first.
func (t *Tx) Sightings(ctx context.Context, key string) ([]Sighting, error) {
	return readInto[Sighting](ctx, t.tx, table("sightings"), `item_key = ? ORDER BY first_seen_ts, rowid`, key)
}

// NewestDecision reads the newest decision at gate on the item under key,
// and false where none is.
func (t *Tx) NewestDecision(ctx context.Context, key, gate string) (Decision, bool, error) {
	return first[Decision](ctx, t.tx, table("decisions"), `item_key = ? AND gate = ? ORDER BY sequence DESC`, key, gate)
}

// Decisions reads every decision on the item under key, at every gate.
func (t *Tx) Decisions(ctx context.Context, key string) ([]Decision, error) {
	return readInto[Decision](ctx, t.tx, table("decisions"), `item_key = ? ORDER BY gate, sequence`, key)
}

// Outputs reads every output of the item under key, oldest first, in the order
// that Store.Outputs reads them.
func (t *Tx) Outputs(ctx context.Context, key string) ([]Output, error) {
	return readInto[Output](ctx, t.tx, table("outputs"), `item_key = ? ORDER BY created_ts, rowid`, key)
}

// UnplacedScannedKeys reads the key of each item with a done scan whose
// newest side decision is a review or a drop, or that holds none. The side
// gate decides again only a review or a model's drop, so it narrows these to
// those, and to the items whose newest parse is the one scanned.
func (t *Tx) UnplacedScannedKeys(ctx context.Context) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx, `
SELECT i.key FROM items i
WHERE EXISTS (SELECT 1 FROM outputs o WHERE o.item_key = i.key AND o.stage = 'scan' AND o.state = 'done')
	AND coalesce((SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = 'side'
		ORDER BY d.sequence DESC LIMIT 1), 'review') IN ('review', 'drop')
ORDER BY i.key`)
	if err != nil {
		return nil, fmt.Errorf("select the scanned items that no side decision placed: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// Blob reads the blob row for sum, and false where the store holds none.
func (t *Tx) Blob(ctx context.Context, sum string) (Blob, bool, error) {
	return first[Blob](ctx, t.tx, table("blobs"), `sha256 = ?`, sum)
}

// LiveHomesAt reads every home at root and pathSHA256 that is not gone,
// whatever blob it names.
func (t *Tx) LiveHomesAt(ctx context.Context, root, pathSHA256 string) ([]Home, error) {
	return readInto[Home](ctx, t.tx, table("homes"), `root = ? AND path_sha256 = ? AND gone_ts IS NULL`, root, pathSHA256)
}

// LiveHomes reads every home of the blob sum that is not gone, oldest first.
func (t *Tx) LiveHomes(ctx context.Context, sum string) ([]Home, error) {
	return readInto[Home](ctx, t.tx, table("homes"), `blob_sha256 = ? AND gone_ts IS NULL ORDER BY found_ts, rowid`, sum)
}

// LiveHomesUnder reads every home at root that is not gone, in path order.
func (t *Tx) LiveHomesUnder(ctx context.Context, root string) ([]Home, error) {
	return readInto[Home](ctx, t.tx, table("homes"), `root = ? AND gone_ts IS NULL ORDER BY path, rowid`, root)
}

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
// first seen. Where the arrival took the sighting's counts, the caller sets
// MetricsReadTS to the write's time.
func (t *Tx) PutSighting(ctx context.Context, s Sighting) error {
	if s.Tags == nil {
		s.Tags = []string{}
	}
	if s.Fields == nil {
		s.Fields = map[string]string{}
	}
	if s.Metrics == nil {
		s.Metrics = map[string]int64{}
	}
	stamp := t.stamp()
	s.FirstSeenTS, s.LastSeenTS, s.WrittenTS = stamp, stamp, &stamp
	return t.put(ctx, table("sightings"), s)
}

// metricName is a metric's name: lowercase letters, digits and underscores,
// from a letter, such as duration_s.
var metricName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ErrInvalidMetric is a metric whose name metricName refuses.
var ErrInvalidMetric = errors.New("a metric is named in lowercase letters, digits and underscores, from a letter")

// CheckMetrics refuses a metric in metrics whose name is not a metric's, with
// ErrInvalidMetric.
func CheckMetrics(metrics map[string]int64) error {
	for _, name := range slices.Sorted(maps.Keys(metrics)) {
		if !metricName.MatchString(name) {
			return fmt.Errorf("%w, not %q", ErrInvalidMetric, name)
		}
	}
	return nil
}

// PutSightingMetrics writes metrics, as a read of the source just took them,
// onto connector's sighting of the item under key, each by its name. It keeps
// the sighting's other metrics, its fields and tags, and when it was last
// seen, and stamps when its metrics were read, whether or not one changed. It
// writes nothing where the store holds no such sighting. It answers whether a
// metric changed.
func (t *Tx) PutSightingMetrics(ctx context.Context, key, connector string, metrics map[string]int64) (bool, error) {
	if err := CheckMetrics(metrics); err != nil {
		return false, err
	}
	s, ok, err := t.Sighting(ctx, key, connector)
	if err != nil || !ok {
		return false, err
	}
	next := maps.Clone(s.Metrics)
	if next == nil {
		next = map[string]int64{}
	}
	maps.Copy(next, metrics)
	changed := !maps.Equal(next, s.Metrics)
	stamp := t.stamp()
	s.Metrics, s.WrittenTS, s.MetricsReadTS = next, &stamp, &stamp
	return changed, t.put(ctx, table("sightings"), s)
}

// ReviewKeysSightedBy reads, in key order, the key of each item in phase
// review that one of connectors sighted, leaving out each item whose
// metrics such a sighting took at or after readSince, where it is not empty.
func (t *Tx) ReviewKeysSightedBy(ctx context.Context, connectors []string, readSince string) ([]string, error) {
	if len(connectors) == 0 {
		return nil, nil
	}
	in := `(?` + strings.Repeat(", ?", len(connectors)-1) + `)`
	args := []any{PhaseReview}
	for _, c := range connectors {
		args = append(args, c)
	}
	unread := ""
	if readSince != "" {
		unread = ` AND NOT EXISTS (SELECT 1 FROM sightings r WHERE r.item_key = s.item_key AND r.connector IN ` + in +
			` AND r.metrics_read_ts >= ?)`
		for _, c := range connectors {
			args = append(args, c)
		}
		args = append(args, readSince)
	}
	rows, err := t.tx.QueryContext(ctx, `SELECT DISTINCT s.item_key FROM sightings s JOIN items i ON i.key = s.item_key
		WHERE i.phase = ? AND s.connector IN `+in+unread+` ORDER BY s.item_key`, args...)
	if err != nil {
		return nil, fmt.Errorf("select the items in review that %s sighted: %w", strings.Join(connectors, ", "), err)
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
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

// PutCursor writes one connector's cursor, keeping when it was created.
func (t *Tx) PutCursor(ctx context.Context, c Cursor) error {
	c.CreatedTS, c.UpdatedTS = t.stamp(), t.stamp()
	return t.put(ctx, table("cursors"), c)
}

// Cursor reads connector's cursor as this write sees it, and false where the
// store holds none.
func (t *Tx) Cursor(ctx context.Context, connector string) (Cursor, bool, error) {
	return first[Cursor](ctx, t.tx, table("cursors"), `connector = ?`, connector)
}

// PutDeclarations records one applied set of declarations, stamped now. A
// commit applied again is stamped again, so it is the set in force.
func (t *Tx) PutDeclarations(ctx context.Context, d Declarations) error {
	for _, list := range []*[]json.RawMessage{&d.Connectors, &d.RoutingRules, &d.Destinations} {
		if *list == nil {
			*list = []json.RawMessage{}
		}
	}
	d.AppliedTS = t.stamp()
	return t.put(ctx, table("declarations"), d)
}

// KeyedRequest reads the request caller sent under key, and false where the
// store holds none.
func (t *Tx) KeyedRequest(ctx context.Context, caller, key string) (KeyedRequest, bool, error) {
	return first[KeyedRequest](ctx, t.tx, table("keyed_requests"), `caller = ? AND idempotency_key = ?`, caller, key)
}

// AddKeyedRequest records a request and its answer. A key the caller already
// sent is left as it is.
func (t *Tx) AddKeyedRequest(ctx context.Context, r KeyedRequest) error {
	r.CreatedTS = t.stamp()
	return t.put(ctx, table("keyed_requests"), r)
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
			if n, ok := v.(int64); ok && tab.IsBool(name) {
				v = n != 0
			}
			row.Values[name] = v
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// first reads the first of tab's rows matching where into a record of type T,
// and false where none matches.
func first[T any](ctx context.Context, db querier, tab Table, where string, args ...any) (T, bool, error) {
	records, err := readInto[T](ctx, db, tab, where+` LIMIT 1`, args...)
	if err != nil || len(records) == 0 {
		var none T
		return none, false, err
	}
	return records[0], true, nil
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
