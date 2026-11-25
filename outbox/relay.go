// Package outbox is the relay half of a transactional outbox whose log is the
// store itself. Every table logs its change in the transaction that makes it,
// as a row stamped later than every write before it. The relay reads each
// table past its mark in stamp order and ships each row over OTLP: its keys,
// states, hashes, versions, tags, zones and decisions as one ingestion.record,
// and every outsider-written field as one ingestion.text of the same kind, id
// and version. A mark moves only once the collector accepts the batch, so the
// relay ships at least once, and a resend folds on kind, id and version.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/datapointchris/ingestion/store"
)

const (
	// RecordKind is the kind attribute of a row's keys, states and decisions.
	RecordKind = "ingestion.record"
	// TextKind is the kind attribute of a row's outsider-written fields.
	TextKind = "ingestion.text"

	// poll is how often the relay reads past each mark while nothing waits.
	poll = time.Second
	// retryCeiling is the longest a refused table waits for its next try.
	retryCeiling = time.Minute
	// batchRows and batchBytes bound one export. A batch holds one row past
	// the byte bound, so a large row still goes, alone.
	batchRows  = 250
	batchBytes = 4 << 20
	// bodyMax bounds one record's body. It sits under an OTLP receiver's
	// request limit, because a record refused for good by any hop would hold
	// its table's mark for good.
	bodyMax = 8 << 20
	// columnMax is the largest column a body past bodyMax keeps.
	columnMax = 64 << 10
	// bodyBytesPerSecond paces the bodies every table ships together, so a
	// first fill of a large source drains rather than filling the collector's
	// queue.
	bodyBytesPerSecond = 1 << 20
	exportTimeout      = 30 * time.Second
	// versionDigits is the width of a padded version, so a version sorts as
	// text in the order it counts.
	versionDigits = 20
)

// record is one OTLP log record in a batch.
type record struct {
	fleetKind, kind, id, version string
	at                           time.Time
	body                         []byte
}

// tableState is what the relay holds of one table between passes.
type tableState struct {
	table store.Table
	mark  store.OutboxMark

	refusals     int
	refusedSince time.Time
	retryAt      time.Time
	lastErr      error
}

// Relay ships every logged table's rows past its mark.
type Relay struct {
	store   *store.Store
	version string
	log     *slog.Logger
	ship    func(context.Context, []record) error
	pace    *rate.Limiter

	mu     sync.Mutex
	tables []*tableState
}

// Open reads each table's mark from st, and ships to endpoint, the OTLP/HTTP
// logs URL. An empty endpoint ships nothing and keeps every mark.
func Open(ctx context.Context, st *store.Store, endpoint, version string, log *slog.Logger) (*Relay, error) {
	if log == nil {
		log = slog.Default()
	}
	r := &Relay{store: st, version: version, log: log, pace: rate.NewLimiter(bodyBytesPerSecond, bodyMax)}
	marks, err := st.OutboxMarks(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range store.Tables {
		mark := marks[t.Name]
		mark.Table = t.Name
		r.tables = append(r.tables, &tableState{table: t, mark: mark})
	}
	if endpoint != "" {
		if r.ship, err = otlpShipper(ctx, endpoint, version); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Run ships each table past its mark every poll, and again at once while a
// backlog waits, until ctx ends.
func (r *Relay) Run(ctx context.Context) {
	if r.ship == nil {
		r.log.Warn("the outbox ships nothing, because no records_otlp_url is configured")
		return
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		more := r.pass(ctx, time.Now())
		if ctx.Err() != nil {
			return
		}
		if more {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// pass ships one batch from each table that is due, and answers whether any
// table left rows waiting.
func (r *Relay) pass(ctx context.Context, now time.Time) bool {
	more := false
	for _, ts := range r.tables {
		r.mu.Lock()
		due := !now.Before(ts.retryAt)
		r.mu.Unlock()
		if !due {
			continue
		}
		left, err := r.shipBatch(ctx, ts)
		if ctx.Err() != nil {
			return false
		}
		r.mu.Lock()
		if err != nil {
			if ts.refusals == 0 {
				ts.refusedSince = now
			}
			ts.refusals++
			ts.lastErr = err
			wait := min(poll<<min(ts.refusals-1, 6), retryCeiling)
			ts.retryAt = now.Add(wait)
			r.mu.Unlock()
			r.log.Warn("ship ingestion's records", "table", ts.table.Name, "refusals", ts.refusals, "retry_in", wait.String(), "error", err)
			continue
		}
		ts.refusals, ts.retryAt, ts.refusedSince, ts.lastErr = 0, time.Time{}, time.Time{}, nil
		r.mu.Unlock()
		more = more || left
	}
	return more
}

// shipBatch ships the rows of one table past its mark, at most a batch of
// them, and answers whether rows still wait.
func (r *Relay) shipBatch(ctx context.Context, ts *tableState) (bool, error) {
	r.mu.Lock()
	mark := ts.mark
	r.mu.Unlock()
	t := ts.table
	stamp := t.StampSQL("")
	rows, err := store.ReadRows(ctx, r.store.Reader(), t,
		`(`+stamp+` > ? OR (`+stamp+` = ? AND rowid > ?)) ORDER BY `+stamp+`, rowid LIMIT ?`,
		mark.ThroughStamp, mark.ThroughStamp, mark.ThroughRowID, batchRows)
	if err != nil {
		return false, err
	}
	if len(rows) == 0 {
		return false, nil
	}
	records, through, err := r.records(t, rows)
	if err != nil {
		return false, err
	}
	size := 0
	for _, rec := range records {
		size += len(rec.body)
	}
	if err := r.pace.WaitN(ctx, min(size, r.pace.Burst())); err != nil {
		return false, fmt.Errorf("pace a batch of %d: %w", len(records), err)
	}
	exportCtx, cancel := context.WithTimeout(ctx, exportTimeout)
	err = r.ship(exportCtx, records)
	cancel()
	if err != nil {
		return false, fmt.Errorf("a batch of %d records: %w", len(records), err)
	}
	last := rows[through-1]
	moved := mark
	moved.ThroughStamp, moved.ThroughRowID = stampOf(t, last), last.RowID
	moved.Shipped += int64(len(records))
	r.mu.Lock()
	ts.mark = moved
	r.mu.Unlock()
	if err := r.store.KeepOutboxMark(ctx, moved, r.version); err != nil {
		r.log.Warn("keep an outbox mark, so a restart sends its rows again", "table", t.Name, "error", err)
	}
	r.log.Info("shipped ingestion's records", "table", t.Name, "rows", through, "records", len(records), "bytes", size,
		"through_stamp", moved.ThroughStamp, "through_rowid", moved.ThroughRowID)
	return through < len(rows) || len(rows) == batchRows, nil
}

// records builds the records of rows in order, as many rows as one batch
// carries: bodies up to batchBytes, and always the first row. It answers how
// many rows they cover. A row's record and its text always go in one batch.
func (r *Relay) records(t store.Table, rows []store.Row) ([]record, int, error) {
	var out []record
	size := 0
	for i, row := range rows {
		recs, err := r.rowRecords(t, row)
		if err != nil {
			return nil, 0, err
		}
		rowSize := 0
		for _, rec := range recs {
			rowSize += len(rec.body)
		}
		if i > 0 && size+rowSize > batchBytes {
			return out, i, nil
		}
		size += rowSize
		out = append(out, recs...)
	}
	return out, len(rows), nil
}

// rowRecords is row as its ingestion.record, and as its ingestion.text where
// any outsider-written field holds a value.
func (r *Relay) rowRecords(t store.Table, row store.Row) ([]record, error) {
	keys := make([]string, len(t.Key))
	for i, k := range t.Key {
		keys[i] = row.Text(k)
	}
	base := record{kind: t.Name, id: strings.Join(keys, "/")}
	stamp := stampOf(t, row)
	base.at, _ = time.Parse(store.StampLayout, stamp)
	base.version = Version(t, stamp)

	fields, text := map[string]any{}, map[string]any{}
	anyText := false
	for name, v := range row.Values {
		if t.IsText(name) {
			text[name] = v
			anyText = anyText || v != nil
			continue
		}
		fields[name] = v
	}
	main := base
	main.fleetKind = RecordKind
	var err error
	if main.body, err = r.body(main, fields); err != nil {
		return nil, fmt.Errorf("write %s %s: %w", t.Name, main.id, err)
	}
	out := []record{main}
	if anyText {
		outside := base
		outside.fleetKind = TextKind
		if outside.body, err = r.body(outside, text); err != nil {
			return nil, fmt.Errorf("write %s %s's text: %w", t.Name, outside.id, err)
		}
		out = append(out, outside)
	}
	return out, nil
}

// body is values as JSON. A body past bodyMax keeps each column under
// columnMax, names the size of each one it dropped, and says why in
// outbox_omitted, because one record that no hop accepts would hold its
// table's mark for good.
func (r *Relay) body(rec record, values map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(values)
	if err != nil || len(encoded) <= bodyMax {
		return encoded, err
	}
	r.log.Warn("a record is past what one carries, so each large column names its size", "fleet_kind", rec.fleetKind,
		"kind", rec.kind, "id", rec.id, "version", rec.version, "bytes", len(encoded))
	kept := make(map[string]any, len(values)+1)
	for name, v := range values {
		if column, _ := json.Marshal(v); len(column) > columnMax {
			v = map[string]int{"omitted_bytes": len(column)}
		}
		kept[name] = v
	}
	kept["outbox_omitted"] = fmt.Sprintf("the record is %d bytes, past the %d one carries, so each column past %d names its size",
		len(encoded), bodyMax, columnMax)
	return json.Marshal(kept)
}

// stampOf is row's stamp: the first of its table's stamp columns that holds
// one.
func stampOf(t store.Table, row store.Row) string {
	for _, column := range t.Stamp {
		if s := row.Text(column); s != "" {
			return s
		}
	}
	return ""
}

// Version counts a row's record: the Unix milliseconds of its stamp where
// each write is a version, and 1 where the row is written once, zero-padded
// to 20 digits.
func Version(t store.Table, stamp string) string {
	n := int64(1)
	if t.Writes == store.Upserts {
		if at, err := time.Parse(store.StampLayout, stamp); err == nil {
			n = at.UnixMilli()
		}
	}
	return fmt.Sprintf("%0*d", versionDigits, n)
}
