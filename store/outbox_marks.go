package store

import (
	"context"
	"fmt"
	"time"
)

// OutboxMark is how far the outbox has shipped one table.
type OutboxMark struct {
	Table        string
	ThroughStamp string
	ThroughRowID int64
	// Shipped counts the records every accepted batch carried, resends
	// included.
	Shipped int64
}

// OutboxMarks reads every table's mark. A table missing from the answer has
// shipped nothing.
func (s *Store) OutboxMarks(ctx context.Context) (map[string]OutboxMark, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT source_table, through_stamp, through_rowid, shipped FROM outbox_marks`)
	if err != nil {
		return nil, fmt.Errorf("read the outbox's marks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	marks := map[string]OutboxMark{}
	for rows.Next() {
		var m OutboxMark
		if err := rows.Scan(&m.Table, &m.ThroughStamp, &m.ThroughRowID, &m.Shipped); err != nil {
			return nil, fmt.Errorf("read an outbox mark: %w", err)
		}
		marks[m.Table] = m
	}
	return marks, rows.Err()
}

// KeepOutboxMark writes m, under the release that moved it.
func (s *Store) KeepOutboxMark(ctx context.Context, m OutboxMark, version string) error {
	_, err := s.write.ExecContext(ctx, `INSERT INTO outbox_marks
		(source_table, through_stamp, through_rowid, shipped, moved_ts, service_version) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (source_table) DO UPDATE SET through_stamp = excluded.through_stamp, through_rowid = excluded.through_rowid,
			shipped = excluded.shipped, moved_ts = excluded.moved_ts, service_version = excluded.service_version`,
		m.Table, m.ThroughStamp, m.ThroughRowID, m.Shipped, Stamp(time.Now()), version)
	if err != nil {
		return fmt.Errorf("keep %s's outbox mark: %w", m.Table, err)
	}
	return nil
}
