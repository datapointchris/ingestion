// Package store holds ingestion's SQLite store: every item, its sightings, its
// bytes' records, each stage's outputs and decisions, its deliveries, the task
// leases and the connector cursors.
//
// The store is one writer connection beside a pool of readers. Each stage's
// tables come from an embedded schema file of its own in this directory,
// applied on every open with CREATE TABLE IF NOT EXISTS, so a stage adds its
// tables by adding a file here. Every write runs through Update, which stamps
// it later than every write before it, so the outbox reads each table in commit
// order by stamp.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed *.sql
var schemaFiles embed.FS

// StampLayout is how every table writes an instant: RFC 3339 in UTC at a fixed
// width, so the text sorts as it reads.
const StampLayout = "2006-01-02T15:04:05.000Z"

// readConnections is how many reads run at once beside the one writer.
const readConnections = 4

// Store is the open database.
type Store struct {
	write *sql.DB
	read  *sql.DB
	clock func() time.Time

	mu sync.Mutex
	// last is the newest stamp taken, so the next is later.
	last time.Time
}

// Option changes how Open opens the store.
type Option func(*Store)

// WithClock reads now from clock rather than the system's, for a test that
// moves time past a lease.
func WithClock(clock func() time.Time) Option {
	return func(s *Store) { s.clock = clock }
}

// Open opens the database at path, creating it and its directory where they
// are missing, and converges every schema file and every logged table's guard.
func Open(ctx context.Context, path string, options ...Option) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create the store's directory: %w", err)
	}
	s := &Store{clock: time.Now}
	for _, o := range options {
		o(s)
	}
	var err error
	if s.write, err = sql.Open("sqlite", dsn(path, false)); err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// One connection is the one writer, so two claims on a task serialize and
	// a stamp taken inside a transaction rises in commit order.
	s.write.SetMaxOpenConns(1)
	if s.read, err = sql.Open("sqlite", dsn(path, true)); err != nil {
		_ = s.write.Close()
		return nil, fmt.Errorf("open %s to read: %w", path, err)
	}
	s.read.SetMaxOpenConns(readConnections)
	if err := s.converge(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

// dsn names path with the pragmas for every connection. A reader refuses any
// write. A writer's transaction takes the write lock as it begins.
func dsn(path string, readOnly bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "journal_mode(wal)")
	q.Add("_pragma", "synchronous(normal)")
	q.Add("_pragma", "foreign_keys(on)")
	if readOnly {
		q.Add("_pragma", "query_only(on)")
	} else {
		q.Set("_txlock", "immediate")
	}
	return "file:" + path + "?" + q.Encode()
}

// converge applies each schema file in name order, guards each logged table,
// and starts the stamp clock past the newest stamp in any table.
func (s *Store) converge(ctx context.Context) error {
	names, err := fs.Glob(schemaFiles, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		statements, err := schemaFiles.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := s.write.ExecContext(ctx, string(statements)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	for _, t := range Tables {
		if err := guard(ctx, s.write, t); err != nil {
			return err
		}
	}
	for _, t := range Tables {
		var newest sql.NullString
		if err := s.write.QueryRowContext(ctx, `SELECT max(`+t.StampSQL("")+`) FROM `+t.Name).Scan(&newest); err != nil {
			return fmt.Errorf("read %s's newest stamp: %w", t.Name, err)
		}
		if at, err := time.Parse(StampLayout, newest.String); err == nil && at.After(s.last) {
			s.last = at
		}
	}
	return nil
}

// Close closes both handles.
func (s *Store) Close() error { return errors.Join(s.read.Close(), s.write.Close()) }

// Reader is the pool of read-only connections, for a reader such as the
// outbox that walks a table past a mark.
func (s *Store) Reader() *sql.DB { return s.read }

// Ping says whether a read reaches the database.
func (s *Store) Ping(ctx context.Context) error { return s.read.PingContext(ctx) }

// Now is the store's clock.
func (s *Store) Now() time.Time { return s.clock() }

// stamp is now, or a millisecond past the last stamp where now is not later,
// so every write is stamped later than every write committed before it.
func (s *Store) stamp() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := s.clock().UTC().Truncate(time.Millisecond)
	if !at.After(s.last) {
		at = s.last.Add(time.Millisecond)
	}
	s.last = at
	return at
}

// Update runs fn inside one writing transaction, and commits what it wrote
// only where fn returns nil. Every row fn writes carries the transaction's
// stamp.
func (s *Store) Update(ctx context.Context, fn func(*Tx) error) error {
	sqlTx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin a write: %w", err)
	}
	// The stamp is taken once the write lock is held, so it is in commit order.
	tx := &Tx{tx: sqlTx, at: s.stamp()}
	if err := fn(tx); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit a write: %w", err)
	}
	return nil
}

// Stamp formats t as every table writes an instant.
func Stamp(t time.Time) string { return t.UTC().Format(StampLayout) }
