// Package store holds ingestion's SQLite store: every item, its sightings, its
// bytes' records, each stage's outputs and decisions, its deliveries, the task
// leases and the connector cursors.
//
// The store is one writer connection beside a pool of readers. Each stage's
// tables come from an embedded schema file of its own in this directory,
// converged on every open. A stage adds a table in its file and declares it in
// Tables, and Open refuses a table that is in neither Tables nor unlogged.
// Every write runs through Update, which stamps it later than every write
// before it, so the outbox reads each table in commit order by stamp. Update
// also settles the phase of each item the write touched, on the item's row, so
// the outbox ships the phase that ItemRecords.Phase decides.
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
	"slices"
	"sort"
	"strings"
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
	path  string
	write *sql.DB
	read  *sql.DB
	clock func() time.Time

	// itemColumns names each logged table's column that names an item.
	itemColumns []itemColumn

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
	s := &Store{path: path, clock: time.Now}
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

// converge brings the store to what the schema files declare, reading that
// from a scratch database the files are applied to. Where the store already
// holds tables and the files change one, it first copies the store beside
// itself, so an earlier release has a store to return to. It rebuilds each
// table whose column the files make nullable, adds each column a table lacks,
// applies each file in name order, checks that Tables declares every table the
// files create, guards each logged table, and starts the stamp clock past the
// newest stamp in any table.
func (s *Store) converge(ctx context.Context) error {
	names, err := fs.Glob(schemaFiles, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	files := make([]string, len(names))
	for i, name := range names {
		statements, err := schemaFiles.ReadFile(name)
		if err != nil {
			return err
		}
		files[i] = string(statements)
	}
	declared, err := sql.Open("sqlite", "file::memory:")
	if err != nil {
		return err
	}
	defer func() { _ = declared.Close() }()
	// One connection, because each connection to :memory: is its own database.
	declared.SetMaxOpenConns(1)
	for _, statements := range files {
		if _, err := declared.ExecContext(ctx, statements); err != nil {
			return fmt.Errorf("read the schema files: %w", err)
		}
	}
	if err := checkDeclared(ctx, declared); err != nil {
		return err
	}
	if s.itemColumns, err = itemColumns(ctx, declared); err != nil {
		return err
	}
	changes, err := planChanges(ctx, declared, s.write)
	if err != nil {
		return err
	}
	if changes.held && changes.any() {
		if err := s.copyBeside(ctx); err != nil {
			return err
		}
	}
	for _, table := range changes.rebuild {
		if err := s.rebuild(ctx, declared, table); err != nil {
			return err
		}
	}
	for _, add := range changes.add {
		definition := quote(add.column.name) + " " + add.column.kind
		if add.column.notNull {
			definition += " NOT NULL"
		}
		if add.column.dflt.Valid {
			definition += " DEFAULT " + add.column.dflt.String
		}
		if _, err := s.write.ExecContext(ctx, `ALTER TABLE `+add.table+` ADD COLUMN `+definition); err != nil {
			return fmt.Errorf("add column %s to %s: %w", add.column.name, add.table, err)
		}
	}
	for i, statements := range files {
		if _, err := s.write.ExecContext(ctx, statements); err != nil {
			return fmt.Errorf("apply %s: %w", names[i], err)
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

// schemaChanges is what converging the files changes in a store. A CREATE
// TABLE in a file never changes a table that exists, so a column the table
// lacks is added with its type, NOT NULL and default and no other constraint.
// A table whose column the files make nullable, or whose table constraints
// the files change, is rebuilt whole.
type schemaChanges struct {
	// held says the store held tables before this open.
	held    bool
	create  []string
	rebuild []string
	add     []columnAdd
}

type columnAdd struct {
	table  string
	column column
}

func (c schemaChanges) any() bool { return len(c.create)+len(c.rebuild)+len(c.add) > 0 }

// planChanges compares each table the files declare with the one live holds.
func planChanges(ctx context.Context, declared, live *sql.DB) (schemaChanges, error) {
	var changes schemaChanges
	held, err := tableNames(ctx, live)
	if err != nil {
		return changes, err
	}
	changes.held = len(held) > 0
	tables, err := tableNames(ctx, declared)
	if err != nil {
		return changes, err
	}
	for _, name := range tables {
		want, err := tableColumns(ctx, declared, name)
		if err != nil {
			return changes, err
		}
		have, err := tableColumns(ctx, live, name)
		if err != nil {
			return changes, err
		}
		if len(have) == 0 {
			changes.create = append(changes.create, name)
			continue
		}
		constrained, err := constraintsChanged(ctx, declared, live, name)
		if err != nil {
			return changes, err
		}
		if constrained {
			changes.rebuild = append(changes.rebuild, name)
			continue
		}
		notNull := map[string]bool{}
		for _, c := range have {
			notNull[c.name] = c.notNull
		}
		loosened := false
		var missing []columnAdd
		for _, c := range want {
			was, ok := notNull[c.name]
			switch {
			case !ok:
				missing = append(missing, columnAdd{table: name, column: c})
			case was && !c.notNull:
				loosened = true
			}
		}
		if loosened {
			changes.rebuild = append(changes.rebuild, name)
		} else {
			changes.add = append(changes.add, missing...)
		}
	}
	return changes, nil
}

// copyBeside writes a copy of the store beside it, named for the instant it
// was taken, before converge changes the store's schema. An earlier release
// refuses or misreads a store a later one converged, so returning to one
// starts from this copy.
func (s *Store) copyBeside(ctx context.Context) error {
	target := s.path + ".before-" + time.Now().UTC().Format("20060102T150405.000Z")
	if _, err := s.write.ExecContext(ctx, `VACUUM INTO ?`, target); err != nil {
		return fmt.Errorf("copy the store to %s before changing its schema: %w", target, err)
	}
	return nil
}

// constraintsChanged says whether the files declare table with other table
// constraints than live holds it with. A column that converge added to live
// carries no constraint of the table's, so only the table constraints compare.
func constraintsChanged(ctx context.Context, declared, live *sql.DB, table string) (bool, error) {
	var want, have string
	if err := declared.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&want); err != nil {
		return false, fmt.Errorf("read how the files create %s: %w", table, err)
	}
	if err := live.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&have); err != nil {
		return false, fmt.Errorf("read how the store holds %s: %w", table, err)
	}
	return !slices.Equal(tableConstraints(want), tableConstraints(have)), nil
}

// tableConstraints is each table constraint in a CREATE TABLE statement, such
// as a CHECK or a PRIMARY KEY over columns, with its whitespace collapsed,
// sorted. A column's own constraints are part of its definition, so none is
// listed.
func tableConstraints(create string) []string {
	open := strings.IndexByte(create, '(')
	if open < 0 {
		return nil
	}
	var parts []string
	depth, start := 0, open+1
	var quoted byte
	for i := open + 1; i < len(create); i++ {
		c := create[i]
		switch {
		case quoted != 0:
			if c == quoted {
				quoted = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quoted = c
		case c == '[':
			quoted = ']'
		case c == '(':
			depth++
		case c == ')' && depth == 0:
			parts = append(parts, create[start:i])
			i = len(create)
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, create[start:i])
			start = i + 1
		}
	}
	var out []string
	for _, part := range parts {
		fields := strings.Fields(part)
		if len(fields) == 0 {
			continue
		}
		word, _, _ := strings.Cut(strings.ToUpper(fields[0]), "(")
		switch word {
		case "CONSTRAINT", "CHECK", "PRIMARY", "UNIQUE", "FOREIGN":
			out = append(out, strings.Join(fields, " "))
		}
	}
	slices.Sort(out)
	return out
}

// rebuild remakes table as the files declare it and keeps every row, because
// SQLite can neither make a column nullable nor change a table constraint in
// place. It follows SQLite's documented procedure: foreign keys off, a new
// table, the rows copied, the old table dropped and the new one renamed, then
// a foreign key check, in one transaction. Applying the files and guarding the
// tables afterwards puts the table's indexes and triggers back.
func (s *Store) rebuild(ctx context.Context, declared *sql.DB, table string) error {
	var create string
	if err := declared.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&create); err != nil {
		return fmt.Errorf("read how the files create %s: %w", table, err)
	}
	body, ok := strings.CutPrefix(create, "CREATE TABLE "+table+" ")
	if !ok {
		return fmt.Errorf("the files create %s as %.40q, which rebuild cannot rename", table, create)
	}
	want, err := tableColumns(ctx, declared, table)
	if err != nil {
		return err
	}
	have, err := tableColumns(ctx, s.write, table)
	if err != nil {
		return err
	}
	held := map[string]bool{}
	for _, c := range have {
		held[c.name] = true
	}
	var kept []string
	for _, c := range want {
		if held[c.name] {
			kept = append(kept, quote(c.name))
		}
	}
	columns := strings.Join(kept, ", ")
	rebuilt := table + "_rebuilt"

	conn, err := s.write.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, `PRAGMA foreign_keys = ON`)
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("turn foreign keys off to rebuild %s: %w", table, err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`CREATE TABLE ` + rebuilt + ` ` + body,
		`INSERT INTO ` + rebuilt + ` (` + columns + `) SELECT ` + columns + ` FROM ` + table,
		`DROP TABLE ` + table,
		`ALTER TABLE ` + rebuilt + ` RENAME TO ` + table,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("rebuild %s: %w", table, err)
		}
	}
	var dangling int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pragma_foreign_key_check`).Scan(&dangling); err != nil {
		return fmt.Errorf("check foreign keys after rebuilding %s: %w", table, err)
	}
	if dangling > 0 {
		return fmt.Errorf("rebuilding %s left %d rows whose foreign key names no row", table, dangling)
	}
	return tx.Commit()
}

// checkDeclared reads the tables the schema files create, in db, and refuses
// one that Tables does not declare and unlogged does not name, a declared
// table no file creates, and a declared key other than the table's primary
// key. Leaving a table out of Tables would leave it unguarded and never
// shipped, with nothing to say so. It reads the files rather than the store,
// so a table that a later release added is left as it is.
func checkDeclared(ctx context.Context, db *sql.DB) error {
	names, err := tableNames(ctx, db)
	if err != nil {
		return err
	}
	created := map[string]bool{}
	for _, name := range names {
		created[name] = true
		if _, ok := TableNamed(name); !ok && !contains(unlogged, name) {
			return fmt.Errorf("the schema files create table %s, which store.Tables does not declare and unlogged does not name", name)
		}
	}
	for _, t := range Tables {
		if !created[t.Name] {
			return fmt.Errorf("store.Tables declares %s, which no schema file creates", t.Name)
		}
		columns, err := tableColumns(ctx, db, t.Name)
		if err != nil {
			return err
		}
		sort.Slice(columns, func(i, j int) bool { return columns[i].pk < columns[j].pk })
		var key []string
		for _, c := range columns {
			if c.pk > 0 {
				key = append(key, c.name)
			}
		}
		if strings.Join(key, ",") != strings.Join(t.Key, ",") {
			return fmt.Errorf("%s's primary key is (%s), and store.Tables declares its key as (%s)",
				t.Name, strings.Join(key, ", "), strings.Join(t.Key, ", "))
		}
	}
	return nil
}

func tableNames(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

type column struct {
	name    string
	kind    string
	notNull bool
	dflt    sql.NullString
	pk      int
}

func tableColumns(ctx context.Context, db *sql.DB, table string) ([]column, error) {
	rows, err := db.QueryContext(ctx, `SELECT name, type, "notnull", dflt_value, pk FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, fmt.Errorf("read %s's columns: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.name, &c.kind, &c.notNull, &c.dflt, &c.pk); err != nil {
			return nil, err
		}
		columns = append(columns, c)
	}
	return columns, rows.Err()
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
// stamp. Before it commits, it settles the phase of each item that fn touched.
func (s *Store) Update(ctx context.Context, fn func(*Tx) error) error {
	sqlTx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin a write: %w", err)
	}
	// The stamp is taken once the write lock is held, so it is in commit order.
	tx := &Tx{tx: sqlTx, at: s.stamp()}
	if _, err := sqlTx.ExecContext(ctx, `INSERT INTO write_stamp (id, stamp) VALUES (1, ?)
		ON CONFLICT (id) DO UPDATE SET stamp = excluded.stamp`, tx.stamp()); err != nil {
		_ = sqlTx.Rollback()
		return fmt.Errorf("stamp a write: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := s.settleTouched(ctx, tx); err != nil {
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
