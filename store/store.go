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
	"sync/atomic"
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
	// inputsNamed counts the outputs that this open named their parse on.
	inputsNamed int
	// phasesSettled says that SettlePhases settled every item's stored phase
	// under this build's Phase, which every write keeps current after.
	phasesSettled atomic.Bool

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
// It then names on each output of a stage whose prompt quotes a parse, where
// the output names none, the parse that its stage quotes.
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
	held, err := s.generation(ctx)
	if err == nil {
		err = s.converge(ctx)
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	if held < namedInputsGeneration {
		if s.inputsNamed, err = s.fillQuotedInputs(ctx); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	if held < SchemaGeneration {
		if _, err := s.write.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, SchemaGeneration)); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("stamp the store's schema generation: %w", err)
		}
	}
	return s, nil
}

// generation is the schema generation that the store holds, 0 where it holds
// no tables yet.
func (s *Store) generation(ctx context.Context) (int, error) {
	var generation int
	if err := s.write.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&generation); err != nil {
		return 0, fmt.Errorf("read the store's schema generation: %w", err)
	}
	return generation, nil
}

// namedInputsGeneration is the generation from which each output of a stage
// whose prompt quotes a parse names that parse. The open that raises a store
// to it names the parse on each stored output that names none.
const namedInputsGeneration = 4

// InputsNamedAtOpen counts the stored outputs that Open named the parse they
// read on, each of a stage whose prompt quotes a parse.
func (s *Store) InputsNamedAtOpen() int { return s.inputsNamed }

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

// SchemaGeneration is the generation of the schema files. Raise it with any
// change to them that rebuilds a table: a column made nullable, a CHECK
// changed, or a table constraint changed. Raise it too with a fill of stored
// rows that Open runs once, as namedInputsGeneration names. A store keeps the
// highest generation that converged it in PRAGMA user_version. A build whose
// generation is older rebuilds none of its tables, so a release reverted
// after a later one converged the store keeps that release's columns and
// constraints.
const SchemaGeneration = 4

// converge brings the store to what the schema files declare, reading that
// from a scratch database the files are applied to. Where the store already
// holds tables and the files change one, it first copies the store beside
// itself, so an earlier release has a store to return to. It rebuilds each
// table whose column the files make nullable, or whose CHECK or table
// constraints they change, unless a later generation converged the store. It
// adds each column a table lacks, applies each file in name order, checks
// that Tables declares every table the files create, guards each logged
// table, and starts the stamp clock past the newest stamp in any table. Open
// stamps the store's generation once its fills ran.
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
	generation, err := s.generation(ctx)
	if err != nil {
		return err
	}
	if generation > SchemaGeneration {
		changes.rebuild = nil
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
// A table whose column the files make nullable, or whose CHECK or table
// constraints the files change, is rebuilt whole.
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
		loosened, err := checksChanged(ctx, declared, live, name)
		if err != nil {
			return changes, err
		}
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

// checksChanged says whether live holds a CHECK on table that declared does
// not, such as one the files widen to take a new state. SQLite cannot change a
// CHECK in place, so such a table is rebuilt. A CHECK the files declare and
// live lacks is on a column added later, which converge adds with no other
// constraint, so it changes nothing.
func checksChanged(ctx context.Context, declared, live *sql.DB, table string) (bool, error) {
	want, err := tableChecks(ctx, declared, table)
	if err != nil {
		return false, err
	}
	have, err := tableChecks(ctx, live, table)
	if err != nil {
		return false, err
	}
	for _, check := range have {
		if !slices.Contains(want, check) {
			return true, nil
		}
	}
	return false, nil
}

// tableChecks reads the expression of each CHECK in table's CREATE TABLE, with
// its runs of whitespace made one space.
func tableChecks(ctx context.Context, db *sql.DB, table string) ([]string, error) {
	var create string
	if err := db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&create); err != nil {
		return nil, fmt.Errorf("read how %s is created: %w", table, err)
	}
	var checks []string
	upper := strings.ToUpper(create)
	for at := 0; ; {
		found := strings.Index(upper[at:], "CHECK")
		if found < 0 {
			return checks, nil
		}
		start := at + found
		at = start + len("CHECK")
		if start > 0 && isWordByte(upper[start-1]) || at < len(upper) && isWordByte(upper[at]) {
			continue
		}
		open := at + len(upper[at:]) - len(strings.TrimLeft(upper[at:], " \t\r\n"))
		if open >= len(create) || create[open] != '(' {
			continue
		}
		end, ok := closingParen(create, open)
		if !ok {
			return nil, fmt.Errorf("%s's CHECK at byte %d never closes", table, start)
		}
		checks = append(checks, strings.Join(strings.Fields(create[open+1:end]), " "))
		at = end + 1
	}
}

// closingParen is the index of the parenthesis that closes the one at open in
// s, skipping any inside a quoted string or name.
func closingParen(s string, open int) (int, bool) {
	depth := 0
	var quote byte
	for i := open; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
	}
	return 0, false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

// copyBeside writes a copy of the store beside it, named for the instant it
// was taken, before converge changes the store's schema. An earlier release
// refuses or misreads a store a later one converged, so returning to one
// starts from this copy. SQLite leaves a copy it was stopped partway through,
// so a failed copy is removed, or each canceled start would leave one more.
func (s *Store) copyBeside(ctx context.Context) error {
	target := s.path + ".before-" + time.Now().UTC().Format("20060102T150405.000Z")
	if _, err := s.write.ExecContext(ctx, `VACUUM INTO ?`, target); err != nil {
		err = fmt.Errorf("copy the store to %s before changing its schema: %w", target, err)
		if rmErr := os.Remove(target); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove the partial copy: %w", rmErr))
		}
		return err
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
// SQLite can neither make a column nullable nor change a CHECK or a table
// constraint in place. It follows SQLite's documented procedure: foreign keys
// off, a new table, the rows copied, the old table dropped and the new one
// renamed, then a foreign key check, in one transaction. Applying the files
// and guarding the tables afterwards puts the table's indexes and triggers
// back.
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

// snapshot begins a read-only transaction, so every query through it reads
// one committed state. A write that ends a task writes its outputs beside
// it, and a reader that queried each on its own could see one without the
// other. The caller rolls it back.
func (s *Store) snapshot(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin a read: %w", err)
	}
	return tx, nil
}

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
