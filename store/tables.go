package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Writes is how the store writes a table, which decides how the outbox reads
// it past its mark and how a row's version counts.
type Writes int

const (
	// Appends inserts each row once and never updates it, so each row is one
	// version.
	Appends Writes = iota + 1
	// Upserts writes a row again in place, each write stamped later than the
	// last, so each write is a version.
	Upserts
)

// Table is one table the outbox ships each row of, as the store writes it.
type Table struct {
	Name string
	// Key names a row within the table. Its columns joined with a slash are
	// the record's ingestion.record.id, and an upsert conflicts on them.
	Key []string
	// Stamp is the columns whose first non-null value says when the row was
	// last written. Each write sets it from the transaction's stamp.
	Stamp []string
	// JSON names the columns holding JSON text.
	JSON []string
	// Text names the outsider-written columns, which ship as ingestion.text
	// and never as ingestion.record.
	Text []string
	// Kept names the columns written once, on insert, and never by an upsert.
	Kept   []string
	Writes Writes
}

// Tables is every table the outbox ships. A stage adding a table adds it
// here, beside its schema file.
var Tables = []Table{
	{
		Name: "items", Key: []string{"key"}, Stamp: []string{"updated_ts"}, Text: []string{"title", "url"},
		Kept: []string{"created_ts", "flow_id", "parent_flow_id"}, Writes: Upserts,
	},
	{
		Name: "sightings", Key: []string{"item_key", "connector"}, Stamp: []string{"last_seen_ts"}, JSON: []string{"tags"},
		Text: []string{"external_key"}, Kept: []string{"first_seen_ts"}, Writes: Upserts,
	},
	{Name: "blobs", Key: []string{"sha256"}, Stamp: []string{"created_ts"}, Writes: Appends},
	{
		Name: "homes", Key: []string{"blob_sha256", "root", "path_sha256"}, Stamp: []string{"gone_ts", "found_ts"},
		Text: []string{"path"}, Writes: Upserts,
	},
	{
		Name: "outputs", Key: []string{"item_key", "stage", "producer", "producer_version"}, Stamp: []string{"updated_ts"},
		JSON: []string{"tokens", "findings", "units", "spans", "answer"},
		Text: []string{"text", "units", "spans", "answer", "error"}, Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{Name: "decisions", Key: []string{"item_key", "gate", "sequence"}, Stamp: []string{"decided_ts"}, Writes: Appends},
	{Name: "deliveries", Key: []string{"item_key", "destination"}, Stamp: []string{"updated_ts"}, Writes: Upserts},
	{
		Name: "tasks", Key: []string{"id"}, Stamp: []string{"updated_ts"}, Text: []string{"error"},
		Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{
		Name: "workers", Key: []string{"id"}, Stamp: []string{"heartbeat_ts"}, JSON: []string{"capabilities"},
		Kept: []string{"started_ts"}, Writes: Upserts,
	},
	{Name: "cursors", Key: []string{"connector"}, Stamp: []string{"updated_ts"}, Writes: Upserts},
	{Name: "declarations", Key: []string{"commit"}, Stamp: []string{"applied_ts"}, JSON: []string{"connectors"}, Writes: Appends},
}

// TableNamed is the table called name, and false where none is.
func TableNamed(name string) (Table, bool) {
	for _, t := range Tables {
		if t.Name == name {
			return t, true
		}
	}
	return Table{}, false
}

// StampSQL is the expression reading the row's stamp, each column qualified
// by qualifier where it is not empty, such as new in a trigger.
func (t Table) StampSQL(qualifier string) string {
	columns := make([]string, len(t.Stamp))
	for i, c := range t.Stamp {
		columns[i] = quote(c)
		if qualifier != "" {
			columns[i] = qualifier + "." + columns[i]
		}
	}
	if len(columns) == 1 {
		return columns[0]
	}
	return "coalesce(" + strings.Join(columns, ", ") + ")"
}

// IsText says whether an outsider wrote column.
func (t Table) IsText(column string) bool { return contains(t.Text, column) }

// IsJSON says whether column holds JSON text.
func (t Table) IsJSON(column string) bool { return contains(t.JSON, column) }

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// quote is column as an SQL identifier, since commit is a keyword.
func quote(column string) string { return `"` + column + `"` }

// guard makes the database refuse a write that would hide a version from the
// outbox: any update of an appended table, and an update of an upserted table
// that stamps a row earlier than its last write. A stage that writes otherwise
// then fails in its own tests rather than leaving bronze short.
func guard(ctx context.Context, db *sql.DB, t Table) error {
	var statement string
	name := t.Name + "_logged_"
	switch t.Writes {
	case Appends:
		name += "appends_only"
		statement = `CREATE TRIGGER IF NOT EXISTS ` + name + ` BEFORE UPDATE ON ` + t.Name + `
			BEGIN SELECT RAISE(ABORT, '` + t.Name + ` is appended, so no row is updated'); END`
	case Upserts:
		name += "stamp_rises"
		statement = `CREATE TRIGGER IF NOT EXISTS ` + name + ` BEFORE UPDATE ON ` + t.Name + `
			WHEN ` + t.StampSQL("new") + ` < ` + t.StampSQL("old") + `
			BEGIN SELECT RAISE(ABORT, '` + t.Name + ` is upserted, so each write stamps a row no earlier than its last'); END`
	default:
		return fmt.Errorf("%s declares no way it is written", t.Name)
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("guard how %s is written: %w", t.Name, err)
	}
	return nil
}
