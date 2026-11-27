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
	// Bool names the columns holding a boolean, which SQLite keeps as 0 or 1
	// and a read answers as false or true.
	Bool []string
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
		Name: "sightings", Key: []string{"item_key", "connector"}, Stamp: []string{"written_ts", "last_seen_ts"}, JSON: []string{"tags", "fields", "metrics"},
		Text: []string{"external_key", "fields"}, Kept: []string{"first_seen_ts"}, Writes: Upserts,
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
	{
		Name: "parse_units", Key: []string{"item_key", "extractor", "extractor_version", "n"}, Stamp: []string{"created_ts"},
		Text: []string{"label", "text"}, Writes: Appends,
	},
	{
		Name: "sections", Key: []string{"item_key", "producer", "producer_version", "n"}, Stamp: []string{"created_ts"},
		Text: []string{"title"}, Writes: Appends,
	},
	{Name: "replays", Key: []string{"id"}, Stamp: []string{"requested_ts"}, Writes: Appends},
	{Name: "decisions", Key: []string{"item_key", "gate", "sequence"}, Stamp: []string{"decided_ts"}, JSON: []string{"presented"}, Writes: Appends},
	{
		Name: "deliveries", Key: []string{"item_key", "destination"}, Stamp: []string{"updated_ts"}, Text: []string{"last_error", "title", "url", "path"},
		Writes: Upserts,
	},
	{
		Name: "tasks", Key: []string{"id"}, Stamp: []string{"updated_ts"}, Text: []string{"error"},
		Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{
		Name: "workers", Key: []string{"id"}, Stamp: []string{"heartbeat_ts"}, JSON: []string{"capabilities"},
		Kept: []string{"started_ts"}, Writes: Upserts,
	},
	{
		Name: "cursors", Key: []string{"connector"}, Stamp: []string{"updated_ts"}, Text: []string{"position", "last_error"},
		Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{
		Name: "declarations", Key: []string{"commit"}, Stamp: []string{"applied_ts"},
		JSON: []string{"connectors", "routing_rules", "destinations"}, Writes: Upserts,
	},
	{
		Name: "keyed_requests", Key: []string{"caller", "idempotency_key"}, Stamp: []string{"created_ts"},
		JSON: []string{"answer"}, Writes: Appends,
	},
	{Name: "marks", Key: []string{"item_key", "sequence"}, Stamp: []string{"marked_ts"}, JSON: []string{"families"}, Writes: Appends},
	{
		Name: "gate_trust", Key: []string{"family"}, Stamp: []string{"updated_ts"}, Bool: []string{"asks_promotion"},
		Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{
		Name: "gate_trust_decisions", Key: []string{"family", "sequence"}, Stamp: []string{"decided_ts"},
		Bool: []string{"promoted"}, Writes: Appends,
	},
	{
		Name: "prompt_proposals", Key: []string{"id"}, Stamp: []string{"updated_ts"}, JSON: []string{"misses", "tokens", "answer"},
		Kept: []string{"created_ts"}, Writes: Upserts,
	},
	{
		Name: "routing_rules", Key: []string{"name", "sequence"}, Stamp: []string{"decided_ts"}, JSON: []string{"match_values"},
		Bool: []string{"retired"}, Text: []string{"match_values"}, Writes: Appends,
	},
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

// IsBool says whether column holds a boolean.
func (t Table) IsBool(column string) bool { return contains(t.Bool, column) }

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

// unlogged names the tables the store keeps for itself, which the outbox never
// ships. Every other table a schema file creates is declared in Tables.
var unlogged = []string{"outbox_marks", "write_stamp"}

// guard makes the database refuse a write that would hide a version from the
// outbox: a row stamped other than by the write in flight, which write_stamp
// holds, and any update of an appended table. A write that forgets its stamp
// then fails at the write rather than leaving bronze short. Each trigger is
// made again on every open, so a change here reaches a store that exists.
func guard(ctx context.Context, db *sql.DB, t Table) error {
	stampedOtherwise := t.StampSQL("new") + ` IS NOT (SELECT stamp FROM write_stamp)`
	refuse := func(why string) string {
		return ` BEGIN SELECT RAISE(ABORT, '` + t.Name + ` ` + why + `'); END`
	}
	triggers := map[string]string{
		t.Name + "_logged_inserts_stamped": `BEFORE INSERT ON ` + t.Name + ` WHEN ` + stampedOtherwise +
			refuse("rows carry the stamp of the write that adds them"),
	}
	switch t.Writes {
	case Appends:
		triggers[t.Name+"_logged_appends_only"] = `BEFORE UPDATE ON ` + t.Name + refuse("is appended, so no row is updated")
	case Upserts:
		triggers[t.Name+"_logged_updates_stamped"] = `BEFORE UPDATE ON ` + t.Name + ` WHEN ` + stampedOtherwise +
			refuse("rows carry the stamp of the write that changes them")
	default:
		return fmt.Errorf("%s declares no way it is written", t.Name)
	}
	for name, body := range triggers {
		if _, err := db.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+name+`; CREATE TRIGGER `+name+` `+body); err != nil {
			return fmt.Errorf("guard how %s is written: %w", t.Name, err)
		}
	}
	return nil
}
