package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrNoParse is a read of an item that holds no done parse to match it.
var ErrNoParse = errors.New("no done parse")

// ErrUnitsOutOfOrder is a parse whose units are not numbered 1, 2, 3 in order.
var ErrUnitsOutOfOrder = errors.New("a parse's units are numbered from 1 in their order")

// ErrUnitsOutsideParse is a stage other than parse reporting units.
var ErrUnitsOutsideParse = errors.New("only a parse holds units")

// PutOutputWithUnits writes o, a stage's output, and the units that only a parse
// holds. A done output is kept against a later one that is not done and
// answers the same note, so a rerun that fails leaves the done output readable
// and fails only its task. A model read of a new note replaces it, so its
// dispatch is stored. A done parse is kept against any later one, output and
// units alike. A changed extractor takes a new version, which writes beside
// the old.
func (t *Tx) PutOutputWithUnits(ctx context.Context, o Output, units []Unit) error {
	if o.Stage != StageParse && len(units) > 0 {
		return fmt.Errorf("%w: a %s output carries %d units", ErrUnitsOutsideParse, o.Stage, len(units))
	}
	var state string
	var note sql.NullString
	err := t.tx.QueryRowContext(ctx, `SELECT state, note_digest FROM outputs WHERE item_key = ? AND stage = ? AND producer = ? AND producer_version = ?`,
		o.ItemKey, o.Stage, o.Producer, o.ProducerVersion).Scan(&state, &note)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read %s's %s output by %s %s: %w", o.ItemKey, o.Stage, o.Producer, o.ProducerVersion, err)
	case err == nil && state == OutputDone && (o.Stage == StageParse || (o.State != OutputDone && note.String == deref(o.NoteDigest))):
		return nil
	}
	if err := t.PutOutput(ctx, o); err != nil {
		return err
	}
	if o.Stage != StageParse {
		return nil
	}
	return t.putUnits(ctx, o.ItemKey, o.Producer, o.ProducerVersion, units)
}

// putUnits writes the units that extractor at version made of the item under
// key, once. Where that version already wrote units, they stay as they are.
func (t *Tx) putUnits(ctx context.Context, key, extractor, version string, units []Unit) error {
	for i, u := range units {
		if u.N != i+1 {
			return fmt.Errorf("%w: unit %d is numbered %d", ErrUnitsOutOfOrder, i+1, u.N)
		}
	}
	var held int
	if err := t.tx.QueryRowContext(ctx, `SELECT count(*) FROM parse_units WHERE item_key = ? AND extractor = ? AND extractor_version = ?`,
		key, extractor, version).Scan(&held); err != nil {
		return fmt.Errorf("count %s's units from %s v%s: %w", key, extractor, version, err)
	}
	if held > 0 {
		return nil
	}
	for _, u := range units {
		row := ParseUnit{ItemKey: key, Extractor: extractor, ExtractorVersion: version, N: u.N, Label: u.Label, Text: u.Text, CreatedTS: t.stamp()}
		if err := t.put(ctx, table("parse_units"), row); err != nil {
			return err
		}
	}
	return nil
}

// ParsePick picks one of an item's done parses: the newest, of Extractor and
// at Version where each is set.
type ParsePick struct {
	Extractor string
	Version   string
}

// Parse is one item's done parse with its units, and the newest done scan of it.
type Parse struct {
	Item   Item
	Output Output
	Units  []Unit
	// Scan is the newest done scan that read this parse, and nil where none did.
	Scan *Output
}

// Text is the parse's whole text: its units' text in order, joined by a blank
// line.
func (p Parse) Text() string {
	text, _ := JoinUnits(p.Units)
	return text
}

// JoinUnits joins units' text in order by a blank line, and answers where each
// unit's text starts in the joined text.
func JoinUnits(units []Unit) (text string, starts []int) {
	var b strings.Builder
	starts = make([]int, len(units))
	for i, u := range units {
		if i > 0 {
			b.WriteString("\n\n")
		}
		starts[i] = b.Len()
		b.WriteString(u.Text)
	}
	return b.String(), starts
}

// ScanVersion is the producer version of a scan by rules that read the parse
// extractor wrote at extractorVersion. The parse is part of it, so a scan of
// each parse keeps its own row beside the parse it read.
func ScanVersion(rules, extractor, extractorVersion string) string {
	return rules + "/" + extractor + "@" + extractorVersion
}

// preferredParsers names each extractor that PreferParse registered.
var preferredParsers []string

// PreferParse registers extractor as preferred: an item's readers take its
// done parse over any other extractor's, whichever was written later. An extractor
// that reads a selected item better than its primary one registers so, from
// its package's init. Every reader of a parse, from the scan to the item's
// parse route, then reads the same one, and a newer parse by the primary
// extractor never takes its place.
func PreferParse(extractor string) {
	if !slices.Contains(preferredParsers, extractor) {
		preferredParsers = append(preferredParsers, extractor)
	}
}

// readingRank ranks a done parse by its extractor, a preferred one first.
func readingRank(o Output) int {
	if slices.Contains(preferredParsers, o.Producer) {
		return 1
	}
	return 0
}

// ReadingParseScanned says whether outputs, oldest first as Tx.Outputs reads
// them, hold a done scan of the item's reading parse: its newest done parse
// by a preferred extractor where it holds one, and its newest done parse
// otherwise. The side gate places an item only then, so no item is placed
// before the scan of the parse that its readers read.
func ReadingParseScanned(outputs []Output) bool {
	var parse *Output
	for i := range outputs {
		if outputs[i].Stage == StageParse && outputs[i].State == OutputDone && (parse == nil || readingRank(outputs[i]) >= readingRank(*parse)) {
			parse = &outputs[i]
		}
	}
	return parse != nil && slices.ContainsFunc(outputs, func(o Output) bool {
		return o.Stage == StageScan && o.State == OutputDone && o.InputProducer != nil && *o.InputProducer == parse.Producer &&
			o.InputVersion != nil && *o.InputVersion == parse.ProducerVersion
	})
}

// ScanRules is the rules version within a scan's producer version.
func ScanRules(producerVersion string) string {
	rules, _, _ := strings.Cut(producerVersion, "/")
	return rules
}

// Parse reads the done parse of the item under key that pick names, with its
// units. It answers ErrNoItem where the store holds no such item, and
// ErrNoParse where no done parse matches. It reads in one transaction, so the
// units are the ones written with the output.
func (s *Store) Parse(ctx context.Context, key string, pick ParsePick) (Parse, error) {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Parse{}, fmt.Errorf("begin a read: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return parseOf(ctx, tx, key, pick)
}

// parseOf reads the done parse of the item under key that pick names, with
// its units, through tx.
func parseOf(ctx context.Context, tx querier, key string, pick ParsePick) (Parse, error) {
	var p Parse
	var found bool
	var err error
	if p.Item, found, err = first[Item](ctx, tx, table("items"), `key = ?`, key); err != nil {
		return p, err
	} else if !found {
		return p, fmt.Errorf("%s: %w", key, ErrNoItem)
	}
	where, args := `item_key = ? AND stage = ? AND state = ?`, []any{key, StageParse, OutputDone}
	if pick.Extractor != "" {
		where, args = where+` AND producer = ?`, append(args, pick.Extractor)
	}
	if pick.Version != "" {
		where, args = where+` AND producer_version = ?`, append(args, pick.Version)
	}
	order := ` ORDER BY created_ts DESC, rowid DESC`
	if len(preferredParsers) > 0 {
		order = ` ORDER BY producer IN (?` + strings.Repeat(`, ?`, len(preferredParsers)-1) + `) DESC, created_ts DESC, rowid DESC`
		for _, name := range preferredParsers {
			args = append(args, name)
		}
	}
	if p.Output, found, err = first[Output](ctx, tx, table("outputs"), where+order, args...); err != nil {
		return p, err
	} else if !found {
		return p, fmt.Errorf("%s: %w", key, ErrNoParse)
	}
	units, err := readInto[ParseUnit](ctx, tx, table("parse_units"), `item_key = ? AND extractor = ? AND extractor_version = ? ORDER BY n`,
		key, p.Output.Producer, p.Output.ProducerVersion)
	if err != nil {
		return p, err
	}
	p.Units = make([]Unit, len(units))
	for i, u := range units {
		p.Units[i] = Unit{N: u.N, Label: u.Label, Text: u.Text}
	}
	scan, found, err := first[Output](ctx, tx, table("outputs"),
		`item_key = ? AND stage = ? AND state = ? AND input_producer = ? AND input_version = ? ORDER BY created_ts DESC, rowid DESC`,
		key, StageScan, OutputDone, p.Output.Producer, p.Output.ProducerVersion)
	if found {
		p.Scan = &scan
	}
	return p, err
}

// parseVersion names one extractor version's parse of one item.
type parseVersion struct{ item, extractor, version string }

// unitCounts counts each parse's units among the items under keys.
func unitCounts(ctx context.Context, db querier, keys []any) (map[parseVersion]int, error) {
	rows, err := db.QueryContext(ctx, `SELECT item_key, extractor, extractor_version, count(*) FROM parse_units
		WHERE item_key IN (?`+strings.Repeat(", ?", len(keys)-1)+`) GROUP BY item_key, extractor, extractor_version`, keys...)
	if err != nil {
		return nil, fmt.Errorf("count each parse's units: %w", err)
	}
	defer func() { _ = rows.Close() }()
	counts := map[parseVersion]int{}
	for rows.Next() {
		var v parseVersion
		var n int
		if err := rows.Scan(&v.item, &v.extractor, &v.version, &n); err != nil {
			return nil, err
		}
		counts[v] = n
	}
	return counts, rows.Err()
}

// UnitCount counts the units of the parse o, and is false where o is no
// parse.
func (r ItemRecords) UnitCount(o Output) (int, bool) {
	if o.Stage != StageParse {
		return 0, false
	}
	return r.unitCounts[parseVersion{o.ItemKey, o.Producer, o.ProducerVersion}], true
}

// NoUsableParse says whether the item's parse failed and no parse is done, so
// it waits in review rather than passing a gate on empty text.
func (r ItemRecords) NoUsableParse() bool {
	if c, ok := r.outputCondition(StageParse); ok {
		return c.Status == ConditionFalse
	}
	for _, t := range r.Tasks {
		if t.Stage == StageParse && t.State == TaskFailed {
			return true
		}
	}
	return false
}
