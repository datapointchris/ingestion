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
// holds. Where o's stage quotes a parse and o names none, o first names the
// parse that its stage quotes now. Only an answer replaces a done output. A
// later one that is not done is dropped where it answers the same note over
// the same parse, so a rerun that fails leaves the done output readable and
// fails only its task. One over another parse waits beside the done output
// as its replacement, which holds the call's dispatch, until the call
// answers. A model read of a new note replaces the done read. A done parse is
// kept against any later one, output and units alike. A changed extractor
// takes a new version, which writes beside the old.
func (t *Tx) PutOutputWithUnits(ctx context.Context, o Output, units []Unit) error {
	if o.Stage != StageParse && len(units) > 0 {
		return fmt.Errorf("%w: a %s output carries %d units", ErrUnitsOutsideParse, o.Stage, len(units))
	}
	if err := t.nameQuotedInput(ctx, &o); err != nil {
		return err
	}
	var state string
	var note, inputProducer, inputVersion sql.NullString
	err := t.tx.QueryRowContext(ctx, `SELECT state, note_digest, input_producer, input_version FROM outputs
		WHERE item_key = ? AND stage = ? AND producer = ? AND producer_version = ?`,
		o.ItemKey, o.Stage, o.Producer, o.ProducerVersion).Scan(&state, &note, &inputProducer, &inputVersion)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read %s's %s output by %s %s: %w", o.ItemKey, o.Stage, o.Producer, o.ProducerVersion, err)
	case err == nil && state == OutputDone && o.Stage == StageParse:
		return nil
	case err == nil && state == OutputDone && o.State != OutputDone && note.String == deref(o.NoteDigest):
		if SameParse(InputOf(nullable(inputProducer), nullable(inputVersion)), o.Input()) {
			return nil
		}
		o.CreatedTS, o.UpdatedTS = t.stamp(), t.stamp()
		return t.put(ctx, replacements, o)
	}
	if err := t.PutOutput(ctx, o); err != nil {
		return err
	}
	if o.State == OutputDone && o.Stage != StageParse {
		if _, err := t.tx.ExecContext(ctx, `DELETE FROM output_replacements
			WHERE item_key = ? AND stage = ? AND producer = ? AND producer_version = ?`,
			o.ItemKey, o.Stage, o.Producer, o.ProducerVersion); err != nil {
			return fmt.Errorf("drop %s's %s replacement by %s %s: %w", o.ItemKey, o.Stage, o.Producer, o.ProducerVersion, err)
		}
	}
	if o.Stage != StageParse {
		return nil
	}
	return t.putUnits(ctx, o.ItemKey, o.Producer, o.ProducerVersion, units)
}

// replacements is output_replacements, the outputs that wait beside a done
// output until their call answers. It has the outputs table's columns, and the
// store keeps it for itself.
var replacements = func() Table {
	t := table("outputs")
	t.Name = "output_replacements"
	return t
}()

// Replacements reads the item's outputs at stage that wait beside a done
// output until their call answers.
func (t *Tx) Replacements(ctx context.Context, key, stage string) ([]Output, error) {
	return readInto[Output](ctx, t.tx, replacements, `item_key = ? AND stage = ? ORDER BY created_ts, rowid`, key, stage)
}

// nullable is s as a pointer, nil where s is null.
func nullable(s sql.NullString) *string {
	if !s.Valid {
		return nil
	}
	return &s.String
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

// PickedParse is the done parse among outputs, oldest first as Tx.Outputs
// reads them, that pick names: the newest of pick's extractor and version
// where each is set, a preferred extractor's first, as Parse reads it. It is
// nil where none is done.
func PickedParse(outputs []Output, pick ParsePick) *Output {
	var parse *Output
	for i := range outputs {
		o := &outputs[i]
		if o.Stage != StageParse || o.State != OutputDone || (pick.Extractor != "" && o.Producer != pick.Extractor) ||
			(pick.Version != "" && o.ProducerVersion != pick.Version) {
			continue
		}
		if parse == nil || readingRank(*o) >= readingRank(*parse) {
			parse = o
		}
	}
	return parse
}

// ReadingParse is the item's reading parse among outputs: its newest done
// parse by a preferred extractor where it holds one, and its newest done
// parse otherwise. Every reader of the item's text reads it.
func ReadingParse(outputs []Output) *Output { return PickedParse(outputs, ParsePick{}) }

// ReadingParseScanned says whether outputs, oldest first as Tx.Outputs reads
// them, hold a done scan of the item's reading parse. The side gate places an
// item only then, so no item is placed before the scan of the parse that its
// readers read.
func ReadingParseScanned(outputs []Output) bool {
	parse := ReadingParse(outputs)
	return parse != nil && slices.ContainsFunc(outputs, func(o Output) bool {
		return o.Stage == StageScan && o.State == OutputDone && o.ReadParse(parse)
	})
}

// ParseRef names one parse by its extractor and version.
type ParseRef struct {
	Extractor string `json:"extractor"`
	Version   string `json:"version"`
}

// InputOf is the parse that an output's input producer and version name, and
// nil where they name none.
func InputOf(producer, version *string) *ParseRef {
	if producer == nil || version == nil {
		return nil
	}
	return &ParseRef{Extractor: *producer, Version: *version}
}

// SameParse says whether a and b name one parse, or both name none. Each
// test of whether an output read a parse asks it.
func SameParse(a, b *ParseRef) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// Input is the parse that o names as the one it read, and nil where it names
// none.
func (o Output) Input() *ParseRef { return InputOf(o.InputProducer, o.InputVersion) }

// Ref names o, a parse, and is nil where o is nil.
func (o *Output) Ref() *ParseRef {
	if o == nil {
		return nil
	}
	return &ParseRef{Extractor: o.Producer, Version: o.ProducerVersion}
}

// ReadParse says whether o names parse as the parse it read.
func (o Output) ReadParse(parse *Output) bool {
	return parse != nil && SameParse(o.Input(), parse.Ref())
}

// QuotedPick is the pick of the parse that stage's prompt quotes, as
// modelStageQuotes declares it, and false where the stage quotes none.
func QuotedPick(stage string) (ParsePick, bool) {
	for _, s := range modelStageQuotes {
		if s.stage == stage {
			return s.quotes, true
		}
	}
	return ParsePick{}, false
}

// QuotedParse is the done parse among outputs that stage's prompt quotes now,
// and nil where it quotes none or the item holds none.
func QuotedParse(outputs []Output, stage string) *Output {
	pick, ok := QuotedPick(stage)
	if !ok {
		return nil
	}
	return PickedParse(outputs, pick)
}

// ReadsQuotedParse says whether o, a stage's output among the item's outputs,
// read the parse that its stage's prompt quotes now. An answer over an older
// parse is not current, so its stage is due on the item again. A stage that
// quotes no parse, such as a prompt proposal's draft, answers over none, so
// no parse makes its output stale.
func ReadsQuotedParse(o Output, outputs []Output) bool {
	if _, ok := QuotedPick(o.Stage); !ok {
		return true
	}
	return o.ReadParse(QuotedParse(outputs, o.Stage))
}

// nameQuotedInput names on o the parse that its stage quotes now of its item,
// where o's stage quotes a parse and o names none. A worker of an older build
// reports none, so serve names it in the write that keeps the output.
func (t *Tx) nameQuotedInput(ctx context.Context, o *Output) error {
	pick, quotes := QuotedPick(o.Stage)
	if !quotes || o.Input() != nil {
		return nil
	}
	outputs, err := t.Outputs(ctx, o.ItemKey)
	if err != nil {
		return err
	}
	if parse := PickedParse(outputs, pick); parse != nil {
		producer, version := parse.Producer, parse.ProducerVersion
		o.InputProducer, o.InputVersion = &producer, &version
	}
	return nil
}

// fillQuotedInputs names, on each output of a stage whose prompt quotes a
// parse, where the output names none, the parse that its stage quotes now.
// Open runs it only on the open that raises a store to
// namedInputsGeneration. Such an output was written before a stage recorded
// the parse it read, so it takes the parse that its readers read then, and no
// item whose parse never changed is due again.
func (s *Store) fillQuotedInputs(ctx context.Context) (int, error) {
	filled := 0
	err := s.Update(ctx, func(tx *Tx) error {
		filled = 0
		args := make([]any, len(modelStages))
		for i, stage := range modelStages {
			args[i] = stage
		}
		keys, err := tx.ids(ctx, `SELECT DISTINCT item_key FROM outputs WHERE input_producer IS NULL
			AND stage IN (?`+strings.Repeat(", ?", len(modelStages)-1)+`) ORDER BY item_key`, args...)
		if err != nil {
			return fmt.Errorf("find each output that names no parse: %w", err)
		}
		for _, key := range keys {
			outputs, err := tx.Outputs(ctx, key)
			if err != nil {
				return err
			}
			for _, o := range outputs {
				parse := QuotedParse(outputs, o.Stage)
				if o.InputProducer != nil || parse == nil {
					continue
				}
				if _, err := tx.tx.ExecContext(ctx, `UPDATE outputs SET input_producer = ?, input_version = ?, updated_ts = ?
					WHERE item_key = ? AND stage = ? AND producer = ? AND producer_version = ?`,
					parse.Producer, parse.ProducerVersion, tx.stamp(), key, o.Stage, o.Producer, o.ProducerVersion); err != nil {
					return fmt.Errorf("name the parse that %s's %s output read: %w", key, o.Stage, err)
				}
				filled++
			}
		}
		return nil
	})
	return filled, err
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
	tx, err := s.snapshot(ctx)
	if err != nil {
		return Parse{}, err
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
