package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrNoParse is a read of an item that holds no done parse to match it.
var ErrNoParse = errors.New("no done parse")

// ErrUnitsOutOfOrder is a parse whose units are not numbered 1, 2, 3 in order.
var ErrUnitsOutOfOrder = errors.New("a parse's units are numbered from 1 in their order")

// PutUnits writes the units that extractor at version made of the item under
// key. A version's units are written once, so where that version already
// wrote units, they stay as they are and units is not read. A changed
// extractor takes a new version, which writes beside the old.
func (t *Tx) PutUnits(ctx context.Context, key, extractor, version string, units []Unit) error {
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

// Parse is one item's done parse with its units, and the newest done scan.
type Parse struct {
	Item   Item
	Output Output
	Units  []Unit
	// Scan is the item's newest done scan output, and nil where none is.
	Scan *Output
}

// Text is the parse's whole text: its units' text in order, joined by a blank
// line.
func (p Parse) Text() string {
	texts := make([]string, len(p.Units))
	for i, u := range p.Units {
		texts[i] = u.Text
	}
	return strings.Join(texts, "\n\n")
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
	var p Parse
	var found bool
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
	if p.Output, found, err = first[Output](ctx, tx, table("outputs"), where+` ORDER BY created_ts DESC, rowid DESC`, args...); err != nil {
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
	scan, found, err := first[Output](ctx, tx, table("outputs"), `item_key = ? AND stage = ? AND state = ? ORDER BY created_ts DESC, rowid DESC`,
		key, StageScan, OutputDone)
	if found {
		p.Scan = &scan
	}
	return p, err
}

// parseVersion names one extractor version's parse of one item.
type parseVersion struct{ item, extractor, version string }

// unitCounts counts each parse's units among the items under keys.
func (s *Store) unitCounts(ctx context.Context, keys []any) (map[parseVersion]int, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT item_key, extractor, extractor_version, count(*) FROM parse_units
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
