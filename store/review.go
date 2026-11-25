package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Why an item waits on a person where no side decision sent it to review, in
// the words of schemas/review-list.schema.json. Every other reason is the
// cause of the side decision that sent the item to review.
const (
	ReasonNoParse       = "no-parse"
	ReasonQualityReview = "quality-review"
)

// Waiting is one item that waits on a person's placement.
type Waiting struct {
	ItemRecords
	// Reason is why it waits, in the words of schemas/review-list.schema.json.
	Reason string
	// Since is when it began to wait.
	Since string
}

// WaitsOnPerson says why the item waits on a person's placement, and since
// when. It is the one answer that Phase, the review queue and each connector's
// count read. An item waits where its newest side decision sent it to review,
// under that decision's cause. Where no side decision holds it, the item waits
// as no-parse where no parse is usable, and as quality-review where the quality
// gate's newest decision sent it there. It is false where the item waits on
// nobody.
func (r ItemRecords) WaitsOnPerson() (reason, since string, ok bool) {
	if side, sided := r.NewestDecision(GateSide); sided {
		return side.Cause, side.DecidedTS, side.Verdict == "review"
	}
	if r.NoUsableParse() {
		since = r.Item.CreatedTS
		for _, t := range r.Tasks {
			if t.Stage == StageParse && t.State == TaskFailed && t.UpdatedTS > since {
				since = t.UpdatedTS
			}
		}
		return ReasonNoParse, since, true
	}
	if quality, decided := r.NewestDecision(GateQuality); decided && quality.Verdict == "review" {
		return ReasonQualityReview, quality.DecidedTS, true
	}
	return "", "", false
}

// reviewCandidates selects every item that may wait on a person, as a superset
// that WaitsOnPerson narrows: each one that a side decision sent to review, and
// each one that holds no side decision.
const reviewCandidates = `
SELECT DISTINCT d.item_key FROM decisions d WHERE d.gate = 'side' AND d.verdict = 'review'
UNION
SELECT i.key FROM items i WHERE NOT EXISTS (SELECT 1 FROM decisions x WHERE x.item_key = i.key AND x.gate = 'side')`

// ReviewQueue reads every item that waits on a person's placement, the
// longest waiting first.
func (s *Store) ReviewQueue(ctx context.Context) ([]Waiting, error) {
	keys, err := s.reviewCandidateKeys(ctx)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []Waiting{}, nil
	}
	items, err := readInto[Item](ctx, s.read, table("items"), `key IN (?`+strings.Repeat(", ?", len(keys)-1)+`)`, keys...)
	if err != nil {
		return nil, err
	}
	records, err := recordsOf(ctx, s.read, items)
	if err != nil {
		return nil, err
	}
	out := make([]Waiting, 0, len(records))
	for _, r := range records {
		if reason, since, ok := r.WaitsOnPerson(); ok {
			out = append(out, Waiting{ItemRecords: r, Reason: reason, Since: since})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Since != out[j].Since {
			return out[i].Since < out[j].Since
		}
		return out[i].Item.Key < out[j].Item.Key
	})
	return out, nil
}

// personPlaced selects every item whose newest side decision a person made.
const personPlaced = `
SELECT d.item_key FROM decisions d
WHERE d.gate = 'side' AND d.decided_by LIKE 'person:%'
	AND d.sequence = (SELECT max(x.sequence) FROM decisions x WHERE x.item_key = d.item_key AND x.gate = 'side')`

// PersonPlacedItems reads every item whose newest side decision a person
// made, with its records, oldest first.
func (s *Store) PersonPlacedItems(ctx context.Context) ([]ItemRecords, error) {
	keys, err := s.selectKeys(ctx, personPlaced, "select the items a person placed")
	if err != nil || len(keys) == 0 {
		return []ItemRecords{}, err
	}
	items, err := readInto[Item](ctx, s.read, table("items"), `key IN (?`+strings.Repeat(", ?", len(keys)-1)+`) ORDER BY created_ts, key`, keys...)
	if err != nil {
		return nil, err
	}
	return recordsOf(ctx, s.read, items)
}

// reviewCandidateKeys reads the key of each item that may wait in review.
func (s *Store) reviewCandidateKeys(ctx context.Context) ([]any, error) {
	return s.selectKeys(ctx, reviewCandidates, "select the items waiting in review")
}

// selectKeys reads the item key in each row that query answers, and wraps an
// error in what.
func (s *Store) selectKeys(ctx context.Context, query, what string) ([]any, error) {
	rows, err := s.read.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer func() { _ = rows.Close() }()
	var keys []any
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}
