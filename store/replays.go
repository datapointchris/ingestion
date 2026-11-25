package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// ErrReplayDiffers is a replay whose id names one already recorded with
// another stage, connector or key.
var ErrReplayDiffers = errors.New("the replay's id names a replay of another stage or other items")

// ErrNoSighting is a replay naming a connector that sighted no item.
var ErrNoSighting = errors.New("the connector sighted no item")

// ReplayItems reads the items that r covers, oldest first: each item that r's
// connector sighted and r's key names, or every item where neither is set,
// that takes says the stage runs on. It answers ErrNoItem where r's key names
// no item, and ErrNoSighting where r's connector sighted none.
func (s *Store) ReplayItems(ctx context.Context, r Replay, takes func(Item) bool) ([]Item, error) {
	return replayItems(ctx, s.read, r, takes)
}

// ReplayQueue queues a stage's tasks for item, and answers whether it queued
// any.
type ReplayQueue func(ctx context.Context, tx *Tx, item Item) (bool, error)

func replayItems(ctx context.Context, db querier, r Replay, takes func(Item) bool) ([]Item, error) {
	where, args := `1`, []any{}
	if connector := deref(r.Connector); connector != "" {
		_, found, err := first[Sighting](ctx, db, table("sightings"), `connector = ?`, connector)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%s: %w", connector, ErrNoSighting)
		}
		where, args = where+` AND key IN (SELECT item_key FROM sightings WHERE connector = ?)`, append(args, connector)
	}
	if key := deref(r.ItemKey); key != "" {
		_, found, err := first[Item](ctx, db, table("items"), `key = ?`, key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%s: %w", key, ErrNoItem)
		}
		where, args = where+` AND key = ?`, append(args, key)
	}
	selected, err := readInto[Item](ctx, db, table("items"), where+` ORDER BY created_ts, key`, args...)
	if err != nil {
		return nil, err
	}
	if takes == nil {
		return selected, nil
	}
	var items []Item
	for _, item := range selected {
		if takes(item) {
			items = append(items, item)
		}
	}
	return items, nil
}

// Replay runs queue over each item that r covers, then records r with the
// count of items queue queued for, in one write. A replay already recorded
// under r.ID answers as recorded and queues nothing, and one recorded with
// another stage, connector or key answers ErrReplayDiffers. The bool says
// whether this call queued.
func (s *Store) Replay(ctx context.Context, r Replay, queue ReplayQueue) (Replay, bool, error) {
	queued := false
	err := s.Update(ctx, func(tx *Tx) error {
		held, found, err := first[Replay](ctx, tx.tx, table("replays"), `id = ?`, r.ID)
		if err != nil {
			return err
		}
		if found {
			if held.Stage != r.Stage || deref(held.Connector) != deref(r.Connector) || deref(held.ItemKey) != deref(r.ItemKey) {
				return fmt.Errorf("replay %s: %w", r.ID, ErrReplayDiffers)
			}
			r = held
			return nil
		}
		items, err := replayItems(ctx, tx.tx, r, nil)
		if err != nil {
			return err
		}
		r.Items = 0
		for _, item := range items {
			ok, err := queue(ctx, tx, item)
			if err != nil {
				return err
			}
			if ok {
				r.Items++
			}
		}
		r.RequestedTS = tx.stamp()
		queued = true
		return tx.put(ctx, table("replays"), r)
	})
	return r, queued, err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// StageTokens reads the tokens by kind of up to limit of stage's outputs that
// hold any, the newest first, which a model stage's replay estimates from.
func (s *Store) StageTokens(ctx context.Context, stage string, limit int) ([]Tokens, error) {
	outputs, err := readInto[Output](ctx, s.read, table("outputs"), `stage = ? AND tokens IS NOT NULL ORDER BY updated_ts DESC LIMIT ?`,
		stage, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Tokens, 0, len(outputs))
	for _, o := range outputs {
		if o.Tokens != nil {
			out = append(out, *o.Tokens)
		}
	}
	return out, nil
}

// MedianTokens is each kind's median over runs, each kind taken apart.
func MedianTokens(runs []Tokens) Tokens {
	kind := func(of func(Tokens) int64) int64 {
		values := make([]int64, len(runs))
		for i, t := range runs {
			values[i] = of(t)
		}
		slices.Sort(values)
		if len(values) == 0 {
			return 0
		}
		return values[len(values)/2]
	}
	return Tokens{
		UncachedInput: kind(func(t Tokens) int64 { return t.UncachedInput }),
		CacheWrite:    kind(func(t Tokens) int64 { return t.CacheWrite }),
		CacheRead:     kind(func(t Tokens) int64 { return t.CacheRead }),
		Output:        kind(func(t Tokens) int64 { return t.Output }),
	}
}
