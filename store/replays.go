package store

import (
	"context"
	"errors"
	"fmt"
)

// ErrReplayDiffers is a replay whose id names one already recorded with
// another stage, connector or key.
var ErrReplayDiffers = errors.New("the replay's id names a replay of another stage or other items")

// ErrNoSighting is a replay naming a connector that sighted no item.
var ErrNoSighting = errors.New("the connector sighted no item")

// ReplayItems reads the items that r covers, oldest first: each item that r's
// connector sighted and r's key names, or every item where neither is set,
// that capability takes. capability answers the task's capability for an
// item, and false where the stage does not run on it. It answers ErrNoItem
// where r's key names no item, and ErrNoSighting where r's connector sighted
// none.
func (s *Store) ReplayItems(ctx context.Context, r Replay, capability func(Item) (string, bool)) ([]Item, error) {
	return replayItems(ctx, s.read, r, capability)
}

func replayItems(ctx context.Context, db querier, r Replay, capability func(Item) (string, bool)) ([]Item, error) {
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
	var items []Item
	for _, item := range selected {
		if _, ok := capability(item); ok {
			items = append(items, item)
		}
	}
	return items, nil
}

// Replay queues a task of r.Stage for each item that ReplayItems answers for
// r, then records r with its count, in one write. A replay already recorded
// under r.ID answers as recorded and queues nothing, and one recorded with
// another stage, connector or key answers ErrReplayDiffers. The bool says
// whether this call queued.
func (s *Store) Replay(ctx context.Context, r Replay, capability func(Item) (string, bool)) (Replay, bool, error) {
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
		items, err := replayItems(ctx, tx.tx, r, capability)
		if err != nil {
			return err
		}
		for _, item := range items {
			c, _ := capability(item)
			if _, err := tx.Enqueue(ctx, Task{ItemKey: &item.Key, Stage: r.Stage, Capability: c}); err != nil {
				return err
			}
		}
		r.Items, r.RequestedTS = len(items), tx.stamp()
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
