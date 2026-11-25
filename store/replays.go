package store

import (
	"context"
	"errors"
	"fmt"
)

// ErrReplayDiffers is a replay whose id names one already recorded with
// another stage, connector or key.
var ErrReplayDiffers = errors.New("the replay's id names a replay of another stage or other items")

// ItemSelection selects items by a connector that sighted them and by key.
// An empty field selects every item.
type ItemSelection struct {
	Connector string
	Key       string
}

// SelectItems reads the items sel selects, oldest first.
func (s *Store) SelectItems(ctx context.Context, sel ItemSelection) ([]Item, error) {
	return selectItems(ctx, s.read, sel)
}

func selectItems(ctx context.Context, db querier, sel ItemSelection) ([]Item, error) {
	where, args := `1`, []any{}
	if sel.Connector != "" {
		where, args = where+` AND key IN (SELECT item_key FROM sightings WHERE connector = ?)`, append(args, sel.Connector)
	}
	if sel.Key != "" {
		where, args = where+` AND key = ?`, append(args, sel.Key)
	}
	return readInto[Item](ctx, db, table("items"), where+` ORDER BY created_ts, key`, args...)
}

// Replay queues a task of r.Stage for each item that r's connector and key
// select and capability takes, then records r with its count, in one write.
// capability answers the task's capability for an item, and false where the
// stage does not run on it. A replay already recorded under r.ID answers as
// recorded and queues nothing, and one recorded with another stage, connector
// or key answers ErrReplayDiffers. The bool says whether this call queued.
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
		items, err := selectItems(ctx, tx.tx, ItemSelection{Connector: deref(r.Connector), Key: deref(r.ItemKey)})
		if err != nil {
			return err
		}
		r.Items = 0
		for _, item := range items {
			c, ok := capability(item)
			if !ok {
				continue
			}
			if _, err := tx.Enqueue(ctx, Task{ItemKey: item.Key, Stage: r.Stage, Capability: c}); err != nil {
				return err
			}
			r.Items++
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
