package connectors

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/datapointchris/ingestion/store"
)

// Applied is what one apply left in force.
type Applied struct {
	Commit    string
	AppliedTS string
	AppliedBy string
	// Unchanged is true where this commit's set was already in force, so the
	// apply wrote nothing.
	Unchanged bool
}

// Apply validates set whole, then keeps it under commit as the set in force,
// and moves each connector's cursor to its due time under it. An invalid set
// is an InvalidError, and writes nothing. A connector the set drops keeps its
// cursor, and the scheduler no longer runs it. roots names the [serve.homes]
// roots a folder may read.
func Apply(ctx context.Context, st *store.Store, set Set, commit, by string, roots []string) (Applied, error) {
	if invalid := Validate(set, roots); len(invalid) > 0 {
		return Applied{}, &InvalidError{Invalid: invalid}
	}
	connectors, rules, destinations, err := set.encode()
	if err != nil {
		return Applied{}, err
	}
	applied := Applied{Commit: commit, AppliedBy: by}
	err = st.Update(ctx, func(tx *store.Tx) error {
		current, ok, err := tx.AppliedDeclarations(ctx)
		if err != nil {
			return err
		}
		if ok && current.Commit == commit && sameJSON(current.Connectors, connectors) &&
			sameJSON(current.RoutingRules, rules) && sameJSON(current.Destinations, destinations) {
			applied.AppliedTS, applied.AppliedBy, applied.Unchanged = current.AppliedTS, current.AppliedBy, true
			return nil
		}
		applied.AppliedTS = store.Stamp(tx.Now())
		if err := tx.PutDeclarations(ctx, store.Declarations{
			Commit: commit, AppliedBy: by, Connectors: connectors, RoutingRules: rules, Destinations: destinations,
		}); err != nil {
			return err
		}
		for _, c := range set.Connectors {
			cur, held, err := tx.Cursor(ctx, c.Name)
			if err != nil {
				return err
			}
			if !held {
				cur = store.Cursor{Connector: c.Name, FirstFill: store.FirstFillNone, CreatedTS: store.Stamp(tx.Now())}
			}
			due, err := NextDue(c, cur)
			if err != nil {
				return err
			}
			if held && cur.DueAt == store.Stamp(due) {
				continue
			}
			cur.DueAt = store.Stamp(due)
			if err := tx.PutCursor(ctx, cur); err != nil {
				return err
			}
		}
		return nil
	})
	return applied, err
}

func sameJSON(a, b []json.RawMessage) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(ja, jb)
}

// Delivered records that c delivered at the write's time, and moves its due
// time on from it.
func Delivered(ctx context.Context, tx *store.Tx, c Connector) error {
	cur, ok, err := tx.Cursor(ctx, c.Name)
	if err != nil || !ok {
		return err
	}
	now := store.Stamp(tx.Now())
	cur.LastDeliveryTS = &now
	due, err := NextDue(c, cur)
	if err != nil {
		return err
	}
	cur.DueAt = store.Stamp(due)
	return tx.PutCursor(ctx, cur)
}
