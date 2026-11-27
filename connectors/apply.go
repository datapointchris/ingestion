package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

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

// MovedError is an apply whose replaces is not the commit in force, so the
// set it was built against has moved.
type MovedError struct {
	// Current is the commit in force, and nil where no set is applied.
	Current *string
	// Replaces is the commit the apply named, and nil where it named none.
	Replaces *string
}

func (e *MovedError) Error() string {
	return fmt.Sprintf("the apply replaces %s, and %s is in force", commitOrNone(e.Replaces), commitOrNone(e.Current))
}

func commitOrNone(c *string) string {
	if c == nil {
		return "no set"
	}
	return *c
}

// Apply validates set whole, then keeps it under commit as the set in force,
// and moves each connector's cursor to its due time under it. replaces is the
// commit the caller saw in force, nil for none. An apply of the set in force
// answers Unchanged whatever it replaces, so a retry lands nothing twice. Any
// other apply whose replaces is not the commit in force is a MovedError. An
// invalid set is an InvalidError, and so is a set that an accepted routing
// rule could not hold under, as acceptedRulesRefused reads it in the write.
// Either writes nothing. A connector the set
// drops keeps its cursor, and the scheduler does not run it. A connector that
// the set declares other than once loses its cursor's finished time, so a
// backfill declared again on a schedule reads stale and overdue as any poll
// does. roots names the
// [serve.homes] roots a folder may read. after, where it is not nil, runs in
// the same write once the set is kept, and its error writes nothing.
func Apply(ctx context.Context, st *store.Store, set Set, commit string, replaces *string, by string, roots []string,
	after AfterApply,
) (Applied, error) {
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
		if ok != (replaces != nil) || (ok && *replaces != current.Commit) {
			e := &MovedError{Replaces: replaces}
			if ok {
				e.Current = &current.Commit
			}
			return e
		}
		if invalid, err := set.acceptedRulesRefused(ctx, tx); err != nil || len(invalid) > 0 {
			if err != nil {
				return err
			}
			return &InvalidError{Invalid: invalid}
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
			unfinished := cur.FinishedTS != nil && !c.Once()
			if unfinished {
				cur.FinishedTS = nil
			}
			due, err := NextDue(c, cur)
			if err != nil {
				return err
			}
			if held && !unfinished && cur.DueAt == store.Stamp(due) && sameStamp(cur.StaleAt, StaleAt(c, cur)) {
				continue
			}
			cur.DueAt = store.Stamp(due)
			if err := putCursor(ctx, tx, c, cur); err != nil {
				return err
			}
		}
		if after != nil {
			return after(ctx, tx)
		}
		return nil
	})
	return applied, err
}

// AfterApply runs in the write that keeps a changed set, which tx then reads
// as the set in force.
type AfterApply func(ctx context.Context, tx *store.Tx) error

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
	return putCursor(ctx, tx, c, cur)
}
