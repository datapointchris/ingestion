package connectors

import (
	"context"
	"errors"
	"fmt"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// deleters holds each source that this build registers a deleter for.
var deleters = map[string]bool{}

// RegisterDeleter records that this build carries a deleter for source, which
// removes an item from that source once it has landed. A poll connector of
// the source may then declare after_landing delete. deletion.Register calls
// it, so a set names only a deleter that a worker can run. Registering a
// source twice panics, as a duplicate is a build error.
func RegisterDeleter(source string) {
	if deleters[source] {
		panic("connectors: the deleter of source " + source + " is registered twice")
	}
	deleters[source] = true
}

// ErrNotDeclared is a poll of a connector that is absent from the set in
// force, so no declaration names the zone and tags of what it found.
var ErrNotDeclared = errors.New("the set in force does not declare the connector")

// PollLanded counts what one poll's report wrote: the arrivals it carried,
// the items and sightings they added, and the deletes they queued.
type PollLanded struct {
	Arrivals  int
	Items     int
	Sightings int
	Deletes   int
}

// LandPoll lands each of a poll's arrivals in tx, the write that ends the
// poll's lease done, under connector as the set in force declares it. The
// declaration gives each arrival its connector, zone and tags, whatever the
// worker sent. Where the connector deletes after landing, it queues a delete
// of each arrival's item, unless one is already in flight. It then moves the
// connector's cursor to cursor, where the poll gave one, records a delivery
// where an arrival added a sighting, clears the last failure, and finishes a
// connector scheduled once.
func LandPoll(ctx context.Context, tx *store.Tx, l *landing.Lander, connector string, arrivals []landing.Arrival, cursor *string) (PollLanded, error) {
	c, cur, err := polled(ctx, tx, connector)
	if err != nil {
		return PollLanded{}, err
	}
	out := PollLanded{Arrivals: len(arrivals)}
	for i, a := range arrivals {
		a.Connector, a.Zone, a.Tags = c.Name, c.Zone, c.Tags
		landed, err := l.LandIn(ctx, tx, a)
		if err != nil {
			return out, fmt.Errorf("arrival %d: %w", i, err)
		}
		if landed.ItemAdded {
			out.Items++
		}
		if landed.SightingAdded {
			out.Sightings++
		}
		if c.AfterLanding != AfterLandingDelete {
			continue
		}
		inFlight, err := tx.DeleteInFlight(ctx, landed.Key, c.Name)
		if err != nil {
			return out, err
		}
		if inFlight {
			continue
		}
		if _, err := tx.Enqueue(ctx, store.DeleteTask(landed.Key, c.Name, c.Source)); err != nil {
			return out, err
		}
		out.Deletes++
	}
	now := store.Stamp(tx.Now())
	if cursor != nil {
		position := *cursor
		cur.Position = &position
	}
	if out.Sightings > 0 {
		cur.LastDeliveryTS = &now
	}
	cur.LastError, cur.LastErrorTS, cur.Failures = nil, nil, 0
	if c.Once() {
		cur.FinishedTS = &now
	}
	due, err := NextDue(c, cur)
	if err != nil {
		return out, err
	}
	cur.DueAt = store.Stamp(due)
	return out, putCursor(ctx, tx, c, cur)
}

// PollFailed records on connector's cursor, in tx, that its poll ended
// failed for reason, and makes it due again as any failed run is.
func PollFailed(ctx context.Context, tx *store.Tx, connector, reason string) error {
	c, cur, err := polled(ctx, tx, connector)
	if errors.Is(err, ErrNotDeclared) {
		return nil
	}
	if err != nil {
		return err
	}
	due, err := failedRun(c, &cur, errors.New(reason), tx.Now())
	if err != nil {
		return err
	}
	cur.DueAt = store.Stamp(due)
	return putCursor(ctx, tx, c, cur)
}

// polled is connector's declaration in the set in force and its cursor.
func polled(ctx context.Context, tx *store.Tx, connector string) (Connector, store.Cursor, error) {
	set, err := AppliedSet(ctx, tx)
	if err != nil {
		return Connector{}, store.Cursor{}, err
	}
	c, ok := set.Connector(connector)
	if !ok {
		return Connector{}, store.Cursor{}, fmt.Errorf("connector %s: %w", connector, ErrNotDeclared)
	}
	cur, ok, err := tx.Cursor(ctx, connector)
	if err != nil {
		return c, cur, err
	}
	if !ok {
		return c, cur, fmt.Errorf("connector %s: %w", connector, store.ErrNoCursor)
	}
	return c, cur, nil
}
