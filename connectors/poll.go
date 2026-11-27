package connectors

import (
	"context"
	"errors"
	"fmt"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// ErrNotDeclared is a poll of a connector that is absent from the set in
// force, so no declaration names the zone and tags of what it found.
var ErrNotDeclared = errors.New("the set in force does not declare the connector")

// PollArrival is one arrival that a poll's report carried. Refused is why the
// report's reader could not read it as an arrival, and nil where it could.
type PollArrival struct {
	Arrival landing.Arrival
	Refused error
}

// PollLanded counts what one poll's report wrote: the arrivals it carried,
// the items and sightings they added, the deletes they queued, and the
// arrivals refused. Failure is why the poll failed where any was refused, and
// "" where none was.
type PollLanded struct {
	Arrivals  int
	Items     int
	Sightings int
	Deletes   int
	Refused   int
	Failure   string
}

// LandPoll lands each of a poll's arrivals in tx, the write that ends the
// poll's lease done, under connector as the set in force declares it. The
// declaration gives each arrival its connector, zone and tags, whatever the
// worker sent. Where the connector deletes after landing, it queues a delete
// of each arrival's item, unless one is queued or leased, or failed and waits
// for a person to retry it.
//
// Where every arrival landed, it moves the connector's cursor to cursor, where
// the poll gave one, clears the last failure, and finishes a connector
// scheduled once. An arrival that landing refuses lands nothing, and the rest
// land. The poll then fails with the first refusal as its reason. The cursor
// stays where it was, so the refused item is read again rather than passed
// over, and a connector scheduled once stays unfinished. Either way, a
// delivery is recorded where an arrival added a sighting.
func LandPoll(ctx context.Context, tx *store.Tx, l *landing.Lander, connector string, arrivals []PollArrival, cursor *string) (PollLanded, error) {
	c, cur, err := polled(ctx, tx, connector)
	if err != nil {
		return PollLanded{}, err
	}
	out := PollLanded{Arrivals: len(arrivals)}
	var firstRefusal error
	refuse := func(i int, err error) {
		out.Refused++
		if firstRefusal == nil {
			firstRefusal = fmt.Errorf("arrival %d: %w", i, err)
		}
	}
	for i, pa := range arrivals {
		if pa.Refused != nil {
			refuse(i, pa.Refused)
			continue
		}
		a := pa.Arrival
		a.Connector, a.Zone, a.Tags = c.Name, c.Zone, c.Tags
		landed, err := l.LandIn(ctx, tx, a)
		if errors.Is(err, landing.ErrInvalidArrival) {
			refuse(i, err)
			continue
		}
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
		outstanding, err := tx.DeleteOutstanding(ctx, landed.Key, c.Name)
		if err != nil {
			return out, err
		}
		if outstanding {
			continue
		}
		if _, err := tx.Enqueue(ctx, store.DeleteTask(landed.Key, c.Name, c.Source)); err != nil {
			return out, err
		}
		out.Deletes++
	}
	now := store.Stamp(tx.Now())
	if out.Sightings > 0 {
		cur.LastDeliveryTS = &now
	}
	if firstRefusal != nil {
		out.Failure = fmt.Sprintf("%d of %d arrivals could not land, so the cursor stays where it was: %v",
			out.Refused, out.Arrivals, firstRefusal)
		due, err := failedRun(c, &cur, errors.New(out.Failure), tx.Now())
		if err != nil {
			return out, err
		}
		cur.DueAt = store.Stamp(due)
		return out, putCursor(ctx, tx, c, cur)
	}
	if cursor != nil {
		position := *cursor
		cur.Position = &position
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

// Unwanted answers why the set s wants nobody to run task, and "" where it
// still wants it. A poll is unwanted once s drops its connector, or declares
// it other than as a poll of the source that the task reads. A delete is
// unwanted where s does not declare that its connector deletes from that
// source after landing. A refresh of a source's metrics is unwanted where s
// declares no connector of the source, or this build reads none of its
// metrics. Any other task is always wanted.
func Unwanted(s Set, task store.Task) string {
	switch {
	case task.Stage == store.StagePoll && task.Connector != nil:
		c, ok := s.Connector(*task.Connector)
		if !ok || c.Mode != ModePoll || store.PollCapability(c.Source) != task.Capability {
			return fmt.Sprintf("the set in force does not poll connector %s from this source, so nobody runs this poll", *task.Connector)
		}
	case task.Stage == store.StageDelete && task.SightingConnector != nil:
		c, ok := s.Connector(*task.SightingConnector)
		if !ok || c.AfterLanding != AfterLandingDelete || store.DeleteCapability(c.Source) != task.Capability {
			return fmt.Sprintf("the set in force does not delete what connector %s lands from this source, so nobody runs this delete",
				*task.SightingConnector)
		}
	case task.Stage == store.StageRefreshMetrics && task.Source != nil:
		if len(s.SourceConnectors(*task.Source)) == 0 || !RefreshesMetrics(*task.Source) {
			return fmt.Sprintf("the set in force declares no connector of source %s whose metrics this build reads, so nobody runs this refresh",
				*task.Source)
		}
	}
	return ""
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
