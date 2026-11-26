package connectors

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/datapointchris/ingestion/store"
)

// scheduleParser reads a standard five-field cron expression, a descriptor
// such as @hourly, or @every 15m. It reads the fields in UTC, or in the zone
// that a CRON_TZ=<zone> prefix names.
var scheduleParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func parseSchedule(expr string) (cron.Schedule, error) {
	if !strings.HasPrefix(expr, "CRON_TZ=") && !strings.HasPrefix(expr, "TZ=") {
		expr = "CRON_TZ=UTC " + expr
	}
	return scheduleParser.Parse(expr)
}

// ParseInterval reads an interval: a positive count of m, h, d or w, such as
// 90m, 12h, 30d or 2w. A day is 24 hours and a week 7 days. The schema's
// interval pattern accepts the same strings.
func ParseInterval(s string) (time.Duration, error) {
	units := map[string]time.Duration{"m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}
	unit, ok := units[s[max(len(s)-1, 0):]]
	n, err := strconv.Atoi(s[:max(len(s)-1, 0)])
	if !ok || err != nil || n <= 0 || strings.HasPrefix(s, "0") || strings.HasPrefix(s, "+") {
		return 0, fmt.Errorf("an interval is a positive count of m, h, d or w, such as 90m, 12h or 30d, not %q", s)
	}
	return time.Duration(n) * unit, nil
}

// onceRecheck is how long after a once connector's poll was made it is due
// again, where no report has moved it. So a poll whose lease lapsed until its
// claims failed it is made again.
const onceRecheck = time.Hour

// NextDue is when c is next due, from its cursor. A poll or a folder is due at
// its schedule's next time after its last run. A once connector is due at
// once until its first run, then onceRecheck after its last, and keeps the
// time it finished once a poll ended done. The scheduler never runs a
// finished one. A drop or a push is due its interval after its last delivery.
// One that never ran or delivered counts from when its cursor was made.
func NextDue(c Connector, cur store.Cursor) (time.Time, error) {
	created, err := time.Parse(store.StampLayout, cur.CreatedTS)
	if err != nil {
		return time.Time{}, fmt.Errorf("connector %s's cursor was made at %q: %w", c.Name, cur.CreatedTS, err)
	}
	since := func(stamp *string) time.Time {
		if stamp == nil {
			return created
		}
		at, err := time.Parse(store.StampLayout, *stamp)
		if err != nil {
			return created
		}
		return at
	}
	switch {
	case c.Once() && cur.FinishedTS != nil:
		return since(cur.FinishedTS), nil
	case c.Once() && cur.LastRunTS == nil:
		return created, nil
	case c.Once():
		return since(cur.LastRunTS).Add(onceRecheck), nil
	}
	if c.Scheduled() {
		schedule, err := parseSchedule(c.Schedule)
		if err != nil {
			return time.Time{}, err
		}
		return schedule.Next(since(cur.LastRunTS)).UTC(), nil
	}
	interval, err := ParseInterval(c.DueInterval)
	if err != nil {
		return time.Time{}, err
	}
	return since(cur.LastDeliveryTS).Add(interval).UTC(), nil
}

// StaleAt is when c goes stale from its cursor: its last delivery plus its
// freshness. It is nil where c declares no freshness, has delivered nothing,
// or finished.
func StaleAt(c Connector, cur store.Cursor) *string {
	if c.Freshness == "" || cur.LastDeliveryTS == nil || cur.FinishedTS != nil {
		return nil
	}
	freshness, err := ParseInterval(c.Freshness)
	if err != nil {
		return nil
	}
	delivered, err := time.Parse(store.StampLayout, *cur.LastDeliveryTS)
	if err != nil {
		return nil
	}
	at := store.Stamp(delivered.Add(freshness))
	return &at
}

// OverdueSince is when the connector whose cursor is cur went overdue, and
// false where it is not overdue at now. It goes overdue once its due time
// passes with no delivery since, or once its stale time passes, whichever
// comes first. A connector that finished is never overdue.
func OverdueSince(cur store.Cursor, now time.Time) (string, bool) {
	if cur.FinishedTS != nil {
		return "", false
	}
	var delivered time.Time
	if cur.LastDeliveryTS != nil {
		delivered, _ = time.Parse(store.StampLayout, *cur.LastDeliveryTS)
	}
	var since []string
	if due, err := time.Parse(store.StampLayout, cur.DueAt); err == nil && now.After(due) && delivered.Before(due) {
		since = append(since, cur.DueAt)
	}
	if cur.StaleAt != nil {
		if stale, err := time.Parse(store.StampLayout, *cur.StaleAt); err == nil && now.After(stale) {
			since = append(since, *cur.StaleAt)
		}
	}
	if len(since) == 0 {
		return "", false
	}
	return slices.Min(since), true
}

// putCursor writes cur with the stale time that c's freshness gives it. Each
// cursor write in this package runs through it, so OverdueSince reads a
// current one.
func putCursor(ctx context.Context, tx *store.Tx, c Connector, cur store.Cursor) error {
	cur.StaleAt = StaleAt(c, cur)
	return tx.PutCursor(ctx, cur)
}

// keepStaleAt writes c's cursor again where its stale time differs from the
// one that c's freshness gives it. A cursor that an earlier release wrote
// holds none, or one from before its last delivery.
func keepStaleAt(ctx context.Context, tx *store.Tx, c Connector) error {
	cur, ok, err := tx.Cursor(ctx, c.Name)
	if err != nil || !ok || sameStamp(cur.StaleAt, StaleAt(c, cur)) {
		return err
	}
	return putCursor(ctx, tx, c, cur)
}

func sameStamp(a, b *string) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}
