package connectors

import (
	"fmt"
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

// ParseInterval reads an interval such as 90m, 12h, 30d or 2w. A day is 24
// hours and a week 7 days.
func ParseInterval(s string) (time.Duration, error) {
	var d time.Duration
	var err error
	switch unit := s[max(len(s)-1, 0):]; unit {
	case "d", "w":
		var n int
		n, err = strconv.Atoi(s[:len(s)-1])
		d = time.Duration(n) * 24 * time.Hour
		if unit == "w" {
			d *= 7
		}
	default:
		d, err = time.ParseDuration(s)
	}
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("an interval is a positive count of m, h, d or w, such as 90m, 12h or 30d, not %q", s)
	}
	return d, nil
}

// NextDue is when c is next due, from its cursor. A poll or a folder is due at
// its schedule's next time after its last run. A drop or a push is due its
// interval after its last delivery. One that never ran or delivered counts
// from when its cursor was made.
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
	switch c.Mode {
	case ModePoll, ModeFolder:
		schedule, err := parseSchedule(c.Schedule)
		if err != nil {
			return time.Time{}, err
		}
		return schedule.Next(since(cur.LastRunTS)).UTC(), nil
	default:
		interval, err := ParseInterval(c.DueInterval)
		if err != nil {
			return time.Time{}, err
		}
		return since(cur.LastDeliveryTS).Add(interval).UTC(), nil
	}
}

// Overdue says whether c is overdue at now. Any connector is, once its due
// time passed with no delivery since. A poll or a folder declaring a
// freshness also is once its last delivery is older than that.
func Overdue(c Connector, cur store.Cursor, now time.Time) bool {
	var delivered time.Time
	if cur.LastDeliveryTS != nil {
		delivered, _ = time.Parse(store.StampLayout, *cur.LastDeliveryTS)
	}
	due, err := time.Parse(store.StampLayout, cur.DueAt)
	if err == nil && now.After(due) && delivered.Before(due) {
		return true
	}
	if c.Freshness == "" || cur.LastDeliveryTS == nil {
		return false
	}
	freshness, err := ParseInterval(c.Freshness)
	return err == nil && now.Sub(delivered) > freshness
}
