package metricsrefresh

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/store"
)

// DefaultSchedule is the schedule where config names none: once a day.
const DefaultSchedule = "0 5 * * *"

// Queue queues source's refresh, in one write, where a connector of source in
// the set in force sighted an item that waits in review, and no refresh of
// source is queued or leased. It answers the task, or why it queued none.
func Queue(ctx context.Context, st *store.Store, source string) (store.Task, string, error) {
	var queued store.Task
	why := ""
	err := st.Update(ctx, func(tx *store.Tx) error {
		if task, ok, err := tx.RefreshMetricsOutstanding(ctx, source); err != nil {
			return err
		} else if ok {
			why = "refresh " + task.ID + " is still " + task.State
			return nil
		}
		set, err := connectors.AppliedSet(ctx, tx)
		if err != nil {
			return err
		}
		keys, err := connectors.InReview(ctx, tx, set, source)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			why = "no item that a connector of the source sighted waits in review"
			return nil
		}
		queued, err = tx.Enqueue(ctx, store.RefreshMetricsTask(source))
		return err
	})
	if err != nil || why != "" {
		return store.Task{}, why, err
	}
	return queued, "", nil
}

// Scheduler queues each source's refresh at each time that its schedule
// names.
type Scheduler struct {
	store    *store.Store
	schedule cron.Schedule
	log      *slog.Logger
	// started anchors a source's first time where the store holds no refresh
	// of it, and checked is the latest time that a pass queued each source's
	// refresh for.
	started time.Time
	checked map[string]time.Time
}

// NewScheduler is the scheduler over st that queues at each time of expr, a
// cron expression as a connector's schedule reads it.
func NewScheduler(st *store.Store, expr string, log *slog.Logger) (*Scheduler, error) {
	schedule, err := connectors.ParseSchedule(expr)
	if err != nil {
		return nil, fmt.Errorf("schedule %q is not a cron expression: %w", expr, err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: st, schedule: schedule, log: log, started: st.Now(), checked: map[string]time.Time{}}, nil
}

// Run passes until ctx ends, sleeping until the schedule's next time.
func (s *Scheduler) Run(ctx context.Context) {
	connectors.RunPasses(ctx, s.store.Now, s.log, "a metrics refresh's pass failed", s.Pass)
}

// Pass queues, once, the refresh of each source that this build refreshes
// and that the set in force declares a connector of, for the latest time of
// the schedule that has come since the source's newest refresh. It answers
// when to pass again. A time that came while the service was down is queued
// for on the first pass after. Before a source's first refresh, the first
// time after serve started is the first that queues one.
func (s *Scheduler) Pass(ctx context.Context) (time.Time, error) {
	now := s.store.Now()
	next := s.schedule.Next(now)
	set, err := connectors.AppliedSet(ctx, s.store)
	if err != nil {
		return now.Add(connectors.PassRetry), err
	}
	var refreshed []string
	for _, c := range set.Connectors {
		if connectors.RefreshesMetrics(c.Source) && !slices.Contains(refreshed, c.Source) {
			refreshed = append(refreshed, c.Source)
		}
	}
	slices.Sort(refreshed)
	for _, source := range refreshed {
		anchor := s.started
		newest, held, err := s.store.NewestRefreshMetrics(ctx, source)
		if err != nil {
			return now.Add(connectors.PassRetry), err
		}
		if held {
			if anchor, err = time.Parse(store.StampLayout, newest.CreatedTS); err != nil {
				return now.Add(connectors.PassRetry), fmt.Errorf("read refresh %s's created_ts: %w", newest.ID, err)
			}
		}
		due := connectors.LatestDue(s.schedule, anchor, now)
		if due.IsZero() || !due.After(s.checked[source]) {
			continue
		}
		task, why, err := Queue(ctx, s.store, source)
		if err != nil {
			return now.Add(connectors.PassRetry), err
		}
		s.checked[source] = due
		if why != "" {
			s.log.InfoContext(ctx, "queued no metrics refresh", "source", source, "due", store.Stamp(due), "why", why)
			continue
		}
		s.log.InfoContext(ctx, "queued a metrics refresh", "source", source, "due", store.Stamp(due), "task", task.ID)
	}
	return next, nil
}
