package connectors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// maxWait bounds how long the scheduler sleeps between passes, so a due time
// that a write moved earlier without waking it is met within a minute.
const maxWait = time.Minute

// scanBudget bounds how long one pass spends landing a folder's files. A
// large first scan stops there, so the pass goes on to every other connector,
// and the next pass resumes it.
const scanBudget = 30 * time.Second

// settleWindow is how long a file goes unmodified before a scan lands it, so
// a copy still in progress is not landed at a partial size.
const settleWindow = 10 * time.Second

// retryAfter is how long the scheduler waits before running a failed
// connector again. The wait doubles after each failure in a row, and never
// runs past the connector's next scheduled time.
const retryAfter = 5 * time.Minute

// maxErrorText bounds the error a cursor keeps, since a scan's error names
// every file that did not land.
const maxErrorText = 4096

// Scheduler owns when each connector is due. It makes a poll connector's task
// when its due time comes, and scans a folder connector's folder itself,
// since only the service holds the named roots. Each run moves the
// connector's due_at to its schedule's next time. A drop or a push runs when
// its items arrive, so only a delivery moves its due time.
type Scheduler struct {
	store  *store.Store
	lander *landing.Lander
	log    *slog.Logger
	wake   chan struct{}
}

// NewScheduler is a scheduler over st, landing each folder's files through l.
func NewScheduler(st *store.Store, l *landing.Lander, log *slog.Logger) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: st, lander: l, log: log, wake: make(chan struct{}, 1)}
}

// Wake has the scheduler pass over the connectors now, rather than at the
// next due time.
func (s *Scheduler) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run passes over the connectors until ctx ends, sleeping until the next due
// time or a Wake.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		next, err := s.Pass(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.ErrorContext(ctx, "a scheduler pass failed", "error", err)
		}
		wait := min(max(time.Until(next), time.Second), maxWait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Pass runs each poll and folder connector in the set in force whose due time
// has come, and answers the earliest due time left. One connector's failed run
// is recorded on its cursor and logged, and leaves the rest to run.
func (s *Scheduler) Pass(ctx context.Context) (time.Time, error) {
	now := s.store.Now()
	next := now.Add(maxWait)
	declared, ok, err := s.store.AppliedDeclarations(ctx)
	if err != nil || !ok {
		return next, err
	}
	set, err := Decode(declared)
	if err != nil {
		return next, err
	}
	var failed []error
	for _, c := range set.Connectors {
		if !c.Scheduled() {
			continue
		}
		cur, ok, err := s.store.Cursor(ctx, c.Name)
		if err != nil {
			return next, err
		}
		if !ok {
			continue
		}
		due, err := time.Parse(store.StampLayout, cur.DueAt)
		if err != nil {
			failed = append(failed, fmt.Errorf("connector %s is due at %q: %w", c.Name, cur.DueAt, err))
			continue
		}
		if due.After(now) {
			next = earliest(next, due)
			continue
		}
		if due, err = s.run(ctx, c); err != nil {
			failed = append(failed, fmt.Errorf("run connector %s: %w", c.Name, err))
		}
		if !due.IsZero() {
			next = earliest(next, due)
		}
	}
	return next, errors.Join(failed...)
}

// run runs c once and answers when it is next due. A run that completes moves
// last_run_ts and makes c due at its schedule's next time. A scan stopped at
// its budget makes c due at once, to resume. A run that fails leaves
// last_run_ts, records its error on the cursor, and makes c due again after
// retryAfter, doubled for each failure in a row.
func (s *Scheduler) run(ctx context.Context, c Connector) (time.Time, error) {
	started := s.store.Now()
	var scanned Scanned
	var runErr error
	if c.Mode == ModeFolder {
		real := time.Now()
		scanned, runErr = ScanFolder(ctx, s.lander, c, real.Add(scanBudget), real.Add(-settleWindow))
		if runErr != nil {
			s.log.ErrorContext(ctx, "a folder scan failed", "connector", c.Name, "root", c.Root, "error", runErr)
		}
		s.log.InfoContext(ctx, "scanned a folder", "connector", c.Name, "root", c.Root, "files", scanned.Files,
			"landed", scanned.Landed, "skipped", scanned.Skipped, "failed", scanned.Failed, "settling", scanned.Settling,
			"unfinished", scanned.Unfinished)
	}
	var due time.Time
	err := s.store.Update(ctx, func(tx *store.Tx) error {
		if c.Mode == ModePoll {
			task, made, err := Poll(ctx, tx, c)
			if err != nil {
				return err
			}
			if made {
				s.log.InfoContext(ctx, "made a poll task", "connector", c.Name, "task", task.ID, "capability", task.Capability)
			}
		}
		cur, ok, err := tx.Cursor(ctx, c.Name)
		if err != nil || !ok {
			return err
		}
		if scanned.Landed > 0 {
			delivered := store.Stamp(tx.Now())
			cur.LastDeliveryTS = &delivered
		}
		switch {
		case runErr != nil:
			due, err = failedRun(c, &cur, runErr, tx.Now())
		case scanned.Unfinished:
			due = tx.Now()
		default:
			ran := store.Stamp(started)
			cur.LastRunTS = &ran
			cur.LastError, cur.LastErrorTS, cur.Failures = nil, nil, 0
			due, err = NextDue(c, cur)
		}
		if err != nil {
			return err
		}
		cur.DueAt = store.Stamp(due)
		return putCursor(ctx, tx, c, cur)
	})
	if err != nil {
		s.log.ErrorContext(ctx, "a connector's run failed", "connector", c.Name, "error", err)
		runFailed := err
		recorded := s.store.Update(ctx, func(tx *store.Tx) error {
			cur, ok, err := tx.Cursor(ctx, c.Name)
			if err != nil || !ok {
				return err
			}
			if due, err = failedRun(c, &cur, runFailed, tx.Now()); err != nil {
				return err
			}
			cur.DueAt = store.Stamp(due)
			return putCursor(ctx, tx, c, cur)
		})
		err = errors.Join(err, recorded)
	}
	return due, errors.Join(runErr, err)
}

// failedRun records runErr on cur at now, and answers when c runs again:
// retryAfter after its first failure in a row, doubled for each after, and at
// its next scheduled time at the latest.
func failedRun(c Connector, cur *store.Cursor, runErr error, now time.Time) (time.Time, error) {
	text := runErr.Error()
	if len(text) > maxErrorText {
		text = text[:maxErrorText] + "…"
	}
	at := store.Stamp(now)
	cur.LastError, cur.LastErrorTS = &text, &at
	cur.Failures++
	wait := retryAfter << min(cur.Failures-1, 16)
	schedule, err := parseSchedule(c.Schedule)
	if err != nil {
		return time.Time{}, err
	}
	return earliest(now.Add(wait), schedule.Next(now)).UTC(), nil
}

// Poll makes c's poll task, and answers it. Where one is already queued or
// leased, it answers that one and made is false, so a connector never polls
// twice at once.
func Poll(ctx context.Context, tx *store.Tx, c Connector) (task store.Task, made bool, err error) {
	if task, inFlight, err := tx.PollInFlight(ctx, c.Name); err != nil || inFlight {
		return task, false, err
	}
	task, err = tx.Enqueue(ctx, store.PollTask(c.Name, c.Source))
	return task, err == nil, err
}

// DueNow makes c due at the write's time, for the scheduler's next pass to run.
func DueNow(ctx context.Context, tx *store.Tx, c Connector) (time.Time, error) {
	cur, ok, err := tx.Cursor(ctx, c.Name)
	if err != nil {
		return time.Time{}, err
	}
	if !ok {
		return time.Time{}, fmt.Errorf("connector %s: %w", c.Name, store.ErrNoCursor)
	}
	cur.DueAt = store.Stamp(tx.Now())
	return tx.Now(), putCursor(ctx, tx, c, cur)
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
