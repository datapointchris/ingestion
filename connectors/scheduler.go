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
// is logged and leaves the rest to run.
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
		if c.Mode != ModePoll && c.Mode != ModeFolder {
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

// run runs c once and answers when it is next due.
func (s *Scheduler) run(ctx context.Context, c Connector) (time.Time, error) {
	started := s.store.Now()
	var scanned Scanned
	var scanErr error
	if c.Mode == ModeFolder {
		scanned, scanErr = ScanFolder(ctx, s.lander, c)
		if scanErr != nil {
			s.log.ErrorContext(ctx, "a folder scan failed", "connector", c.Name, "root", c.Root, "error", scanErr)
		}
		s.log.InfoContext(ctx, "scanned a folder", "connector", c.Name, "root", c.Root, "files", scanned.Files,
			"landed", scanned.Landed, "skipped", scanned.Skipped)
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
		ran := store.Stamp(started)
		cur.LastRunTS = &ran
		if scanned.Landed > 0 {
			delivered := store.Stamp(tx.Now())
			cur.LastDeliveryTS = &delivered
		}
		if due, err = NextDue(c, cur); err != nil {
			return err
		}
		cur.DueAt = store.Stamp(due)
		return tx.PutCursor(ctx, cur)
	})
	return due, errors.Join(scanErr, err)
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
	return tx.Now(), tx.PutCursor(ctx, cur)
}

func earliest(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}
