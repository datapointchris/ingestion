package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Task states.
const (
	TaskQueued = "queued"
	TaskLeased = "leased"
	TaskDone   = "done"
	TaskFailed = "failed"
)

const (
	// LapsedClaimsFail is the claim whose lapsed lease fails its task rather
	// than return it to the next claim. A run that ends its worker, such as an
	// extractor killed for its memory, never reports, and would otherwise take
	// down a worker every lease.
	LapsedClaimsFail = 3
	// HeartbeatEvery is how often a worker's claims write its row, which is
	// how fresh a worker's last heartbeat reads.
	HeartbeatEvery = time.Minute
)

var (
	// ErrNoTask is a call naming a task the store does not hold.
	ErrNoTask = errors.New("no such task")
	// ErrStaleLease is a heartbeat or report under a token that does not hold
	// the task's lease: a later claim took the lapsed lease, or the task ended.
	ErrStaleLease = errors.New("the task's lease is not held under this token")
)

// Task is one stage's work on one item, or one poll of a connector, which a
// worker holding its capability leases. A task names its item or its
// connector, never both.
type Task struct {
	ID         string  `json:"id"`
	ItemKey    *string `json:"item_key"`
	Connector  *string `json:"connector"`
	Stage      string  `json:"stage"`
	Capability string  `json:"capability"`
	State      string  `json:"state"`
	// HeldBy names the connector whose first fill holds this model stage's
	// task. No claim takes it until a person releases the fill.
	HeldBy *string `json:"held_by"`
	// LeaseHolder is the worker that claimed the task last.
	LeaseHolder *string `json:"lease_holder"`
	// LeaseToken is the fencing token of the last claim. Each claim takes one
	// higher, so a report under an older token is refused.
	LeaseToken     *int64  `json:"lease_token"`
	LeaseExpiresTS *string `json:"lease_expires_ts"`
	Attempts       int     `json:"attempts"`
	NotBeforeTS    *string `json:"not_before_ts"`
	Error          *string `json:"error"`
	CreatedTS      string  `json:"created_ts"`
	UpdatedTS      string  `json:"updated_ts"`
}

// Lapsed says whether t is leased and its lease ran out by now, which returns
// it to any worker's claim.
func (t Task) Lapsed(now time.Time) bool {
	if t.State != TaskLeased || t.LeaseExpiresTS == nil {
		return false
	}
	expires, err := time.Parse(StampLayout, *t.LeaseExpiresTS)
	return err == nil && !expires.After(now)
}

// PollTask is the task that polls connector once, which a worker holding the
// capability poll:<source> claims.
func PollTask(connector, source string) Task {
	return Task{Connector: &connector, Stage: StagePoll, Capability: "poll:" + source}
}

// modelStages are the stages that read an item with a model, where a
// connector's first fill holds every item after the first to reach one.
var modelStages = []string{StageQuality, StageModelRead}

// Enqueue adds a queued task for task's item or connector, stage and
// capability, and answers it as written. A model stage's task for an item that
// a connector's first fill holds is held by that connector.
func (t *Tx) Enqueue(ctx context.Context, task Task) (Task, error) {
	if (task.ItemKey == nil) == (task.Connector == nil) {
		return task, fmt.Errorf("a %s task names its item or its connector, and not both", task.Stage)
	}
	task.ID = randomHex(16)
	task.State, task.Attempts = TaskQueued, 0
	task.HeldBy, task.LeaseHolder, task.LeaseToken, task.LeaseExpiresTS, task.Error = nil, nil, nil, nil, nil
	if task.ItemKey != nil && slices.Contains(modelStages, task.Stage) {
		holder, err := t.firstFillHold(ctx, *task.ItemKey)
		if err != nil {
			return task, err
		}
		task.HeldBy = holder
	}
	task.CreatedTS, task.UpdatedTS = t.stamp(), t.stamp()
	return task, t.put(ctx, table("tasks"), task)
}

// firstFillHold is the connector whose first fill holds item at the model
// stages, and nil where none does. The first item of a connector's fill to
// reach a model stage runs, and becomes the item its fill ran end to end.
// Every later one waits until a person releases the fill. An item sighted by
// several connectors waits where any of them holds it.
func (t *Tx) firstFillHold(ctx context.Context, item string) (*string, error) {
	cursors, err := readInto[Cursor](ctx, t.tx, table("cursors"),
		`connector IN (SELECT connector FROM sightings WHERE item_key = ?) ORDER BY connector`, item)
	if err != nil {
		return nil, err
	}
	for _, c := range cursors {
		if c.FirstFill == FirstFillHeld && (c.FirstFillItemKey == nil || *c.FirstFillItemKey != item) {
			return &c.Connector, nil
		}
	}
	for _, c := range cursors {
		if c.FirstFill != FirstFillNone {
			continue
		}
		c.FirstFill, c.FirstFillItemKey = FirstFillHeld, &item
		if err := t.PutCursor(ctx, c); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// ReleaseFirstFill marks connector's first fill released, and returns to the
// claims every task it held whose item no other connector's fill holds. A task
// whose item another held fill sighted moves to that fill. It answers how many
// tasks it returned.
func (t *Tx) ReleaseFirstFill(ctx context.Context, connector string) (int, error) {
	c, ok, err := t.Cursor(ctx, connector)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("connector %s: %w", connector, ErrNoCursor)
	}
	if c.FirstFill != FirstFillReleased {
		c.FirstFill = FirstFillReleased
		if err := t.PutCursor(ctx, c); err != nil {
			return 0, err
		}
	}
	rows, err := t.tx.QueryContext(ctx, `
UPDATE tasks SET updated_ts = ?, held_by = (
	SELECT c.connector FROM cursors c JOIN sightings s ON s.connector = c.connector
	WHERE s.item_key = tasks.item_key AND c.first_fill = 'held'
		AND (c.first_fill_item_key IS NULL OR c.first_fill_item_key != tasks.item_key)
	ORDER BY c.connector LIMIT 1)
WHERE held_by = ?
RETURNING held_by`, t.stamp(), connector)
	if err != nil {
		return 0, fmt.Errorf("release the tasks %s held: %w", connector, err)
	}
	defer func() { _ = rows.Close() }()
	released := 0
	for rows.Next() {
		var holder *string
		if err := rows.Scan(&holder); err != nil {
			return 0, fmt.Errorf("release the tasks %s held: %w", connector, err)
		}
		if holder == nil {
			released++
		}
	}
	return released, rows.Err()
}

// ErrNoCursor names a connector with no cursor, because no applied set
// declared it.
var ErrNoCursor = errors.New("no cursor for the connector")

// PollInFlight is connector's poll task that is queued or leased, and false
// where none is.
func (t *Tx) PollInFlight(ctx context.Context, connector string) (Task, bool, error) {
	return first[Task](ctx, t.tx, table("tasks"), `connector = ? AND stage = ? AND state IN ('queued', 'leased') ORDER BY created_ts`,
		connector, StagePoll)
}

// Worker is one worker process, as its claims and heartbeats record it.
type Worker struct {
	ID           string   `json:"id"`
	Capabilities []string `json:"capabilities"`
	Version      string   `json:"version"`
	StartedTS    string   `json:"started_ts"`
	HeartbeatTS  string   `json:"heartbeat_ts"`
}

// Claim is a worker asking for work.
type Claim struct {
	Worker Worker
	// Max bounds how many tasks the claim leases.
	Max   int
	Lease time.Duration
}

// Leased is a leased task with the media type and bytes of its item, so a
// worker runs the task holding no scope to read items. A poll task has no
// item, so it carries neither.
type Leased struct {
	Task
	MediaType  string
	BlobSHA256 *string
}

// Claim records the worker's heartbeat and leases it up to c.Max tasks whose
// capability it holds: each queued task past its backoff that no first fill
// holds, and each leased task whose lease lapsed. Every claim runs through the
// one writer, so two workers asking at once never lease one task twice.
func (s *Store) Claim(ctx context.Context, c Claim) ([]Leased, error) {
	var claimed []Leased
	err := s.Update(ctx, func(tx *Tx) error {
		now := tx.stamp()
		worker := c.Worker
		if worker.Capabilities == nil {
			worker.Capabilities = []string{}
		}
		if worker.StartedTS == "" {
			worker.StartedTS = now
		}
		worker.HeartbeatTS = now
		if err := tx.keepWorker(ctx, worker); err != nil {
			return err
		}
		if len(c.Worker.Capabilities) == 0 || c.Max <= 0 {
			return nil
		}
		args := make([]any, 0, len(c.Worker.Capabilities)+3)
		for _, capability := range c.Worker.Capabilities {
			args = append(args, capability)
		}
		args = append(args, now, now, c.Max)
		ids, err := tx.ids(ctx, `SELECT id FROM tasks
			WHERE capability IN (?`+strings.Repeat(", ?", len(c.Worker.Capabilities)-1)+`)
			AND ((state = 'queued' AND held_by IS NULL AND (not_before_ts IS NULL OR not_before_ts <= ?))
				OR (state = 'leased' AND lease_expires_ts <= ?))
			ORDER BY created_ts, rowid LIMIT ?`, args...)
		if err != nil {
			return fmt.Errorf("find tasks to claim: %w", err)
		}
		expires := Stamp(tx.at.Add(c.Lease))
		for _, id := range ids {
			lapsed, err := tx.task(ctx, id)
			if err != nil {
				return err
			}
			if lapsed.State == TaskLeased && lapsed.Attempts >= LapsedClaimsFail {
				reason := fmt.Sprintf("its lease lapsed on claim %d with no report, so its run likely ends its worker", lapsed.Attempts)
				if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'failed', error = ?, updated_ts = ? WHERE id = ?`,
					reason, now, id); err != nil {
					return fmt.Errorf("fail task %s: %w", id, err)
				}
				continue
			}
			if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'leased', lease_holder = ?,
				lease_token = coalesce(lease_token, 0) + 1, lease_expires_ts = ?, attempts = attempts + 1, updated_ts = ?
				WHERE id = ?`, c.Worker.ID, expires, now, id); err != nil {
				return fmt.Errorf("lease task %s: %w", id, err)
			}
			task, err := tx.task(ctx, id)
			if err != nil {
				return err
			}
			leased := Leased{Task: task}
			if task.ItemKey != nil {
				item, _, err := tx.Item(ctx, *task.ItemKey)
				if err != nil {
					return err
				}
				leased.MediaType, leased.BlobSHA256 = item.MediaType, item.BlobSHA256
			}
			claimed = append(claimed, leased)
		}
		return nil
	})
	return claimed, err
}

// Heartbeat extends the lease on task id that token holds by lease from now,
// and records its worker's heartbeat.
func (s *Store) Heartbeat(ctx context.Context, id string, token int64, lease time.Duration) (Task, error) {
	var task Task
	err := s.Update(ctx, func(tx *Tx) error {
		var err error
		if task, err = tx.leased(ctx, id, token); err != nil {
			return err
		}
		now := tx.stamp()
		if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET lease_expires_ts = ?, updated_ts = ? WHERE id = ?`,
			Stamp(tx.at.Add(lease)), now, id); err != nil {
			return fmt.Errorf("extend the lease on task %s: %w", id, err)
		}
		if task.LeaseHolder != nil {
			if _, err := tx.tx.ExecContext(ctx, `UPDATE workers SET heartbeat_ts = ? WHERE id = ?`, now, *task.LeaseHolder); err != nil {
				return fmt.Errorf("record worker %s's heartbeat: %w", *task.LeaseHolder, err)
			}
		}
		task, err = tx.task(ctx, id)
		return err
	})
	return task, err
}

// Leased reads task id where token holds its lease, so a worker reads the
// input of a task it holds and of no other. It answers ErrNoTask where there
// is no such task, and ErrStaleLease where token does not hold its lease.
func (s *Store) Leased(ctx context.Context, id string, token int64) (Task, error) {
	task, found, err := first[Task](ctx, s.read, table("tasks"), `id = ?`, id)
	switch {
	case err != nil:
		return task, err
	case !found:
		return task, fmt.Errorf("task %s: %w", id, ErrNoTask)
	case task.State != TaskLeased || task.LeaseToken == nil || *task.LeaseToken != token:
		return task, fmt.Errorf("task %s under token %d: %w", id, token, ErrStaleLease)
	}
	return task, nil
}

// Outcome is how a worker's task ended.
type Outcome struct {
	// State is TaskDone, TaskFailed, or TaskQueued to try again after
	// RetryAfter.
	State      string
	Error      *string
	RetryAfter time.Duration
}

// Report ends the lease that token holds on task id, with outcome. write runs
// in the same transaction before the lease ends, so a stage's records, such
// as a model stage's dispatch id, are stored exactly when the task ends. The
// store refuses a token that does not hold the lease with ErrStaleLease, and
// write never runs. A report repeated under the token that ended the task
// answers the task as it ended, and write never runs again, so a worker whose
// answer was lost can send its report again.
func (s *Store) Report(ctx context.Context, id string, token int64, outcome Outcome, write func(*Tx, Task) error) (Task, error) {
	switch outcome.State {
	case TaskDone, TaskFailed, TaskQueued:
	default:
		return Task{}, fmt.Errorf("a task ends done, failed or queued again, not %q", outcome.State)
	}
	var task Task
	err := s.Update(ctx, func(tx *Tx) error {
		var err error
		task, err = tx.leased(ctx, id, token)
		if errors.Is(err, ErrStaleLease) && reportedUnder(task, token) {
			return nil
		}
		if err != nil {
			return err
		}
		if write != nil {
			if err := write(tx, task); err != nil {
				return err
			}
		}
		var notBefore *string
		if outcome.State == TaskQueued {
			at := Stamp(tx.at.Add(outcome.RetryAfter))
			notBefore = &at
		}
		if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = ?, error = ?, not_before_ts = ?, updated_ts = ? WHERE id = ?`,
			outcome.State, outcome.Error, notBefore, tx.stamp(), id); err != nil {
			return fmt.Errorf("end task %s: %w", id, err)
		}
		task, err = tx.task(ctx, id)
		return err
	})
	return task, err
}

// TaskQuery selects tasks to list.
type TaskQuery struct {
	// States keeps tasks in these states, and every state but done where empty.
	States []string
	Limit  int
}

// Tasks lists the tasks q selects, the most recently changed first.
func (s *Store) Tasks(ctx context.Context, q TaskQuery) ([]Task, error) {
	states := q.States
	if len(states) == 0 {
		states = []string{TaskQueued, TaskLeased, TaskFailed}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	args := make([]any, 0, len(states)+1)
	for _, state := range states {
		args = append(args, state)
	}
	args = append(args, limit)
	return readInto[Task](ctx, s.read, table("tasks"),
		`state IN (?`+strings.Repeat(", ?", len(states)-1)+`) ORDER BY updated_ts DESC, rowid DESC LIMIT ?`, args...)
}

// Workers lists each worker heard from at or after since, the most recent
// heartbeat first, and counts the workers left out. Every since lists every
// worker the store holds.
func (s *Store) Workers(ctx context.Context, since string) ([]Worker, int, error) {
	workers, err := readInto[Worker](ctx, s.read, table("workers"), `heartbeat_ts >= ? ORDER BY heartbeat_ts DESC`, since)
	if err != nil {
		return nil, 0, err
	}
	var hidden int
	if err := s.read.QueryRowContext(ctx, `SELECT count(*) FROM workers WHERE heartbeat_ts < ?`, since).Scan(&hidden); err != nil {
		return nil, 0, fmt.Errorf("count the workers left out: %w", err)
	}
	return workers, hidden, nil
}

// keepWorker writes w's row where it is new, where its capabilities or version
// changed, or where its last heartbeat is HeartbeatEvery old. A claim
// every poll would otherwise ship a version of the row each poll.
func (t *Tx) keepWorker(ctx context.Context, w Worker) error {
	held, err := readInto[Worker](ctx, t.tx, table("workers"), `id = ?`, w.ID)
	if err != nil {
		return err
	}
	if len(held) == 1 {
		last, err := time.Parse(StampLayout, held[0].HeartbeatTS)
		same := held[0].Version == w.Version && strings.Join(held[0].Capabilities, ",") == strings.Join(w.Capabilities, ",")
		if err == nil && same && t.at.Sub(last) < HeartbeatEvery {
			return nil
		}
	}
	return t.put(ctx, table("workers"), w)
}

// reportedUnder says whether task already ended under token, so a report
// repeated after its answer was lost finds the task as the first one left it.
// A later claim raises the token, so a repeat after one is still stale.
func reportedUnder(task Task, token int64) bool {
	return task.State != TaskLeased && task.LeaseToken != nil && *task.LeaseToken == token
}

// leased is task id where token holds its lease, ErrNoTask where there is no
// such task, and ErrStaleLease where token does not hold it.
func (t *Tx) leased(ctx context.Context, id string, token int64) (Task, error) {
	task, err := t.task(ctx, id)
	if err != nil {
		return task, err
	}
	if task.State != TaskLeased || task.LeaseToken == nil || *task.LeaseToken != token {
		return task, fmt.Errorf("task %s under token %d: %w", id, token, ErrStaleLease)
	}
	return task, nil
}

func (t *Tx) task(ctx context.Context, id string) (Task, error) {
	tasks, err := readInto[Task](ctx, t.tx, table("tasks"), `id = ?`, id)
	if err != nil {
		return Task{}, err
	}
	if len(tasks) == 0 {
		return Task{}, fmt.Errorf("task %s: %w", id, ErrNoTask)
	}
	return tasks[0], nil
}

func (t *Tx) ids(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
