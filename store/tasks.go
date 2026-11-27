package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	// LapsedClaimsFail is how many leases in a row may lapse with no report
	// accepted before the claim that finds the last fails its task rather than
	// return it to the next claim. A run that ends its worker, such as an
	// extractor killed for its memory, never reports, and would otherwise take
	// down a worker every lease. A task that reports a retry, such as a delete
	// waiting out its source's quota, starts the count again.
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

// Task is one stage's work on one item, one poll of a connector, the draft of
// one prompt proposal, or one refresh of a source's metrics, which a worker
// holding its capability leases. A task names exactly one of its item, its
// connector, its proposal and its source.
type Task struct {
	ID        string  `json:"id"`
	ItemKey   *string `json:"item_key"`
	Connector *string `json:"connector"`
	// Destination names the destination that a deliver task writes its item
	// to, and is nil for every other task.
	Destination *string `json:"destination"`
	// Model names the model that a measurement arm's task asks its run for,
	// and is nil for every other task.
	Model *string `json:"model"`
	// SightingConnector names the connector whose sighting a delete task
	// removes from that connector's source, and is nil for every other task.
	SightingConnector *string `json:"sighting_connector"`
	// ProposalID names the prompt proposal that a prompt-proposal task drafts, and is
	// nil for every other task.
	ProposalID *string `json:"proposal_id"`
	// Source names the source whose metrics a refresh-metrics task reads
	// again, and is nil for every other task.
	Source     *string `json:"source"`
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
	// Attempts counts the claims that leased the task, but each that ended
	// waiting.
	Attempts int `json:"attempts"`
	// Lapses counts the leases in a row that lapsed with no report accepted.
	Lapses int `json:"lapses"`
	// WaitingSinceTS is when a report first said the task waits, and nil where
	// none has.
	WaitingSinceTS *string `json:"waiting_since_ts"`
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
	return Task{Connector: &connector, Stage: StagePoll, Capability: PollCapability(source)}
}

// PollCapability is the capability of a poll of a source's connector, so a
// worker claims only the polls of the sources it reads.
func PollCapability(source string) string { return StagePoll + ":" + source }

// DeliverCapability is the capability of a deliver task that deliverer runs,
// so a worker claims only the tasks of the deliverers it holds.
func DeliverCapability(deliverer string) string { return StageDeliver + ":" + deliverer }

// DeleteCapability is the capability of a delete from source, so a worker
// claims only the deletes of the sources whose deleter it opened.
func DeleteCapability(source string) string { return StageDelete + ":" + source }

// DeleteTask is the task that removes the item under key from the source of
// connector, which a worker holding the capability delete:<source> claims.
func DeleteTask(key, connector, source string) Task {
	return Task{ItemKey: &key, SightingConnector: &connector, Stage: StageDelete, Capability: DeleteCapability(source)}
}

// RefreshMetricsCapability is the capability of a refresh of a source's
// metrics, so a worker claims only the refreshes of the sources whose
// refresher it opened.
func RefreshMetricsCapability(source string) string { return StageRefreshMetrics + ":" + source }

// RefreshMetricsTask is the task that reads again, at source, the metrics of
// each item that its connectors sighted and that waits in review, which a
// worker holding the capability refresh-metrics:<source> claims.
func RefreshMetricsTask(source string) Task {
	return Task{Source: &source, Stage: StageRefreshMetrics, Capability: RefreshMetricsCapability(source)}
}

// RefreshMetricsOutstanding is source's refresh-metrics task that is queued
// or leased, and false where none is.
func (t *Tx) RefreshMetricsOutstanding(ctx context.Context, source string) (Task, bool, error) {
	return first[Task](ctx, t.tx, table("tasks"), `source = ? AND stage = ? AND state IN ('queued', 'leased') ORDER BY created_ts`,
		source, StageRefreshMetrics)
}

// NewestRefreshMetrics is source's newest refresh-metrics task, whatever its
// state, and false where the store holds none.
func (s *Store) NewestRefreshMetrics(ctx context.Context, source string) (Task, bool, error) {
	return first[Task](ctx, s.read, table("tasks"), `source = ? AND stage = ? ORDER BY created_ts DESC, rowid DESC`,
		source, StageRefreshMetrics)
}

// DeleteOutstanding says whether a delete of the item under key from
// connector's source is queued, leased, or failed. A failed delete is one
// that the source refused for good, which waits for a person to retry it.
func (t *Tx) DeleteOutstanding(ctx context.Context, key, connector string) (bool, error) {
	_, found, err := first[Task](ctx, t.tx, table("tasks"),
		`item_key = ? AND stage = ? AND sighting_connector = ? AND state IN ('queued', 'leased', 'failed')`, key, StageDelete, connector)
	return found, err
}

// modelStages are the stages that read an item with a model, where a
// connector's first fill holds every item after the first to reach one.
var modelStages = []string{StageContents, StageQuality, StageQualityArm, StageModelRead}

// ModelStages are the stages that read an item with a model.
func ModelStages() []string { return slices.Clone(modelStages) }

// Enqueue adds a queued task for task's item, connector, proposal or source,
// stage and capability, and answers it as written. A model stage's task for
// an item that a connector's first fill holds is held by that connector.
func (t *Tx) Enqueue(ctx context.Context, task Task) (Task, error) {
	named := 0
	for _, subject := range []*string{task.ItemKey, task.Connector, task.ProposalID, task.Source} {
		if subject != nil {
			named++
		}
	}
	if named != 1 {
		return task, fmt.Errorf("a %s task names one of its item, its connector, its proposal and its source", task.Stage)
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
// queued tasks it returned. A held task that a person's placement already
// finished loses its hold too, and is not counted.
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
RETURNING held_by, state`, t.stamp(), connector)
	if err != nil {
		return 0, fmt.Errorf("release the tasks %s held: %w", connector, err)
	}
	defer func() { _ = rows.Close() }()
	released := 0
	for rows.Next() {
		var holder *string
		var state string
		if err := rows.Scan(&holder, &state); err != nil {
			return 0, fmt.Errorf("release the tasks %s held: %w", connector, err)
		}
		if holder == nil && state == TaskQueued {
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
	Max int
	// MaxByCapability bounds how many tasks of each capability it names the
	// claim leases, within Max. A capability that it does not name is bound
	// by Max alone.
	MaxByCapability map[string]int
	// SlotGroups bound how many tasks the claim leases of each group's
	// capabilities together, within Max, such as two heavy parses that the
	// worker's memory holds only one of. A capability sits in one group at
	// most, and a grouped one takes its group's bound in place of its own.
	SlotGroups map[string]SlotGroup
	Lease      time.Duration
	// Admit, where it is not nil, runs before the claim leases each task. It
	// answers why nobody should run the task now, and "" otherwise. Such a
	// task may be a poll of a connector that the set in force lacks, or a
	// refresh with no item left to read. When it answers a reason, the claim
	// ends the task done with that reason as its error, and leases it to
	// nobody.
	Admit func(ctx context.Context, tx *Tx, task Task) (string, error)
	// Lapsed, where it is not nil, runs in the claim's write whenever the
	// claim fails a task because its leases lapsed. It gets the reason that
	// the task keeps.
	Lapsed func(ctx context.Context, tx *Tx, task Task, reason string) error
}

// SlotGroup is capabilities that share one bound in a claim.
type SlotGroup struct {
	Capabilities []string
	Max          int
}

// slotPartitions is the SQL that names each task's bound in a claim, its
// arguments, and the bound of each partition: a grouped capability's group,
// and every other capability itself. A group's partition takes a prefix that
// no capability holds, so a group named like a capability never shares its
// bound.
func (c Claim) slotPartitions() (partition string, args []any, bounds map[string]int) {
	bounds = map[string]int{}
	for capability, most := range c.MaxByCapability {
		bounds[capability] = most
	}
	partition = "capability"
	var whens []string
	for _, name := range slices.Sorted(maps.Keys(c.SlotGroups)) {
		group := "slot-group " + name
		bounds[group] = c.SlotGroups[name].Max
		for _, capability := range c.SlotGroups[name].Capabilities {
			delete(bounds, capability)
			whens = append(whens, " WHEN ? THEN ?")
			args = append(args, capability, group)
		}
	}
	if len(whens) > 0 {
		partition = "CASE capability" + strings.Join(whens, "") + " ELSE capability END"
	}
	return partition, args, bounds
}

// Leased is a leased task with its item's row, so a worker runs the task
// holding no scope to read items. A poll task has no item, so its Item is
// empty. A deliver task carries its delivery's row too, whose title, URL and
// source time every attempt sends. A model stage's task carries the item's
// outputs at its stage, so a task whose run is already dispatched reads that
// run rather than starting another. A model read's task carries the item's
// newest read decision, whose lens and note the read answers. A poll carries
// its connector's cursor, whose position it reads on from. A delete task
// carries the sighting whose source it removes the item from. A prompt-proposal task
// carries the prompt proposal that it drafts, which holds its run's dispatch.
type Leased struct {
	Task
	Item     Item
	Delivery *Delivery
	Outputs  []Output
	Read     *Decision
	Cursor   *Cursor
	Sighting *Sighting
	Proposal *PromptProposal
}

// Claim records the worker's heartbeat and leases it up to c.Max tasks whose
// capability it holds, the oldest first: each queued task past its backoff
// that no first fill holds, and each leased task whose lease lapsed. It leases
// no more of a capability than c.MaxByCapability names, and no more of a slot
// group's capabilities together than c.SlotGroups names. Every claim runs
// through the one writer, so two workers asking at once never lease one task
// twice.
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
		partition, args, bounds := c.slotPartitions()
		for _, capability := range c.Worker.Capabilities {
			args = append(args, capability)
		}
		args = append(args, now, now)
		// Each claimable task's place among its partition's, the oldest
		// first, which the partition's own bound caps: its slot group's, or
		// its capability's.
		bound := "?"
		if len(bounds) > 0 {
			bound = "CASE slot" + strings.Repeat(" WHEN ? THEN ?", len(bounds)) + " ELSE ? END"
			for _, slot := range slices.Sorted(maps.Keys(bounds)) {
				args = append(args, slot, bounds[slot])
			}
		}
		args = append(args, c.Max, c.Max)
		ids, err := tx.ids(ctx, `SELECT id FROM (
				SELECT id, slot, created_ts, task_rowid,
					row_number() OVER (PARTITION BY slot ORDER BY created_ts, task_rowid) AS place
				FROM (SELECT id, `+partition+` AS slot, created_ts, rowid AS task_rowid
					FROM tasks
					WHERE capability IN (?`+strings.Repeat(", ?", len(c.Worker.Capabilities)-1)+`)
					AND ((state = 'queued' AND held_by IS NULL AND (not_before_ts IS NULL OR not_before_ts <= ?))
						OR (state = 'leased' AND lease_expires_ts <= ?))))
			WHERE place <= `+bound+`
			ORDER BY created_ts, task_rowid LIMIT ?`, args...)
		if err != nil {
			return fmt.Errorf("find tasks to claim: %w", err)
		}
		expires := Stamp(tx.at.Add(c.Lease))
		for _, id := range ids {
			found, err := tx.task(ctx, id)
			if err != nil {
				return err
			}
			if c.Admit != nil {
				withdraw, err := c.Admit(ctx, tx, found)
				if err != nil {
					return err
				}
				if withdraw != "" {
					if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', error = ?, updated_ts = ? WHERE id = ?`,
						withdraw, now, id); err != nil {
						return fmt.Errorf("withdraw task %s: %w", id, err)
					}
					continue
				}
			}
			lapses := found.Lapses
			if found.State == TaskLeased {
				lapses++
			}
			if lapses >= LapsedClaimsFail {
				reason := fmt.Sprintf("its lease lapsed %d times in a row with no report accepted", lapses)
				if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'failed', lapses = ?, error = ?, updated_ts = ? WHERE id = ?`,
					lapses, reason, now, id); err != nil {
					return fmt.Errorf("fail task %s: %w", id, err)
				}
				if found.ProposalID != nil {
					if err := tx.failProposalRun(ctx, *found.ProposalID, reason); err != nil {
						return err
					}
				}
				if c.Lapsed != nil {
					if err := c.Lapsed(ctx, tx, found, reason); err != nil {
						return err
					}
				}
				continue
			}
			if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'leased', lease_holder = ?,
				lease_token = coalesce(lease_token, 0) + 1, lease_expires_ts = ?, attempts = attempts + 1, lapses = ?, updated_ts = ?
				WHERE id = ?`, c.Worker.ID, expires, lapses, now, id); err != nil {
				return fmt.Errorf("lease task %s: %w", id, err)
			}
			task, err := tx.task(ctx, id)
			if err != nil {
				return err
			}
			leased := Leased{Task: task}
			if task.ItemKey != nil {
				if leased.Item, _, err = tx.Item(ctx, *task.ItemKey); err != nil {
					return err
				}
			}
			if task.ItemKey != nil && slices.Contains(modelStages, task.Stage) {
				if leased.Outputs, err = readInto[Output](ctx, tx.tx, table("outputs"), `item_key = ? AND stage = ? ORDER BY created_ts, rowid`,
					*task.ItemKey, task.Stage); err != nil {
					return err
				}
			}
			if task.Stage == StageModelRead && task.ItemKey != nil {
				read, ok, err := tx.NewestDecision(ctx, *task.ItemKey, GateRead)
				if err != nil {
					return err
				}
				if ok {
					leased.Read = &read
				}
			}
			if task.Stage == StageDeliver && task.ItemKey != nil && task.Destination != nil {
				d, ok, err := tx.Delivery(ctx, *task.ItemKey, *task.Destination)
				if err != nil {
					return err
				}
				if ok {
					leased.Delivery = &d
				}
			}
			if task.Stage == StagePoll && task.Connector != nil {
				cur, ok, err := tx.Cursor(ctx, *task.Connector)
				if err != nil {
					return err
				}
				if ok {
					leased.Cursor = &cur
				}
			}
			if task.Stage == StageDelete && task.ItemKey != nil && task.SightingConnector != nil {
				s, ok, err := tx.Sighting(ctx, *task.ItemKey, *task.SightingConnector)
				if err != nil {
					return err
				}
				if ok {
					leased.Sighting = &s
				}
			}
			if task.ProposalID != nil {
				p, ok, err := tx.PromptProposal(ctx, *task.ProposalID)
				if err != nil {
					return err
				}
				if ok {
					leased.Proposal = &p
				}
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
	// Wait marks a task queued again because its far end asked it to wait, or
	// its worker stopped, so the claim spends none of its attempts.
	Wait bool
}

// Report ends the lease that token holds on task id, with outcome. write runs
// in the same transaction before the lease ends, so a stage's records, such
// as a model stage's dispatch id, are stored exactly when the task ends. write
// may change the outcome, such as to fail a poll whose report it could land
// only in part. The store refuses a token that does not hold the lease with
// ErrStaleLease, and write never runs. A report repeated under the token that
// ended the task answers the task as it ended, and write never runs again, so
// a worker whose answer was lost can send its report again.
func (s *Store) Report(ctx context.Context, id string, token int64, outcome Outcome, write func(*Tx, Task, *Outcome) error) (Task, error) {
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
			if err := write(tx, task, &outcome); err != nil {
				return err
			}
		}
		var notBefore *string
		if outcome.State == TaskQueued {
			at := Stamp(tx.at.Add(outcome.RetryAfter))
			notBefore = &at
		}
		if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = ?, error = ?, not_before_ts = ?, lapses = 0, updated_ts = ? WHERE id = ?`,
			outcome.State, outcome.Error, notBefore, tx.stamp(), id); err != nil {
			return fmt.Errorf("end task %s: %w", id, err)
		}
		if outcome.State == TaskQueued && outcome.Wait {
			if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET attempts = max(attempts - 1, 0),
				waiting_since_ts = coalesce(waiting_since_ts, ?) WHERE id = ?`, tx.stamp(), id); err != nil {
				return fmt.Errorf("end task %s waiting: %w", id, err)
			}
		}
		task, err = tx.task(ctx, id)
		return err
	})
	return task, err
}

// WithdrawQueued ends each queued task of the item under key at stage done,
// with why as its error, where a later decision made the stage needless. A
// task already leased runs on.
func (t *Tx) WithdrawQueued(ctx context.Context, key, stage, why string) error {
	if _, err := t.tx.ExecContext(ctx, `UPDATE tasks SET state = 'done', error = ?, updated_ts = ?
		WHERE item_key = ? AND stage = ? AND state = 'queued'`, why, t.stamp(), key, stage); err != nil {
		return fmt.Errorf("withdraw the %s tasks of %s: %w", stage, key, err)
	}
	return nil
}

// ErrTaskDone is a retry of a task that ended done, which a replay runs again
// instead.
var ErrTaskDone = errors.New("the task ended done")

// ErrTaskSuperseded is a retry of a failed draft whose misses a later prompt
// proposal reads, so a retry would draft them twice.
var ErrTaskSuperseded = errors.New("a later prompt proposal reads its misses")

// ErrRunWithoutSubject is a run reported for a task that names no item and no
// prompt proposal, such as a poll's.
var ErrRunWithoutSubject = errors.New("the task names no item or prompt proposal to keep a run on")

// KeepRun writes a run that task's worker reported where its subject keeps
// it: an item's run as the item's output at the task's stage, with the units
// a parse wrote, and a prompt proposal's run on the proposal.
func (t *Tx) KeepRun(ctx context.Context, task Task, o Output, units []Unit) error {
	switch {
	case task.ItemKey != nil:
		o.ItemKey, o.Stage = *task.ItemKey, task.Stage
		return t.PutOutputWithUnits(ctx, o, units)
	case task.ProposalID != nil:
		if len(units) > 0 {
			return fmt.Errorf("%w: a %s run carries %d units", ErrUnitsOutsideParse, task.Stage, len(units))
		}
		return t.keepProposalRun(ctx, *task.ProposalID, o.StageRun)
	}
	return ErrRunWithoutSubject
}

// RetryTask queues the failed task id again, with its attempts and backoff
// cleared, and answers it. A task that is queued or leased is answered as it
// is, so a retry sent twice queues it once. It answers ErrNoTask where there
// is no such task, ErrTaskDone where it ended done, and ErrTaskSuperseded
// where it drafts a proposal that a later one replaced.
func (s *Store) RetryTask(ctx context.Context, id string) (Task, error) {
	var task Task
	err := s.Update(ctx, func(tx *Tx) error {
		var err error
		if task, err = tx.task(ctx, id); err != nil {
			return err
		}
		switch task.State {
		case TaskDone:
			return fmt.Errorf("task %s: %w", id, ErrTaskDone)
		case TaskQueued, TaskLeased:
			return nil
		}
		if task.ProposalID != nil {
			newest, ok, err := tx.NewestPromptProposal(ctx)
			if err != nil {
				return err
			}
			if ok && newest.ID != *task.ProposalID {
				return fmt.Errorf("task %s drafts proposal %s, and proposal %s reads its misses: %w", id, *task.ProposalID, newest.ID, ErrTaskSuperseded)
			}
		}
		if _, err := tx.tx.ExecContext(ctx, `UPDATE tasks SET state = 'queued', attempts = 0, lapses = 0, waiting_since_ts = NULL, error = NULL,
			not_before_ts = NULL, updated_ts = ? WHERE id = ?`, tx.stamp(), id); err != nil {
			return fmt.Errorf("queue task %s again: %w", id, err)
		}
		task, err = tx.task(ctx, id)
		return err
	})
	return task, err
}

// OpenTask reads the item under key's task at stage that is queued or leased,
// and false where none is.
func (t *Tx) OpenTask(ctx context.Context, key, stage string) (Task, bool, error) {
	return first[Task](ctx, t.tx, table("tasks"), `item_key = ? AND stage = ? AND state IN ('queued', 'leased') ORDER BY created_ts`, key, stage)
}

// HasTask says whether the item under key ever held a task at stage.
func (t *Tx) HasTask(ctx context.Context, key, stage string) (bool, error) {
	_, found, err := first[Task](ctx, t.tx, table("tasks"), `item_key = ? AND stage = ?`, key, stage)
	return found, err
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
