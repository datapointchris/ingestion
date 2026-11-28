// Package worker is `ingestion worker run`. It leases tasks from the service
// by capability, runs each through the handler for its capability, extends
// the lease while the handler runs, and reports the outcome under the lease's
// token. This is the external task pattern that Camunda uses: the worker
// holds no store, and the service is the one writer.
//
// A stage declares its capability by its Handler, and cmd/worker.go lists
// every handler that the worker runs. A worker claims tasks only for the
// capabilities of its handlers.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Task is one leased task.
type Task = server.LeasedTask

// stopReportTimeout bounds the report of a task that a stopping worker hands
// back, within the pod's grace period.
const stopReportTimeout = 10 * time.Second

// Result is how a handler's run ended. It is the result that the report
// carries, so a field that the service reads is one that a handler sets.
type Result = server.TaskResult

// Handler runs the tasks of one capability.
type Handler interface {
	Capability() string
	// Run runs task. ctx ends where the lease is lost, and the result is then
	// refused.
	Run(ctx context.Context, task Task) Result
}

// SlotLimiter is a Handler that caps how many of its tasks run at once by
// default. Zero leaves them uncapped, and Config.SlotLimits overrides it. A
// handler in a slot group sets none, because its group's limit caps it.
type SlotLimiter interface {
	SlotLimit() int
}

// SlotGrouper is a Handler whose tasks share one slot limit with every other
// handler that names the same group, such as two parses whose runs each hold
// gigabytes. It names the group and the group's default limit, which every
// member names alike, and "" names no group.
type SlotGrouper interface {
	SlotGroup() (name string, limit int)
}

// Config is how a worker runs.
type Config struct {
	// ID names the worker in every claim, and in the lease of each task it
	// holds.
	ID      string
	Version string
	// Slots is how many tasks run at once.
	Slots int
	// SlotLimits caps how many tasks of a capability, or of a slot group's
	// capabilities together, run at once, within Slots, such as one slot for
	// a long parse so that the rest stay free for every other stage. A key
	// names a slot group or an ungrouped capability, and overrides the
	// handlers' own SlotLimit. One that neither names may take every slot.
	SlotLimits map[string]int
	Lease      time.Duration
	// Poll is how often an idle worker claims again.
	Poll time.Duration
	Log  *slog.Logger
}

// Run claims and runs tasks for handlers until ctx ends. A worker with no
// handler claims nothing, and its claims still record its heartbeat. Each
// claim asks for no more of a capability, or of a slot group, than its slot
// limit leaves free. Run refuses a slot limit under 1, and one that names a
// capability inside a slot group, whose limit is the group's.
func Run(ctx context.Context, client *Client, cfg Config, handlers ...Handler) error {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	for key, limit := range cfg.SlotLimits {
		if limit < 1 {
			return fmt.Errorf("the slot limit of %s is %d, and a limit is 1 or more", key, limit)
		}
	}
	if err := MarkUndumpable(); err != nil {
		return err
	}
	byCapability := map[string]Handler{}
	capabilities := []string{}
	for _, h := range handlers {
		if _, taken := byCapability[h.Capability()]; taken {
			return fmt.Errorf("two handlers declare capability %s", h.Capability())
		}
		byCapability[h.Capability()] = h
		capabilities = append(capabilities, h.Capability())
	}
	slots, err := partitionSlots(handlers, cfg.SlotLimits, cfg.Log)
	if err != nil {
		return err
	}
	started := store.Stamp(time.Now())
	cfg.Log.Info("the worker runs", "worker", cfg.ID, "capabilities", capabilities, "slots", cfg.Slots,
		"slot_limits", slots.limits, "slot_groups", slots.groups)

	var wg sync.WaitGroup
	defer wg.Wait()
	freed := make(chan string, cfg.Slots)
	running := map[string]int{}
	busy := 0
	ticker := time.NewTicker(cfg.Poll)
	defer ticker.Stop()
	for {
		free := cfg.Slots - busy
		if len(capabilities) == 0 {
			free = 0
		}
		bounds, groups := slots.claimBounds(running)
		answer, err := client.Claim(ctx, server.ClaimRequest{
			WorkerID: cfg.ID, Capabilities: capabilities, Version: cfg.Version, StartedTS: started,
			Max: free, MaxByCapability: bounds, SlotGroups: groups, LeaseSeconds: int(cfg.Lease.Seconds()),
		})
		if err != nil && ctx.Err() == nil {
			cfg.Log.Warn("claim tasks", "error", err)
		}
		for _, task := range answer.Tasks {
			h := byCapability[task.Capability]
			slot := slots.of(task.Capability)
			busy++
			running[slot]++
			wg.Go(func() {
				defer func() { freed <- slot }()
				runTask(ctx, client, cfg, h, task)
			})
		}
		select {
		case <-ctx.Done():
			return nil
		case slot := <-freed:
			busy--
			running[slot]--
		case <-ticker.C:
		}
	drained:
		for {
			select {
			case slot := <-freed:
				busy--
				running[slot]--
			default:
				break drained
			}
		}
	}
}

// slotPartitions is how a worker's slot limits fall: each grouped capability's
// group, and each limit, keyed by a group or an ungrouped capability.
type slotPartitions struct {
	grouped map[string]string
	groups  map[string][]string
	limits  map[string]int
}

// partitionSlots reads each handler's slot group and limit, then overrides
// the limits with configured. It refuses two members of a group that give it
// different limits, and a configured limit that names a grouped capability. It
// warns of one that names nothing a handler here holds.
func partitionSlots(handlers []Handler, configured map[string]int, log *slog.Logger) (slotPartitions, error) {
	p := slotPartitions{grouped: map[string]string{}, groups: map[string][]string{}, limits: map[string]int{}}
	for _, h := range handlers {
		if grouper, ok := h.(SlotGrouper); ok {
			if group, limit := grouper.SlotGroup(); group != "" {
				if held, set := p.limits[group]; set && held != limit {
					return p, fmt.Errorf("capability %s gives slot group %s the limit %d, and another of its capabilities gives %d", h.Capability(), group, limit, held)
				}
				p.grouped[h.Capability()] = group
				p.groups[group] = append(p.groups[group], h.Capability())
				p.limits[group] = limit
				continue
			}
		}
		if limiter, ok := h.(SlotLimiter); ok && limiter.SlotLimit() > 0 {
			p.limits[h.Capability()] = limiter.SlotLimit()
		}
	}
	for _, key := range slices.Sorted(maps.Keys(configured)) {
		if group, ok := p.grouped[key]; ok {
			return p, fmt.Errorf("capability %s runs in slot group %s, so its slot limit is the group's: name %s", key, group, group)
		}
		if _, isGroup := p.groups[key]; !isGroup && !slices.ContainsFunc(handlers, func(h Handler) bool { return h.Capability() == key }) {
			log.Warn("a slot limit names a capability or slot group that no handler here holds", "slot", key)
			continue
		}
		p.limits[key] = configured[key]
	}
	return p, nil
}

// of is the slot that capability's tasks count against: its group's, or its
// own.
func (p slotPartitions) of(capability string) string {
	if group, ok := p.grouped[capability]; ok {
		return group
	}
	return capability
}

// claimBounds bounds the next claim by each limited capability and slot
// group's free slots, given how many tasks of each run. A group of one
// capability bounds it as a capability does, so a service that reads no slot
// group still answers the claim.
func (p slotPartitions) claimBounds(running map[string]int) (map[string]int, map[string]server.SlotGroup) {
	var bounds map[string]int
	var groups map[string]server.SlotGroup
	for slot, limit := range p.limits {
		free := max(limit-running[slot], 0)
		if members, isGroup := p.groups[slot]; isGroup && len(members) > 1 {
			if groups == nil {
				groups = map[string]server.SlotGroup{}
			}
			groups[slot] = server.SlotGroup{Capabilities: members, Max: free}
			continue
		}
		if bounds == nil {
			bounds = map[string]int{}
		}
		if members, isGroup := p.groups[slot]; isGroup {
			slot = members[0]
		}
		bounds[slot] = free
	}
	return bounds, groups
}

// runTask runs one task, extending its lease while it runs, and reports how it
// ended. A lost lease ends the handler's context, and its result is dropped.
func runTask(ctx context.Context, client *Client, cfg Config, h Handler, task Task) {
	log := cfg.Log.With("task", task.ID, "item", deref(task.ItemKey), "connector", deref(task.Connector), "stage", task.Stage,
		"token", task.LeaseToken)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(cfg.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
			// Bounded by the next beat, so one hung call never outlasts the lease.
			beatCtx, beatDone := context.WithTimeout(runCtx, cfg.Lease/3)
			err := client.Heartbeat(beatCtx, task.ID, server.HeartbeatRequest{Token: task.LeaseToken, LeaseSeconds: int(cfg.Lease.Seconds())})
			beatDone()
			if errors.Is(err, ErrStaleLease) {
				log.Warn("the task's lease was lost, so its run stops", "error", err)
				cancel()
				return
			}
			if err != nil && runCtx.Err() == nil {
				log.Warn("extend a task's lease", "error", err)
			}
		}
	}()
	result := run(runCtx, h, task)
	if runCtx.Err() != nil && ctx.Err() == nil {
		return
	}
	if ctx.Err() != nil {
		// The worker is stopping, as a deploy stops it, and the stop likely cut
		// the run short. Such a run hands its task back, so the next worker
		// claims it at once. Its lease never lapses, and a lapse counts toward
		// failing a task whose runs end their worker.
		if result.Outcome != server.OutcomeDone {
			result = Result{Outcome: server.OutcomeRetry, Wait: true, Page: result.Page}
		}
		stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), stopReportTimeout)
		defer stop()
		ctx, runCtx = stopCtx, stopCtx
	}
	err := report(ctx, runCtx, client, cfg, log, task, server.ReportRequest{
		Token: task.LeaseToken, RetryAfterSeconds: int(result.RetryAfter.Seconds()), TaskResult: result,
	})
	switch {
	case errors.Is(err, ErrStaleLease):
		log.Warn("the service refused the report, because a later claim holds the task", "error", err)
	case err != nil:
		log.Error("a task's result is lost, so a later claim runs it again", "error", err)
	default:
		log.Info("reported a task", "outcome", result.Outcome, "outputs", len(result.Outputs), "arrivals", len(result.Arrivals),
			"metrics", len(result.Metrics))
	}
}

// report sends req, and sends it again with backoff while the service gives no
// answer, answers that it is busy or down, or reads no field req carries. The
// last is a service one release behind this worker, as while serve rolls. The
// heartbeat keeps the lease through the retries, and they stop once the lease
// is lost or held a whole lease with no answer. The service answers a repeat
// under the same token with the task as the first report ended it.
func report(ctx, leaseCtx context.Context, client *Client, cfg Config, log *slog.Logger, task Task, req server.ReportRequest) error {
	deadline := time.Now().Add(cfg.Lease)
	wait := time.Second
	for {
		err := client.Report(ctx, task.ID, req)
		if err == nil || !retryable(err) || time.Now().Add(wait).After(deadline) {
			return err
		}
		log.Warn("report a task, and try again", "error", err, "retry_in", wait.String())
		select {
		case <-leaseCtx.Done():
			return fmt.Errorf("the lease was lost while the report waited: %w", err)
		case <-time.After(wait):
		}
		wait = min(2*wait, 30*time.Second)
	}
}

// retryable says whether a call that failed with err may land if sent again.
func retryable(err error) bool {
	var refused *StatusError
	if !errors.As(err, &refused) {
		return !errors.Is(err, ErrStaleLease) && !errors.Is(err, context.Canceled)
	}
	switch {
	case refused.Status == http.StatusTooManyRequests, refused.Status >= 500:
		return true
	case refused.Status == http.StatusUnprocessableEntity && refused.Code == server.CodeUnknownField:
		return true
	}
	return false
}

// run is h's result for task, and a failure where h panics.
func run(ctx context.Context, h Handler, task Task) (result Result) {
	defer func() {
		if p := recover(); p != nil {
			reason := fmt.Sprintf("the %s handler panicked: %v", task.Capability, p)
			result = Result{Outcome: server.OutcomeFailed, Error: &reason}
		}
	}()
	return h.Run(ctx, task)
}

// deref is *s, and "" for nil.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
