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
	"net/http"
	"sync"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Task is one leased task.
type Task = server.LeasedTask

// Result is how a handler's run ended.
type Result struct {
	// Outcome is server.OutcomeDone, OutcomeFailed or OutcomeRetry.
	Outcome    string
	Error      *string
	RetryAfter time.Duration
	// Outputs are the stage's records for the task's item, and Delivery is how
	// a deliver task's write ended. The service writes both in the
	// transaction that ends the lease.
	Outputs  []server.ReportOutput
	Delivery *server.ReportDelivery
}

// Handler runs the tasks of one capability.
type Handler interface {
	Capability() string
	// Run runs task. ctx ends where the lease is lost, and the result is then
	// refused.
	Run(ctx context.Context, task Task) Result
}

// Config is how a worker runs.
type Config struct {
	// ID names the worker in every claim, and in the lease of each task it
	// holds.
	ID      string
	Version string
	// Slots is how many tasks run at once.
	Slots int
	// SlotLimits caps how many tasks of a capability run at once, within
	// Slots, such as one slot for a long parse so that the rest stay free for
	// every other stage. A capability that it does not name may take every
	// slot.
	SlotLimits map[string]int
	Lease      time.Duration
	// Poll is how often an idle worker claims again.
	Poll time.Duration
	Log  *slog.Logger
}

// Run claims and runs tasks for handlers until ctx ends. A worker with no
// handler claims nothing, and its claims still record its heartbeat. Each
// claim asks for no more of a capability than its slot limit leaves free. Run
// refuses a slot limit under 1.
func Run(ctx context.Context, client *Client, cfg Config, handlers ...Handler) error {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	for capability, limit := range cfg.SlotLimits {
		if limit < 1 {
			return fmt.Errorf("the slot limit of %s is %d, and a limit is 1 or more", capability, limit)
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
	limits := map[string]int{}
	for capability, limit := range cfg.SlotLimits {
		if _, held := byCapability[capability]; !held {
			cfg.Log.Warn("a slot limit names a capability that no handler here holds", "capability", capability)
			continue
		}
		limits[capability] = limit
	}
	started := store.Stamp(time.Now())
	cfg.Log.Info("the worker runs", "worker", cfg.ID, "capabilities", capabilities, "slots", cfg.Slots, "slot_limits", limits)

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
		var bounds map[string]int
		if len(limits) > 0 {
			bounds = map[string]int{}
			for capability, limit := range limits {
				bounds[capability] = max(limit-running[capability], 0)
			}
		}
		answer, err := client.Claim(ctx, server.ClaimRequest{
			WorkerID: cfg.ID, Capabilities: capabilities, Version: cfg.Version, StartedTS: started,
			Max: free, MaxByCapability: bounds, LeaseSeconds: int(cfg.Lease.Seconds()),
		})
		if err != nil && ctx.Err() == nil {
			cfg.Log.Warn("claim tasks", "error", err)
		}
		for _, task := range answer.Tasks {
			h := byCapability[task.Capability]
			busy++
			running[task.Capability]++
			wg.Go(func() {
				defer func() { freed <- task.Capability }()
				runTask(ctx, client, cfg, h, task)
			})
		}
		select {
		case <-ctx.Done():
			return nil
		case capability := <-freed:
			busy--
			running[capability]--
		case <-ticker.C:
		}
	drained:
		for {
			select {
			case capability := <-freed:
				busy--
				running[capability]--
			default:
				break drained
			}
		}
	}
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
	err := report(ctx, runCtx, client, cfg, log, task, server.ReportRequest{
		Token: task.LeaseToken, Outcome: result.Outcome, Error: result.Error,
		RetryAfterSeconds: int(result.RetryAfter.Seconds()), Outputs: result.Outputs, Delivery: result.Delivery,
	})
	switch {
	case errors.Is(err, ErrStaleLease):
		log.Warn("the service refused the report, because a later claim holds the task", "error", err)
	case err != nil:
		log.Error("a task's result is lost, so a later claim runs it again", "error", err)
	default:
		log.Info("reported a task", "outcome", result.Outcome, "outputs", len(result.Outputs))
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
