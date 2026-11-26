// Package polling is the worker's side of the poll stage. The scheduler queues
// a poll task each time a poll connector falls due, and a worker that holds
// the source's poller claims it. The poller reads the source past the
// connector's cursor and answers what it found, which serve lands in the write
// that ends the lease. A source registers its poller from its own init, and a
// worker opens it from the source's [source.<name>] table.
package polling

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Registration is one source's poller as this build carries it.
type Registration struct {
	// Source names the source that the poller reads. It is the suffix of the
	// poll tasks' capability, and the table's name under [source] in a
	// worker's config.
	Source string
	// Scopes is what the source's service client asks its token for.
	Scopes []string
	// Open is the poller that reaches the source at api through client, which
	// carries the service client's token.
	Open func(api string, client *http.Client) Poller
}

var registered = map[string]Registration{}

// Register records r. Registering a source twice panics, as a duplicate is a
// build error.
func Register(r Registration) {
	if _, taken := registered[r.Source]; taken {
		panic("polling: the poller of source " + r.Source + " is registered twice")
	}
	registered[r.Source] = r
}

// Lookup is the poller registered for source, and false where none is.
func Lookup(source string) (Registration, bool) {
	r, ok := registered[source]
	return r, ok
}

// Sources names, in order, each source whose poller this build registers.
func Sources() []string { return slices.Sorted(maps.Keys(registered)) }

// Poll is one poll a poller runs: its connector as the set in force declares
// it, and the position an earlier poll left, nil before the first.
type Poll struct {
	Connector connectors.Connector
	Cursor    *string
}

// Outcome is how one poll ended.
type Outcome struct {
	// Arrivals are what the poll found, in the order they land. Cursor is the
	// connector's next position, and nil keeps the one it has.
	Arrivals []server.ReportArrival
	Cursor   *string
	// Error is why the poll read nothing, and nil where it ended done.
	Error *string
	// RetryAfter is how long the source asked the same poll to wait before it
	// tries again, such as until its day's quota resets. Zero fails the poll,
	// and the connector's backoff makes it due again.
	RetryAfter time.Duration
	// Counts are what the poll passed over, by reason, such as unavailable
	// videos. The handler logs them beside the task.
	Counts map[string]int
	// Warnings are what a person should know about a poll that ended done,
	// such as a read that stopped short of where the last one ended. The
	// handler logs each as a warning.
	Warnings []string
}

// Poller reads one source for its connectors.
type Poller interface {
	Poll(ctx context.Context, p Poll) Outcome
}

// Handler is the worker handler that runs source's poll tasks through p.
func Handler(source string, p Poller, log *slog.Logger) worker.Handler {
	return handler{source: source, poller: p, log: log}
}

type handler struct {
	source string
	poller Poller
	log    *slog.Logger
}

func (h handler) Capability() string { return store.PollCapability(h.source) }

// Run polls the task's connector as its declaration reads. A poll whose source
// asked it to wait goes back to the queue for that long, and any other error
// fails it.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.Connector == nil || len(task.Declaration) == 0 {
		reason := "the poll task carries no connector declaration, so the set in force no longer declares its connector"
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	var c connectors.Connector
	if err := json.Unmarshal(task.Declaration, &c); err != nil {
		reason := "read the connector's declaration: " + err.Error()
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	o := h.poller.Poll(ctx, Poll{Connector: c, Cursor: task.Cursor})
	attrs := []any{"task", task.ID, "connector", c.Name, "source", h.source, "arrivals", len(o.Arrivals), "cursor_moved", o.Cursor != nil}
	for _, reason := range slices.Sorted(maps.Keys(o.Counts)) {
		attrs = append(attrs, reason, o.Counts[reason])
	}
	for _, warning := range o.Warnings {
		h.log.Warn(warning, attrs...)
	}
	switch {
	case o.Error == nil:
		h.log.Info("a poll read its source", attrs...)
		return worker.Result{Outcome: server.OutcomeDone, Arrivals: o.Arrivals, Cursor: o.Cursor}
	case o.RetryAfter > 0:
		h.log.Warn("a poll's source asked it to wait", append(attrs, "error", *o.Error, "retry_after", o.RetryAfter.String())...)
		return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: o.RetryAfter}
	}
	h.log.Warn("a poll failed", append(attrs, "error", *o.Error)...)
	return worker.Result{Outcome: server.OutcomeFailed, Error: o.Error}
}
