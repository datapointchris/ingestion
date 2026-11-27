// Package metricsrefresh reads again, on a daily schedule, the metrics of each
// item that waits in review, such as a video's views, at the source whose
// connectors sighted it. A poll cannot carry the read. A capture's save has
// left its playlist once it lands, so no poll reports it again. And a poll's
// arrival lands through the connector, which queues a delete for every
// arrival of a connector that deletes after landing.
//
// So serve's Scheduler queues one refresh-metrics task a day for each source
// whose connectors sighted an item still in review. The claim hands the task
// the key of each such item as they stand then, so an item placed out of
// review in between is not read. A worker holding the source's refresher
// reads them all in one task, in as few requests as the source allows. serve
// writes each item's metrics onto the source's sightings of it in the write
// that ends the lease, and stamps when they were read. It changes nothing
// else, so a refresh lands nothing, deletes nothing and gates nothing. Where
// a request fails partway, serve still writes what the refresh read, and a
// retry's claim hands it only the items that no read took since the task was
// queued. A source names its refresher in its one registration with the
// sources package.
package metricsrefresh

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Outcome is how one refresh ended.
type Outcome struct {
	// Metrics are each item's metrics as its source reports them now, by the
	// item's key, whether or not the refresh ended done. An item that the
	// source leaves out of its answer, such as a deleted video, keeps the
	// metrics it has.
	Metrics map[string]map[string]int64
	// Error is why the refresh stopped before it read every item, and nil
	// where it ended done.
	Error *string
	// Wait is how long the refresh waits before it reads again, spending no
	// attempt. The source asked for it, as while it is busy or until its
	// day's quota resets, unless NoAnswer is set.
	Wait time.Duration
	// NoAnswer says that the source gave no answer, as while it restarts, so
	// the refresh waits for one.
	NoAnswer bool
	// Final says that the source refused the read in a way that no later
	// read clears, so the refresh fails, and the next scheduled one reads
	// again.
	Final bool
	// Counts are what the refresh passed over, by reason, such as an item
	// that the source left out of its answer. The handler logs them.
	Counts map[string]int
}

// Refresher reads the metrics of items at one source.
type Refresher interface {
	Refresh(ctx context.Context, keys []string) Outcome
}

// A read that failed in a way a later read may not clear tries again after
// retryAfter, until its task's claims reach Attempts, then fails. The next
// scheduled refresh reads again.
const (
	Attempts   = 4
	retryAfter = 5 * time.Minute
)

// Handler is the worker handler that runs source's refresh-metrics tasks
// through r.
func Handler(source string, r Refresher, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	return handler{source: source, refresher: r, log: log}
}

type handler struct {
	source    string
	refresher Refresher
	log       *slog.Logger
}

func (h handler) Capability() string { return store.RefreshMetricsCapability(h.source) }

// Run reads the metrics of the task's items at its source, and reports what
// it read however it ends. A read that the source asked to wait, or that got
// no answer, queues again after that wait, spending no claim. One that failed
// otherwise queues again until the task's claims reach Attempts, and one that
// the source refused for good fails.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if len(task.Keys) == 0 {
		return worker.Result{Outcome: server.OutcomeDone}
	}
	o := h.refresher.Refresh(ctx, task.Keys)
	attrs := []any{"task", task.ID, "source", h.source, "keys", len(task.Keys), "read", len(o.Metrics)}
	for _, reason := range slices.Sorted(maps.Keys(o.Counts)) {
		attrs = append(attrs, reason, o.Counts[reason])
	}
	metrics := make([]server.ReportMetrics, 0, len(o.Metrics))
	for _, key := range slices.Sorted(maps.Keys(o.Metrics)) {
		metrics = append(metrics, server.ReportMetrics{Key: key, Metrics: o.Metrics[key]})
	}
	switch {
	case o.Error == nil:
		h.log.Info("a refresh read its items' metrics again", attrs...)
		return worker.Result{Outcome: server.OutcomeDone, Metrics: metrics}
	case o.Wait > 0 && o.NoAnswer:
		h.log.Warn("a refresh got no answer from its source, and reads again", append(attrs, "error", *o.Error, "retry_after", o.Wait.String())...)
		return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: o.Wait, Wait: true, Metrics: metrics}
	case o.Wait > 0:
		h.log.Info("a refresh's source asked it to wait", append(attrs, "error", *o.Error, "retry_after", o.Wait.String())...)
		return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: o.Wait, Wait: true, Metrics: metrics}
	case o.Final || task.Attempts >= Attempts:
		h.log.Warn("a refresh failed", append(attrs, "error", *o.Error)...)
		return worker.Result{Outcome: server.OutcomeFailed, Error: o.Error, Metrics: metrics}
	}
	h.log.Warn("a refresh's read failed, and it reads again", append(attrs, "error", *o.Error)...)
	return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: retryAfter, Metrics: metrics}
}
