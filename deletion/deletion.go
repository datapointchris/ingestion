// Package deletion is the delete stage. A poll connector that declares
// after_landing delete has serve queue a delete task in the write that lands
// each item, so the source lets go of what ingestion now holds, as a message
// queue's consumer acknowledges a message once it is stored. A source that
// removes items names its deleter in its one registration with the sources
// package, so a set may declare after_landing delete on its connectors, and a
// worker opens the deleter from the source's [source.<name>] table. A worker
// runs the deleter through Handler. A failed delete tries again with backoff,
// and one that the source refused for good fails.
package deletion

import (
	"context"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Sighting is one connector's sighting of a landed item, which a deleter
// removes from that connector's source.
type Sighting struct {
	// ItemKey is the landed item's key.
	ItemKey string
	// Connector is the connector that sighted the item at the source.
	Connector string
	// ExternalKey is the source's own key for the item, such as a playlist
	// item's id, where the poll gave one, and the item's key otherwise.
	ExternalKey string
	// Fields are the values read from the source's payload.
	Fields map[string]string
	URL    *string
}

// Outcome is how one delete ended.
type Outcome struct {
	// Error is why the source still holds the item, and nil where it holds
	// none. A delete is idempotent, as HTTP's DELETE is, so a deleter answers
	// nil for an item that its source does not hold.
	Error *string
	// RetryAfter is how long the source asked the next try to wait, such as
	// until its day's quota resets. Zero waits the stage's own backoff.
	RetryAfter time.Duration
	// Final says that the source refused the delete in a way that no wait
	// clears, such as a write to a playlist that its account cannot edit. The
	// task then fails, and no later poll queues another delete of the item
	// until a person retries it.
	Final bool
}

// Deleter removes landed items from one source.
type Deleter interface {
	Delete(ctx context.Context, s Sighting) Outcome
}

// A failed delete tries again retryFirst after its first attempt, doubling
// each attempt up to retryMost.
const (
	retryFirst = 30 * time.Second
	retryMost  = time.Hour
)

// Handler is the worker handler that runs source's delete tasks through d.
func Handler(source string, d Deleter) worker.Handler { return handler{source: source, deleter: d} }

type handler struct {
	source  string
	deleter Deleter
}

func (h handler) Capability() string { return store.DeleteCapability(h.source) }

// Run removes the task's item from its sighting's source. A delete that
// leaves the item at the source tries again, after the wait the source asked
// for or the stage's backoff. One that the source refused for good fails.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.ItemKey == nil || task.Sighting == nil {
		reason := "a delete task names its item and the sighting it removes"
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	o := h.deleter.Delete(ctx, Sighting{
		ItemKey: *task.ItemKey, Connector: task.Sighting.Connector, ExternalKey: task.Sighting.ExternalKey,
		Fields: task.Sighting.Fields, URL: task.URL,
	})
	switch {
	case o.Error == nil:
		return worker.Result{Outcome: server.OutcomeDone}
	case o.Final:
		return worker.Result{Outcome: server.OutcomeFailed, Error: o.Error}
	}
	wait := o.RetryAfter
	if wait <= 0 {
		wait = backoff(task.Attempts)
	}
	return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: wait}
}

// backoff is how long a failed delete waits after its attempt-th claim.
func backoff(attempts int) time.Duration {
	wait := retryFirst
	for i := 1; i < attempts && wait < retryMost; i++ {
		wait *= 2
	}
	return min(wait, retryMost)
}
