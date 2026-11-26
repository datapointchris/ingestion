// Package deletion is the delete stage. A poll connector that declares
// after_landing delete has serve queue a delete task in the write that lands
// each item, so the source lets go of what ingestion now holds, as a message
// queue's consumer acknowledges a message once it is stored. A source that
// removes items registers its deleter from its own init, so a set may declare
// after_landing delete on its connectors, and a worker opens the deleter from
// its [source.<name>] table. A worker runs the deleter through Handler, and a
// failed delete tries again with backoff.
package deletion

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Registration is one source's deleter as this build carries it.
type Registration struct {
	// Source names where the deleter removes items. It is the suffix of the
	// delete tasks' capability, and the table's name under [source] in a
	// worker's config.
	Source string
	// Scopes is what the source's service client asks its token for.
	Scopes []string
	// Open is the deleter that reaches the source at api through client,
	// which carries the service client's token.
	Open func(api string, client *http.Client) Deleter
}

var registered = map[string]Registration{}

// Register records r, and records its source with
// connectors.RegisterDeleter, so each source whose connectors a set lets
// delete is one whose deleter a worker can open.
func Register(r Registration) {
	connectors.RegisterDeleter(r.Source)
	registered[r.Source] = r
}

// Lookup is the deleter registered for source, and false where none is.
func Lookup(source string) (Registration, bool) {
	r, ok := registered[source]
	return r, ok
}

// Sources names, in order, each source whose deleter this build registers.
func Sources() []string {
	sources := make([]string, 0, len(registered))
	for source := range registered {
		sources = append(sources, source)
	}
	slices.Sort(sources)
	return sources
}

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
// for or the stage's backoff.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.ItemKey == nil || task.Sighting == nil {
		reason := "a delete task names its item and the sighting it removes"
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	o := h.deleter.Delete(ctx, Sighting{
		ItemKey: *task.ItemKey, Connector: task.Sighting.Connector, ExternalKey: task.Sighting.ExternalKey,
		Fields: task.Sighting.Fields, URL: task.URL,
	})
	if o.Error == nil {
		return worker.Result{Outcome: server.OutcomeDone}
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
