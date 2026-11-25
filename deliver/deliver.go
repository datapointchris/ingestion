// Package deliver is the deliver stage. Each deliverer writes items to one
// kind of destination through that destination's own API, never its store,
// and names each record by the item's key. So one artifact reaches a
// destination once, however many connectors sighted it. This is reverse ETL,
// as Hightouch and Census sync a warehouse's rows into each tool's API.
//
// A deliverer registers its name with connectors.RegisterDeliverer from this
// package's init, so a destination's declaration may name it. The service
// queues a deliver task where the read gate reads an item as is, and records
// the delivery pending. A worker runs each deliverer that its config sets up
// through Handler, and claims only those deliverers' tasks. The service then
// records how each write ended, and never queues a settled delivery again,
// because a destination may forget a record it held and save the item twice.
package deliver

import (
	"context"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Item is an item as its deliver task carries it.
type Item struct {
	// Key is the item's key, which a destination keeps as its record's
	// source key.
	Key   string
	Zone  store.Zone
	Title *string
	URL   *string
	// LandedTS is when the item first landed, as a store stamp.
	LandedTS string
}

// Outcome is how one write of an item ended.
type Outcome struct {
	// Outcome is store.DeliveryDelivered, DeliveryDeliveredToExisting,
	// DeliveryRefused or DeliveryPending.
	Outcome string
	// DestinationID is the destination's id for the record that holds the
	// item, or that a refusal names.
	DestinationID *string
	// Error is the destination's answer where the delivery is pending, or
	// what a refusal names.
	Error *string
	// Permanent marks a pending write that sending the same request again
	// cannot mend, as where the destination refuses the body as invalid. Its
	// task fails rather than trying again.
	Permanent bool
}

// Deliverer writes items to one kind of destination.
type Deliverer interface {
	// Deliver writes item under its key and answers how the write ended. A
	// destination that fails, times out or answers in part leaves the
	// delivery pending.
	Deliver(ctx context.Context, item Item) Outcome
}

// A pending delivery's task tries again retryFirst after its first attempt,
// doubling each attempt up to retryMost, so a destination that is down for a
// day is asked about once an hour.
const (
	retryFirst = 30 * time.Second
	retryMost  = time.Hour
)

// Handler is the worker handler that runs the deliver tasks of the deliverer
// registered as name through d.
func Handler(name string, d Deliverer) worker.Handler { return handler{name: name, deliverer: d} }

type handler struct {
	name      string
	deliverer Deliverer
}

func (h handler) Capability() string { return store.DeliverCapability(h.name) }

// Run writes the task's item and reports how the write ended. A settled
// outcome ends the task done. A pending one tries again with backoff, or fails
// the task where the destination refused the request itself.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.ItemKey == nil || task.Destination == nil {
		reason := "a deliver task names its item and its destination"
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	o := h.deliverer.Deliver(ctx, Item{Key: *task.ItemKey, Zone: task.Zone, Title: task.Title, URL: task.URL, LandedTS: task.LandedTS})
	delivery := &server.ReportDelivery{Outcome: o.Outcome, DestinationID: o.DestinationID, Error: o.Error}
	switch {
	case o.Outcome != store.DeliveryPending:
		return worker.Result{Outcome: server.OutcomeDone, Delivery: delivery}
	case o.Permanent:
		return worker.Result{Outcome: server.OutcomeFailed, Error: o.Error, Delivery: delivery}
	}
	return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: backoff(task.Attempts), Delivery: delivery}
}

// backoff is how long a pending delivery waits after its attempt-th claim.
func backoff(attempts int) time.Duration {
	wait := retryFirst
	for i := 1; i < attempts && wait < retryMost; i++ {
		wait *= 2
	}
	return min(wait, retryMost)
}
