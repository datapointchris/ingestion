// Package deliver is the deliver stage. Each deliverer writes items to one
// kind of destination through that destination's own API, never its store,
// and names each record by the item's key. So one artifact reaches a
// destination once, however many connectors sighted it. This is reverse ETL,
// as Hightouch and Census sync a warehouse's rows into each tool's API.
//
// A deliverer's file calls Register from its own init, with the deliverer's
// name, what it takes and how to open it, so a destination's declaration may
// name it and a worker's config may open it. The service queues a deliver task
// where the read gate reads an item as is, and records the delivery pending
// with the title, URL and file path that every attempt sends. A worker runs each
// deliverer that its config opens through Handler, and claims only those
// deliverers' tasks. The service then records how each write ended, and never
// queues a settled delivery again, because a destination may forget a record
// it held and save the item twice. Each write runs in the item's flow, so its
// request carries the item's trace in traceparent and its key in baggage.
package deliver

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemflow"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Registration is one deliverer as this build carries it.
type Registration struct {
	// Name is what a destination's deliverer names, the suffix of its tasks'
	// capability, and its table's name under [deliver] in a worker's config.
	Name string
	// Scopes is what the deliverer's service client asks its token for.
	Scopes []string
	// Takes answers why the deliverer cannot write an item, and nil where it
	// can. The service asks it before queueing, so an item it cannot take
	// waits with that reason rather than failing at the destination.
	Takes func(store.Item) error
	// FilePath says the deliverer writes an item by its file. It takes only
	// an item with a live home below the root that serve's [deliver.<name>]
	// table names, and each attempt sends that home's path as the delivery
	// first kept it.
	FilePath bool
	// Chapters says the deliverer writes a book's chapter as part of its
	// book's record. Its chapter's delivery waits for the book's to settle,
	// and each attempt sends that record's id and the chapter's pages as the
	// delivery first kept them. Takes reads no chapter.
	Chapters bool
	// Playlist says the deliverer adds each item to the playlist that a
	// person's sort picked, which each attempt sends as the delivery first
	// kept it. Only a sort places an item on its destination.
	Playlist bool
	// Open is the deliverer that writes to the app at api through client,
	// which carries the service client's token.
	Open func(api string, client *http.Client) Deliverer
}

var registered = map[string]Registration{}

// Register records r, and records its name and what it writes with
// connectors.RegisterDeliverer, so each name a set may give is one a worker
// can open.
func Register(r Registration) {
	connectors.RegisterDeliverer(r.Name, r.Writes())
	registered[r.Name] = r
}

// Writes is what r writes, as connectors.RegisterDeliverer records it.
func (r Registration) Writes() connectors.Writes {
	return connectors.Writes{Takes: r.Takes, FilePath: r.FilePath, Chapters: r.Chapters, Playlist: r.Playlist}
}

// Lookup is the deliverer registered as name, and false where none is.
func Lookup(name string) (Registration, bool) {
	r, ok := registered[name]
	return r, ok
}

// Names is the name of each deliverer this build registers, in order.
func Names() []string {
	names := make([]string, 0, len(registered))
	for name := range registered {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

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
	// SourceTS is when the item was made at its source, such as a Reddit
	// post's creation, as a store stamp, and nil where its source gave none.
	SourceTS *string
	// Path is the item's file below its deliverer's root, as the delivery
	// first kept it, and nil for a deliverer that writes no file.
	Path *string
	// Chapter is where a book's chapter sits in its book's record, as the
	// delivery first kept it, and nil for every other item.
	Chapter *store.ChapterInBook
	// Playlist is the playlist that a watch adds the video to, as the
	// delivery first kept it, and nil for every other deliverer.
	Playlist *string
	// Attempt counts this task's claims that ended with no answer, this one
	// included. A claim that the destination asked to wait is not counted, so
	// an attempt past the first follows one whose write may have landed
	// unanswered.
	Attempt int
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
	// RetryAfter is the wait that the destination asked a pending write for,
	// as while a quota is spent. Its task tries again after it and spends no
	// attempt, and a zero tries again with backoff.
	RetryAfter time.Duration
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
	o := h.deliverer.Deliver(itemflow.Join(ctx, task.FlowID, *task.ItemKey), Item{
		Key: *task.ItemKey, Zone: task.Zone, Title: task.Title, URL: task.URL, LandedTS: task.LandedTS, SourceTS: task.SourceTS,
		Path: task.Path, Chapter: task.Chapter, Playlist: task.Playlist, Attempt: task.Attempts,
	})
	delivery := &server.ReportDelivery{Outcome: o.Outcome, DestinationID: o.DestinationID, Error: o.Error}
	switch {
	case o.Outcome != store.DeliveryPending:
		return worker.Result{Outcome: server.OutcomeDone, Delivery: delivery}
	case o.Permanent:
		return worker.Result{Outcome: server.OutcomeFailed, Error: o.Error, Delivery: delivery}
	case o.RetryAfter > 0:
		return worker.Result{Outcome: server.OutcomeRetry, Error: o.Error, RetryAfter: o.RetryAfter, Wait: true, Delivery: delivery}
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
