package cmd

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// A worker on the default config, with no [worker.slot_limits], never runs two
// Docling parses at once, because the extractor holds one slot. It leases
// another capability's task on its second slot while one runs, though the
// Docling tasks are older.
func TestADoclingParseHoldsOneSlotWhereNoSlotLimitIsConfigured(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := newPipeline(t, extract.Builtin(), func(context.Context, *store.Tx, store.Task) error { return nil })
	p.apply(t, mustAbs(t, "../examples/declarations"))
	var books []string
	for _, body := range []string{"one", "two", "three"} {
		landed, err := p.lander.Land(ctx, landing.Arrival{
			Connector: "books-folder", Zone: store.Outside, MediaType: extract.MediaTypePDF, Body: strings.NewReader(body),
		})
		if err != nil {
			t.Fatal(err)
		}
		books = append(books, landed.Key)
	}
	if err := p.st.Update(ctx, func(tx *store.Tx) error {
		for _, stage := range []struct{ name, capability string }{{store.StageParse, "parse:docling"}, {store.StageScan, scan.Capability}} {
			for _, key := range books {
				if _, err := tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: stage.name, Capability: stage.capability}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var running, most atomic.Int32
	var besideDocling atomic.Bool
	scanned := make(chan struct{})
	var scannedOnce sync.Once
	docling := fakeHandler{capability: extract.Capability(extract.PDFDocling), slotLimit: extract.PDFDocling.SlotLimit, do: func() {
		n := running.Add(1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		select {
		case <-scanned:
		case <-time.After(5 * time.Second):
		}
		running.Add(-1)
	}}
	scanner := fakeHandler{capability: scan.Capability, do: func() {
		for wait := time.Now().Add(5 * time.Second); running.Load() == 0 && time.Now().Before(wait); {
			time.Sleep(5 * time.Millisecond)
		}
		if running.Load() > 0 {
			besideDocling.Store(true)
		}
		scannedOnce.Do(func() { close(scanned) })
	}}
	ran := make(chan error, 1)
	go func() {
		ran <- worker.Run(ctx, p.worker, worker.Config{
			ID: "slots", Version: "test", Slots: 2,
			Lease: time.Minute, Poll: 20 * time.Millisecond, Log: quiet,
		}, docling, scanner)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for p.count(t, `SELECT count(*) FROM tasks WHERE state = 'failed'`) < 6 {
		if time.Now().After(deadline) {
			t.Fatal("the worker did not run its six tasks")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	if most.Load() != 1 {
		t.Errorf("%d Docling parses ran at once, want 1", most.Load())
	}
	if !besideDocling.Load() {
		t.Error("no scan ran while a Docling parse held its slot")
	}
}

// A deploy that stops the worker during a long parse queues its task again at
// once, with its lease handed back rather than left to lapse, so the stop
// spends none of the lapses that fail a task.
func TestADeployThatStopsTheWorkerMidParseQueuesItsTaskAgain(t *testing.T) {
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	p := newPipeline(t, extract.Builtin(), func(context.Context, *store.Tx, store.Task) error { return nil })
	p.apply(t, mustAbs(t, "../examples/declarations"))
	landed, err := p.lander.Land(ctx, landing.Arrival{
		Connector: "books-folder", Zone: store.Outside, MediaType: extract.MediaTypePDF, Body: strings.NewReader("a book"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.st.Update(ctx, func(tx *store.Tx) error {
		_, err := tx.Enqueue(ctx, store.Task{ItemKey: &landed.Key, Stage: store.StageParse, Capability: extract.Capability(extract.PDFDocling)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	docling := fakeHandler{capability: extract.Capability(extract.PDFDocling), do: func() {
		close(started)
		<-ctx.Done()
	}}
	ran := make(chan error, 1)
	go func() {
		ran <- worker.Run(ctx, p.worker, worker.Config{
			ID: "deployed", Version: "test", Slots: 1, Lease: time.Hour,
			Poll: 20 * time.Millisecond, Log: quiet,
		}, docling)
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker did not start the parse")
	}
	stop()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND capability = ? AND state = 'queued' AND error IS NULL`,
		landed.Key, docling.capability); n != 1 {
		t.Errorf("%d of the book's Docling parses are queued once the worker stopped, want 1", n)
	}
}

// fakeHandler runs do for each task of capability, and fails the task.
type fakeHandler struct {
	capability string
	slotLimit  int
	do         func()
}

func (h fakeHandler) Capability() string { return h.capability }

func (h fakeHandler) SlotLimit() int { return h.slotLimit }

func (h fakeHandler) Run(context.Context, worker.Task) worker.Result {
	h.do()
	reason := "a fake handler"
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
}
