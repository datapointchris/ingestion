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

// A worker whose slot limit for parse:docling is 1 never runs two Docling
// parses at once, and leases another capability's task on its second slot
// while one runs, though the Docling tasks are older.
func TestASlotLimitLeavesTheWorkersOtherSlotsToOtherCapabilities(t *testing.T) {
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
	docling := fakeHandler{"parse:docling", func() {
		n := running.Add(1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		select {
		case <-scanned:
		case <-time.After(5 * time.Second):
		}
		running.Add(-1)
	}}
	scanner := fakeHandler{scan.Capability, func() {
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
			ID: "slots", Version: "test", Slots: 2, SlotLimits: map[string]int{docling.capability: 1},
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

// fakeHandler runs do for each task of capability, and fails the task.
type fakeHandler struct {
	capability string
	do         func()
}

func (h fakeHandler) Capability() string { return h.capability }

func (h fakeHandler) Run(context.Context, worker.Task) worker.Result {
	h.do()
	reason := "a fake handler"
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
}
