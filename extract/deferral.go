package extract

import (
	"context"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// AfterParse runs in the write that ends a parse task done, and routes on the
// output that the task's extractor wrote: written runs where the parse holds
// text, and deferred where it handed the item to another extractor. A task
// whose extractor this build does not name runs written.
func AfterParse(written, deferred []server.AfterStage) server.AfterStage {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		hooks := written
		if task.ItemKey != nil {
			wroteDeferred, err := deferredBy(ctx, tx, task)
			if err != nil {
				return err
			}
			if wroteDeferred {
				hooks = deferred
			}
		}
		for _, hook := range hooks {
			if err := hook(ctx, tx, task); err != nil {
				return err
			}
		}
		return nil
	}
}

// deferredBy says whether task's extractor holds its parse of the task's item
// deferred, at the extractor's version.
func deferredBy(ctx context.Context, tx *store.Tx, task store.Task) (bool, error) {
	e, ok := Named(strings.TrimPrefix(task.Capability, store.StageParse+":"))
	if !ok {
		return false, nil
	}
	outputs, err := tx.Outputs(ctx, *task.ItemKey)
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.Producer == e.Name && o.ProducerVersion == e.Version && o.State == store.OutputDeferred
	}), nil
}

// QueueDeferredParse runs where AfterParse finds a parse deferred. It queues a
// parse by the extractor that the task's extractor's DefersTo names, unless
// one is queued or leased, or a done one at its version is held. So a parse
// replay over a deferred item queues the other extractor's parse again only
// where it has not read the item at its version, and an item whose other
// parse failed is read again.
func QueueDeferredParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	e, ok := Named(strings.TrimPrefix(task.Capability, store.StageParse+":"))
	if !ok || e.DefersTo == "" {
		return nil
	}
	next, ok := Named(e.DefersTo)
	if !ok {
		return nil
	}
	r, err := tx.Records(ctx, *task.ItemKey)
	if err != nil {
		return err
	}
	capability := Capability(next)
	if slices.ContainsFunc(r.Tasks, func(t store.Task) bool {
		return t.Capability == capability && (t.State == store.TaskQueued || t.State == store.TaskLeased)
	}) || slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.State == store.OutputDone && o.Producer == next.Name && o.ProducerVersion == next.Version
	}) {
		return nil
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageParse, Capability: capability})
	return err
}
