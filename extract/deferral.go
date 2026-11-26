package extract

import (
	"context"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/store"
)

// QueueDeferredParse runs in the write that ends a parse task done. Where the
// task's extractor wrote its parse deferred, it queues a parse by the
// extractor that DefersTo names, unless one is queued or leased, or a done one
// at its version is held. So a parse replay over a deferred item queues the
// other extractor's parse again only where it has not read the item at its
// version, and an item whose other parse failed is read again.
func QueueDeferredParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
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
	if !slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.Producer == e.Name && o.ProducerVersion == e.Version && o.State == store.OutputDeferred
	}) {
		return nil
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
