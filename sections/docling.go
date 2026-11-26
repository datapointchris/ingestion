package sections

import (
	"context"
	"slices"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/store"
)

// The font shares that select a book for extract.PDFDocling. Code is set in
// monospace, and Docling fences it where PyMuPDF's page text loses its
// layout. Formulas are set in TeX and math fonts, and Docling drops them, so a
// book with both stays pdf-text's alone.
const (
	DoclingMonospaceLeast = 0.20
	DoclingTeXOrMathUnder = 0.10
)

// CodeHeavyMathLight says whether monospace fonts reach DoclingMonospaceLeast
// of the sampled pages while TeX and math fonts stay under
// DoclingTeXOrMathUnder.
func (s FontShares) CodeHeavyMathLight() bool {
	return s.Monospace >= DoclingMonospaceLeast && s.TeXOrMath < DoclingTeXOrMathUnder
}

// QueueDoclingParse runs in the write that ends a PDF's structure task done.
// Where the newest profile is CodeHeavyMathLight, it queues a parse by
// extract.PDFDocling, unless one is queued or leased, or a done one at its
// version is held. So a structure replay queues each selected book that
// Docling has not read at its version, and a book whose Docling parse failed
// is read again.
func QueueDoclingParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
	r, err := tx.Records(ctx, *task.ItemKey)
	if err != nil {
		return err
	}
	profile, ok, err := NewestProfile(r.Outputs)
	if err != nil || !ok || !profile.FontShares.CodeHeavyMathLight() {
		return err
	}
	capability := extract.Capability(extract.PDFDocling)
	if slices.ContainsFunc(r.Tasks, func(t store.Task) bool {
		return t.Capability == capability && (t.State == store.TaskQueued || t.State == store.TaskLeased)
	}) || slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.State == store.OutputDone && o.Producer == extract.PDFDocling.Name &&
			o.ProducerVersion == extract.PDFDocling.Version
	}) {
		return nil
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageParse, Capability: capability})
	return err
}
