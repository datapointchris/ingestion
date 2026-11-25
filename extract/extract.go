// Package extract is the parse stage: one extractor per media type, each
// turning an item's bytes into units, such as a thread's post and comments or
// a book's pages. A registry keyed by media type picks the extractor, so an
// item parses the same whatever source sent it.
//
// An extractor registers by joining builtin. One written in Go sets Extract.
// One whose library is not Go is a Program from the worker image, run through
// this package's one subprocess door. Each output is keyed by item, extractor
// and version, so a changed extractor takes a new version and its outputs
// write beside the old.
package extract

import (
	"context"
	"fmt"
	"io"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// builtin is every extractor that ingestion holds.
var builtin = []Extractor{ThreadText}

// Extractor turns the bytes of one media type into a parse's units.
type Extractor struct {
	// Name names the extractor in each output it writes, such as thread-text.
	Name string
	// Version is written beside each output, so a changed extractor takes a
	// new one.
	Version   string
	MediaType string
	// Program is the absolute path of the extractor's program, and empty for
	// one written in Go. A worker runs it only where the program is installed.
	Program string
	// Extract reads body and answers its units in order.
	Extract func(ctx context.Context, body io.Reader) ([]Unit, error)
}

// Unit is one part of a parse, as an extractor writes it. The parse numbers
// its units from 1 in their order.
type Unit struct {
	// Label is the unit's own heading, such as a comment's author or a
	// chapter's title.
	Label *string
	Text  string
}

// Capability is the capability of e's parse tasks, so a worker claims only
// the tasks of extractors it runs.
func Capability(e Extractor) string { return store.StageParse + ":" + e.Name }

// Registry picks the extractor for an item by its media type.
type Registry struct {
	byType map[string]Extractor
	all    []Extractor
}

// NewRegistry registers extractors. It refuses one with no name, version,
// media type or way to run, and two of one name or one media type.
func NewRegistry(extractors ...Extractor) (*Registry, error) {
	r := &Registry{byType: map[string]Extractor{}}
	names := map[string]bool{}
	for _, e := range extractors {
		switch {
		case e.Name == "" || e.Version == "" || e.MediaType == "":
			return nil, fmt.Errorf("an extractor names its name, version and media type, not %q, %q and %q", e.Name, e.Version, e.MediaType)
		case e.Extract == nil:
			return nil, fmt.Errorf("extractor %s sets no Extract", e.Name)
		case names[e.Name]:
			return nil, fmt.Errorf("two extractors are named %s", e.Name)
		}
		if held, taken := r.byType[e.MediaType]; taken {
			return nil, fmt.Errorf("extractors %s and %s both read %s", held.Name, e.Name, e.MediaType)
		}
		names[e.Name] = true
		r.byType[e.MediaType] = e
		r.all = append(r.all, e)
	}
	return r, nil
}

// Builtin is the registry of every extractor that ingestion holds.
func Builtin() *Registry {
	r, err := NewRegistry(builtin...)
	if err != nil {
		panic("extract: " + err.Error())
	}
	return r
}

// For is the extractor that reads mediaType, and false where none does.
func (r *Registry) For(mediaType string) (Extractor, bool) {
	e, ok := r.byType[mediaType]
	return e, ok
}

// QueueParse queues a parse task for item where an extractor reads its media
// type. Landing calls it in the write that adds the item, so every item lands
// with its parse queued.
func (r *Registry) QueueParse(ctx context.Context, tx *store.Tx, item store.Item) error {
	e, ok := r.For(item.MediaType)
	if !ok {
		return nil
	}
	_, err := tx.Enqueue(ctx, store.Task{ItemKey: item.Key, Stage: store.StageParse, Capability: Capability(e)})
	return err
}

// Replayable is the parse stage as a replay runs it again: a task for each
// item whose media type an extractor reads.
func (r *Registry) Replayable() server.Replayable {
	return server.Replayable{Capability: func(item store.Item) (string, bool) {
		e, ok := r.For(item.MediaType)
		return Capability(e), ok
	}}
}
