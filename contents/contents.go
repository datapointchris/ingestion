// Package contents is the model stage that reads a book's printed table of
// contents, for a PDF of sections.WholeFilePages pages or more whose outline
// gives it no sections. Its prompt and answer schema are prompts.Contents.
//
// The service queues a book's contents task in the write that ends its
// structure task, where the outline rule wrote the book no sections. A
// connector's first fill holds every book after the first to reach the stage,
// as at every model stage. A worker holding a model port runs the task under
// modelstage's dispatch-once protocol. Its prompt quotes the front pages that
// look like a contents page, or the first frontPages pages where none does.
// The model answers each entry's title, printed page and level.
//
// When the task ends done, serve places each entry on a PDF page, by the
// pdf-text parse's page label, else by the book's measured offset, then by a
// search for the title near that page. The entries then pass the outline rule
// through sections.FromEntries, and sections.Write writes them under producer
// contents with a chapter item per chapter, as an outline's are.
package contents

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	// Capability is the capability of every contents task.
	Capability = store.StageContents
	// Producer names the contents stage's outputs, whose version is the
	// prompt's.
	Producer = "contents-reader"
	// PlacementVersion names the placement, so a changed placement writes
	// its sections beside the old.
	PlacementVersion = "1"
)

// Answer is the model's answer, as prompts/contents/answer.schema.json holds
// it. ReadAnswer decodes it.
type Answer struct {
	Entries []AnswerEntry `json:"entries"`
}

// AnswerEntry is one entry of a printed table of contents.
type AnswerEntry struct {
	Title string `json:"title"`
	// Page is the page printed beside the entry, and nil where none is.
	Page  *string `json:"page"`
	Level int     `json:"level"`
}

// ReadAnswer checks raw against the answer's schema and decodes it.
func ReadAnswer(raw json.RawMessage) (Answer, error) {
	if err := prompts.Contents.Check(raw); err != nil {
		return Answer{}, err
	}
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the contents answer: %w", err)
	}
	return a, nil
}

// Version is the producer version of the sections that the placement writes
// from answer and parse, each of which is part of it, with the outline rule
// that kinds them.
func Version(answer, parse store.Output) string {
	return PlacementVersion + "/" + sections.RuleVersion + "/" + answer.Producer + "@" + answer.ProducerVersion + "/" +
		parse.Producer + "@" + parse.ProducerVersion
}

// QueueAfterStructure queues the book's contents task in the write that ends
// its structure task done, where the outline rule wrote it no sections and
// the book never held a contents task. A later structure queues none. The
// stage runs again only through a replay that a person confirms, or through
// `tasks retry` on a failed task.
func QueueAfterStructure(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
	key := *task.ItemKey
	r, err := records(ctx, tx, key)
	if err != nil || !waitsForContents(r) {
		return err
	}
	if held, err := tx.HasTask(ctx, key, store.StageContents); err != nil || held {
		return err
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageContents, Capability: Capability})
	return err
}

// AfterAnswer runs in the write that ends a book's contents task done. It
// places the entries of the newest done answer on the book's pdf-text parse,
// and writes the sections they make through sections.Write. An answer of no
// entries, or of entries that make no usable outline, writes none.
func AfterAnswer(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
	key := *task.ItemKey
	item, ok, err := tx.Item(ctx, key)
	if err != nil || !ok {
		return err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return err
	}
	newest := store.NewestDone(outputs, store.StageContents)
	if newest == nil {
		return nil
	}
	answer, err := ReadAnswer(newest.Answer)
	if err != nil {
		return fmt.Errorf("read the contents answer on %s: %w", key, err)
	}
	profile, _, err := sections.NewestProfile(outputs)
	if err != nil {
		return err
	}
	parse, err := tx.Parse(ctx, key, store.ParsePick{Extractor: sections.ParsedBy.Name})
	if err != nil {
		return fmt.Errorf("read the parse that %s's contents are placed on: %w", key, err)
	}
	book := sections.Book{Pages: len(parse.Units), Titles: []string{deref(item.Title), deref(profile.Title)}, Units: parse.Units}
	rows := sections.FromEntries(Place(answer.Entries, parse.Units), book, store.SectionsFromContents, Version(*newest, parse.Output))
	return sections.Write(ctx, tx, item, rows)
}

// waitsForContents says whether a book waits for its printed contents: its
// newest profile counts sections.WholeFilePages pages or more, its pdf-text
// parse is done, and its outline gave it no sections.
func waitsForContents(r store.ItemRecords) bool {
	profile, ok, err := sections.NewestProfile(r.Outputs)
	if err != nil || !ok || profile.Pages < sections.WholeFilePages {
		return false
	}
	parsed := slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.Producer == sections.ParsedBy.Name && o.State == store.OutputDone
	})
	outlined := slices.ContainsFunc(r.Sections, func(s store.Section) bool { return s.Producer == store.SectionsFromOutline })
	return parsed && !outlined
}

// due says whether a replay runs the stage on a book: the book waits for its
// contents, no contents task of it is open, and it holds no done answer of
// the prompt in force.
func due(r store.ItemRecords, open bool) bool {
	return !open && waitsForContents(r) && !slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageContents && o.Producer == Producer && o.ProducerVersion == prompts.Contents.Version &&
			o.State == store.OutputDone
	})
}

// Replayable is the stage as a replay runs it, such as with a new prompt,
// over each book that waits for its contents. The replay first answers how
// many books it would run on and the median tokens by kind of the stage's
// newest stored runs, times that count, and queues only once a person
// confirms. A connector's first fill holds each task it queues as it holds
// the stage's own.
func Replayable(st *store.Store) server.Replayable {
	return server.Replayable{
		Capability: func(store.Item) (string, bool) { return Capability, true },
		Queue: func(ctx context.Context, tx *store.Tx, item store.Item) (bool, error) {
			r, err := records(ctx, tx, item.Key)
			if err != nil {
				return false, err
			}
			_, open, err := tx.OpenTask(ctx, item.Key, store.StageContents)
			if err != nil || !due(r, open) {
				return false, err
			}
			_, err = tx.Enqueue(ctx, store.Task{ItemKey: &item.Key, Stage: store.StageContents, Capability: Capability})
			return err == nil, err
		},
		Estimate: func(ctx context.Context, items []store.Item) (int, *store.Tokens, error) {
			return modelstage.Estimate(ctx, st, store.StageContents, items, due)
		},
	}
}

// records is the outputs and sections of the item under key, as this write
// sees them.
func records(ctx context.Context, tx *store.Tx, key string) (store.ItemRecords, error) {
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return store.ItemRecords{}, err
	}
	held, err := tx.Sections(ctx, key)
	if err != nil {
		return store.ItemRecords{}, err
	}
	return store.ItemRecords{Outputs: outputs, Sections: held}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
