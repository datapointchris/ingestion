// Package contents is the model stage that reads a book's printed table of
// contents, for a PDF of sections.WholeFilePages pages or more whose outline
// gives it no sections under the outline rule in force. Its prompt and answer
// schema are prompts.Contents.
//
// The service queues a book's contents task in the write that ends its first
// structure task, where the outline rule wrote it no sections. A connector's
// first fill holds every book after the first to reach the stage, as at every
// model stage. A worker holding a model port runs the task under modelstage's
// dispatch-once protocol. Its prompt quotes the front pages that look like a
// contents page, or the first frontPages pages where none does. The model
// answers each entry's title, printed page and level.
//
// When the task ends done, serve places each entry on a PDF page, by the
// pdf-text parse's page label, else by the book's measured offset, then by a
// search for the title near that page. The entries then pass the outline rule
// through sections.FromEntries, and sections.Write writes them under producer
// contents with a chapter item per chapter, as an outline's are. A later
// structure task places the stored answer again, so a structure replay
// applies a new placement, outline rule or parse with no model call.
package contents

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"

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
	// its sections beside the old once a structure replay places each
	// stored answer again.
	PlacementVersion = "2"
	// mostLeftOut is the largest share of an answer's paged entries at one
	// level, at or above the chapter level, that the placement may leave out
	// and still write the book's sections. A left-out chapter leaves its
	// pages to the one before it, and once the book holds chapters no later
	// version makes its item. So a placement that leaves out more writes
	// none, and the book stays whole until a better placement places its
	// answer again. A left-out section costs no chapter, so sections are not
	// counted.
	mostLeftOut = 0.2
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

// AfterStructure is the hook that runs in the write that ends a book's
// structure task done, where the book waits for its contents. Where the book
// holds a done answer, it places that answer again. Where it holds none, it
// queues the book's contents task on the book's first profile. A later
// profile queues none, so a replay of a model-free stage starts no model
// call, and `items replay --stage contents` reads such a book once a person
// confirms its estimate.
func AfterStructure(log *slog.Logger) server.AfterStage {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil {
			return nil
		}
		r, err := tx.Records(ctx, *task.ItemKey)
		if err != nil || !waitsForContents(r) {
			return err
		}
		if store.NewestDone(r.Outputs, store.StageContents) != nil {
			return place(ctx, tx, r, log)
		}
		if slices.ContainsFunc(r.Tasks, func(t store.Task) bool {
			return t.Stage == store.StageContents || t.Stage == store.StageStructure && t.State == store.TaskDone && t.ID != task.ID
		}) {
			return nil
		}
		_, err = tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageContents, Capability: Capability})
		return err
	}
}

// AfterAnswer is the hook that runs in the write that ends a book's contents
// task done, and places its answer where the book still waits for it.
func AfterAnswer(log *slog.Logger) server.AfterStage {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil {
			return nil
		}
		r, err := tx.Records(ctx, *task.ItemKey)
		if err != nil || !waitsForContents(r) {
			return err
		}
		return place(ctx, tx, r, log)
	}
}

// place places the entries of the book's newest done answer on its pdf-text
// parse, and writes the sections they make through sections.Write. It writes
// none where the entries make no usable outline, or where it left out more
// than mostLeftOut of a level's paged entries at or above the chapter level,
// and logs which.
func place(ctx context.Context, tx *store.Tx, r store.ItemRecords, log *slog.Logger) error {
	key := r.Item.Key
	newest := store.NewestDone(r.Outputs, store.StageContents)
	if newest == nil {
		return nil
	}
	answer, err := ReadAnswer(newest.Answer)
	if err != nil {
		return fmt.Errorf("read the contents answer on %s: %w", key, err)
	}
	profile, _, err := sections.NewestProfile(r.Outputs)
	if err != nil {
		return err
	}
	parse, err := tx.Parse(ctx, key, store.ParsePick{Extractor: sections.ParsedBy.Name})
	if err != nil {
		return fmt.Errorf("read the parse that %s's contents are placed on: %w", key, err)
	}
	version := Version(*newest, parse.Output)
	if slices.ContainsFunc(r.Sections, func(s store.Section) bool {
		return s.Producer == store.SectionsFromContents && s.ProducerVersion == version
	}) {
		return nil
	}
	entries, left := Place(answer.Entries, parse.Units)
	counts := []any{"key", key, "version", version, "entries", len(answer.Entries), "placed", len(entries), "left_out", len(left)}
	book := sections.Book{Pages: len(parse.Units), Titles: []string{deref(r.Item.Title), deref(profile.Title)}, Units: parse.Units}
	rows := sections.FromEntries(entries, book, store.SectionsFromContents, version)
	if rows == nil {
		log.Warn("the placed contents entries make no usable outline, so the book holds no sections from them", counts...)
		return nil
	}
	if level, ok := tooManyLeftOut(answer.Entries, left, rows); ok {
		log.Warn("the placement left out too many contents entries at one level, so the book holds no sections from them",
			append(counts, "level", level)...)
		return nil
	}
	log.Info("placed the book's contents entries", append(counts, "sections", len(rows))...)
	return sections.Write(ctx, tx, r.Item, rows)
}

// tooManyLeftOut is the first level, at or above the chapter level of rows,
// at which left holds more than mostLeftOut of answer's paged entries, and
// false where none does.
func tooManyLeftOut(answer, left []AnswerEntry, rows []store.Section) (int, bool) {
	chapters := 0
	for _, s := range rows {
		if s.Kind == store.SectionChapter {
			chapters = max(chapters, s.Level)
		}
	}
	paged, out := map[int]int{}, map[int]int{}
	for _, e := range answer {
		if e.Page != nil && strings.TrimSpace(*e.Page) != "" {
			paged[e.Level]++
		}
	}
	for _, e := range left {
		out[e.Level]++
	}
	for level := 1; level <= chapters; level++ {
		if float64(out[level]) > mostLeftOut*float64(paged[level]) {
			return level, true
		}
	}
	return 0, false
}

// waitsForContents says whether a book takes its sections from its printed
// contents: its newest profile counts sections.WholeFilePages pages or more,
// its pdf-text parse is done, and the outline rule in force gives it no
// sections. It asks the rule rather than the sections held, so a book that an
// older rule outlined waits once the rule in force finds its outline unusable.
func waitsForContents(r store.ItemRecords) bool {
	profile, ok, err := sections.NewestProfile(r.Outputs)
	if err != nil || !ok || profile.Pages < sections.WholeFilePages {
		return false
	}
	parsed := slices.ContainsFunc(r.Outputs, func(o store.Output) bool {
		return o.Stage == store.StageParse && o.Producer == sections.ParsedBy.Name && o.State == store.OutputDone
	})
	return parsed && sections.Outline(profile, nil, deref(r.Item.Title), "") == nil
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
			r, err := tx.Records(ctx, item.Key)
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

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
