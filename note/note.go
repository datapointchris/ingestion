// Package note is the model stage that drafts a model read's note where no
// person wrote one: questions for the read to answer, why the item was likely
// saved, how it bears on the owner's current work, a
// summary and a one-line idea, with how sure the draft is. It reads the item's
// title, URL and the opening of its reading parse under the read decision's
// lens, beside that lens's context where a reader is opened for it. Its
// prompt and answer schema are prompts.Note, one prompt for each lens.
//
// The read gate queues the draft where a read by a model carries no note, and
// serve's route.AfterDraft decides the read from it once it ends. A draft is a
// model's answer over outsider text, so it stays on the note's output and
// never reaches a decision's note.
package note

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/datapointchris/ingestion/lens"
	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// Capability is the capability of every note task.
	Capability = store.StageNote
	// Producer names the note's outputs, whose version is the prompt's under
	// the output's lens.
	Producer = "note"
	// DefaultModel and DefaultEffort are what the draft's run is asked for
	// where config names neither.
	DefaultModel  = "haiku"
	DefaultEffort = "medium"
	// openingRunes bounds the item's opening that the prompt quotes.
	openingRunes = 2000
)

// The draft quotes the opening of the item's reading parse.
func init() { store.QuotesParse(Capability, store.ParsePick{}) }

// Answer is the model's answer, as prompts/note/answer.schema.json holds it.
type Answer struct {
	route.Draft
	Relation string `json:"relation"`
	Summary  string `json:"summary"`
	Idea     string `json:"idea"`
}

// ReadAnswer checks raw against the schema of p, the prompt it answered, and
// decodes it.
func ReadAnswer(p prompts.Prompt, raw json.RawMessage) (Answer, error) {
	if err := p.Check(raw); err != nil {
		return Answer{}, err
	}
	return DecodeAnswer(raw)
}

// DecodeAnswer decodes a stored answer without checking it against this
// build's schema, which its run's schema checked when it arrived.
func DecodeAnswer(raw json.RawMessage) (Answer, error) {
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the note's draft: %w", err)
	}
	return a, nil
}

// NewHandler is the worker handler that drafts each note through port under
// modelstage's dispatch-once protocol. It reads each task's parse through
// client, and its lens's context through contexts.
func NewHandler(port modelport.Port, client *worker.Client, contexts lens.Readers, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: promptOf,
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			return opening(ctx, client, contexts, task, log)
		},
		Keep: keep,
	}, settings, log)
}

// promptOf is the note prompt of the lens that the task's read decision names.
func promptOf(task worker.Task) (prompts.Prompt, error) {
	name := deref(task.Lens)
	p, ok := prompts.Note[name]
	if !ok {
		return prompts.Prompt{}, fmt.Errorf("the item's read decision names the lens %q, and this build holds a note prompt for none of that name", name)
	}
	return p, nil
}

// promptData is what the prompt quotes of the item, and the lens's context.
type promptData struct {
	MediaType, Title, URL, Opening, Context string
	Truncated                               bool
	Shown, Total                            int
}

// opening is the task's item as the prompt quotes it: its title, the opening
// of its reading parse, and its lens's context, which is empty where no
// reader is opened for the lens or none could be read, and logged on log.
func opening(ctx context.Context, client *worker.Client, contexts lens.Readers, task worker.Task, log *slog.Logger) (promptData, error) {
	parse, err := client.QuotedParse(ctx, task)
	if err != nil {
		return promptData{}, fmt.Errorf("read the parse: %w", err)
	}
	context := contexts.Text(ctx, deref(task.Lens), log.With("task", task.ID))
	text, _ := store.JoinUnits(parse.Units)
	runes := []rune(text)
	data := promptData{
		MediaType: task.MediaType, Title: deref(task.Title), URL: deref(task.URL), Opening: text, Context: context, Total: len(runes),
	}
	if len(runes) > openingRunes {
		data.Opening, data.Truncated, data.Shown = string(runes[:openingRunes]), true, openingRunes
	}
	return data, nil
}

// keep checks the answer against its prompt's schema, and logs its counts
// and confidence alone, because a model wrote every field from outsider text.
func keep(_ context.Context, _ worker.Task, p prompts.Prompt, raw json.RawMessage, _ *server.ReportOutput) ([]any, error) {
	a, err := ReadAnswer(p, raw)
	if err != nil {
		return nil, err
	}
	return []any{"questions", len(a.Questions), "confidence", a.Confidence, "summary_runes", len([]rune(strings.TrimSpace(a.Summary)))}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
