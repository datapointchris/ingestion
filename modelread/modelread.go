// Package modelread is the model stage that reads an item whole under its
// read decision's lens, and answers the item's note with a summary, a
// suggestion sentence, or both. Its prompt and answer schema are
// prompts.ModelRead, one prompt for each lens.
//
// The read gate queues an item's model read in the write that appends a read
// decision whose verdict is model, unless the item holds a read that answers
// the decision's note. It withdraws a queued one where a later decision reads
// the item otherwise. The task carries that
// decision's lens and note. A worker holding a model port and the scanner
// runs it through modelstage's dispatch-once protocol. Before the answer is
// kept, the scanner reads its suggestion sentence. The output holds the
// findings and their spans beside the answer, so the sentence never travels
// without them. The summary is kept on the output alone and delivered
// nowhere.
package modelread

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// Capability is the capability of every model read's task.
	Capability = store.StageModelRead
	// Producer names the model read's outputs, whose version is the prompt's
	// under the output's lens.
	Producer = "model-read"
	// DefaultModel and DefaultEffort are what the read's run is asked for
	// where config names neither.
	DefaultModel  = "haiku"
	DefaultEffort = "medium"
	// textRunes bounds the item's text that the prompt quotes.
	textRunes = 100_000
)

// Answer is the model's answer, as prompts/model-read/answer.schema.json
// holds it. Either may be nil, and never both.
type Answer struct {
	Summary    *string `json:"summary"`
	Suggestion *string `json:"suggestion"`
}

// errNothingAnswered is an answer holding neither a summary nor a suggestion.
var errNothingAnswered = errors.New("the model read's answer holds neither a summary nor a suggestion")

// ReadAnswer checks raw against the schema of p, the prompt it answered, and
// decodes it. An answer holding neither a summary nor a suggestion fails as
// one that breaks the schema does.
func ReadAnswer(p prompts.Prompt, raw json.RawMessage) (Answer, error) {
	if err := p.Check(raw); err != nil {
		return Answer{}, err
	}
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the model read's answer: %w", err)
	}
	if a.Summary == nil && a.Suggestion == nil {
		return Answer{}, errNothingAnswered
	}
	return a, nil
}

// Scanner reads a suggestion sentence before it is kept. *scan.Scanner is
// one.
type Scanner interface {
	// Version is the rules' version, which the output records beside the
	// findings.
	Version() string
	Scan(ctx context.Context, text string) ([]store.Finding, []store.Span, error)
}

// NewHandler is the worker handler that runs the model read through port
// under modelstage's dispatch-once protocol. It reads each task's parse
// through client, and scans each suggestion with scanner.
func NewHandler(port modelport.Port, client *worker.Client, scanner Scanner, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: promptOf,
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			return read(ctx, client, task)
		},
		Keep: func(ctx context.Context, p prompts.Prompt, raw json.RawMessage, out *server.ReportOutput) ([]any, error) {
			return keep(ctx, scanner, p, raw, out)
		},
	}, settings, log)
}

// promptOf is the prompt of the lens that the task's read decision names.
func promptOf(task worker.Task) (prompts.Prompt, error) {
	lens := deref(task.Lens)
	p, ok := prompts.ModelRead[lens]
	if !ok {
		return prompts.Prompt{}, fmt.Errorf("the item's read decision names the lens %q, and this build holds a prompt only for %s",
			lens, strings.Join(prompts.Lenses(), ", "))
	}
	return p, nil
}

// promptData is what the prompt quotes of the item, and the note that a
// person wrote.
type promptData struct {
	MediaType, Title, URL, Text, Note string
	Truncated                         bool
	Shown, Total                      int
}

// read is the task's item as the prompt quotes it: its title, its newest
// parse's text up to textRunes, and its note.
func read(ctx context.Context, client *worker.Client, task worker.Task) (promptData, error) {
	parse, err := client.TaskParse(ctx, task.ID, task.LeaseToken)
	if err != nil {
		return promptData{}, fmt.Errorf("read the parse: %w", err)
	}
	text, _ := store.JoinUnits(parse.Units)
	runes := []rune(text)
	data := promptData{
		MediaType: task.MediaType, Title: deref(task.Title), URL: deref(task.URL), Text: text, Note: strings.TrimSpace(deref(task.Note)),
		Total: len(runes),
	}
	if len(runes) > textRunes {
		data.Text, data.Truncated, data.Shown = string(runes[:textRunes]), true, textRunes
	}
	return data, nil
}

// keep scans the answer's suggestion and keeps its findings and their spans
// on the output, under the rules' version. A scan runs under the scan stage's
// bound, and one that fails keeps nothing yet, so no sentence is kept
// unscanned and the task reads the same run again. It logs each field's
// length alone, because a model wrote both from outsider text.
func keep(ctx context.Context, scanner Scanner, p prompts.Prompt, raw json.RawMessage, out *server.ReportOutput) ([]any, error) {
	a, err := ReadAnswer(p, raw)
	if err != nil {
		return nil, err
	}
	logged := []any{"summary_runes", runeCount(a.Summary), "suggestion_runes", runeCount(a.Suggestion)}
	if a.Suggestion == nil {
		return logged, nil
	}
	scanCtx, cancel := context.WithTimeout(ctx, scan.Timeout)
	defer cancel()
	findings, spans, err := scanner.Scan(scanCtx, *a.Suggestion)
	if err != nil {
		return nil, fmt.Errorf("%w: scan the suggestion: %w", modelstage.ErrKeepAgain, err)
	}
	rules := scanner.Version()
	out.Findings, out.Spans, out.RulesVersion = findings, spans, &rules
	return logged, nil
}

// runeCount is the length of a field that may be null, which counts as 0.
func runeCount(s *string) int {
	if s == nil {
		return 0
	}
	return utf8.RuneCountInString(*s)
}

// Replayable is the read as a replay runs it again, such as with a new prompt
// or lens, over each item that route.ModelReadDue names. The replay first answers how many
// items it would run on and the median tokens by kind of the stage's newest
// stored runs, times that count, and queues only once a person confirms.
func Replayable(st *store.Store) server.Replayable {
	return server.Replayable{
		Capability: func(store.Item) (string, bool) { return Capability, true },
		Queue: func(ctx context.Context, tx *store.Tx, item store.Item) (bool, error) {
			side, _, err := tx.NewestDecision(ctx, item.Key, store.GateSide)
			if err != nil {
				return false, err
			}
			read, _, err := tx.NewestDecision(ctx, item.Key, store.GateRead)
			if err != nil {
				return false, err
			}
			outputs, err := tx.Outputs(ctx, item.Key)
			if err != nil {
				return false, err
			}
			_, open, err := tx.OpenTask(ctx, item.Key, store.StageModelRead)
			if err != nil || !route.ModelReadDue(side, read, outputs, open) {
				return false, err
			}
			_, err = tx.Enqueue(ctx, store.Task{ItemKey: &item.Key, Stage: store.StageModelRead, Capability: Capability})
			return err == nil, err
		},
		Estimate: func(ctx context.Context, items []store.Item) (int, *store.Tokens, error) {
			return modelstage.Estimate(ctx, st, store.StageModelRead, items, func(r store.ItemRecords, open bool) bool {
				side, _ := r.NewestDecision(store.GateSide)
				read, _ := r.NewestDecision(store.GateRead)
				return route.ModelReadDue(side, read, r.Outputs, open)
			})
		},
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
