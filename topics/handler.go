package topics

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// DefaultModel and DefaultEffort are what the stage's run is asked for where
// config names neither.
const (
	DefaultModel  = "sonnet"
	DefaultEffort = "medium"
)

// NewHandler is the worker handler that runs the stage through port under
// modelstage's dispatch-once protocol, reading each task's parse through
// client. It keeps an answer only where its topics cover that parse, as
// Answer.Covers decides, and fails it otherwise.
func NewHandler(port modelport.Port, client *worker.Client, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Topics, nil },
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			parse, err := client.QuotedParse(ctx, task)
			if err != nil {
				return nil, fmt.Errorf("read the parse: %w", err)
			}
			return Quote(parse.Units), nil
		},
		Keep: func(ctx context.Context, task worker.Task, _ prompts.Prompt, raw json.RawMessage, _ *server.ReportOutput) ([]any, error) {
			answer, err := ReadAnswer(raw)
			if err != nil {
				return nil, err
			}
			parse, err := client.QuotedParse(ctx, task)
			if err != nil {
				return nil, fmt.Errorf("read the parse that the topics split: %w: %w", modelstage.ErrKeepAgain, err)
			}
			if err := answer.Covers(len(parse.Units)); err != nil {
				return nil, fmt.Errorf("the topics do not hold each unit once, in order: %w", err)
			}
			return []any{"topics", len(answer.Topics), "units", len(parse.Units)}, nil
		},
	}, settings, log)
}

// PromptData is what the prompt quotes of a memo: each unit of its parse.
type PromptData struct {
	Segments []Segment
}

// Segment is one unit of a memo's parse, as the prompt quotes it: its number
// from 1, its span of the recording, and its text.
type Segment struct {
	N    int
	Span string
	Text string
}

// Quote is what the prompt quotes of the memo whose parse is units.
func Quote(units []store.Unit) PromptData {
	data := PromptData{Segments: make([]Segment, len(units))}
	for i, u := range units {
		data.Segments[i] = Segment{N: i + 1, Text: u.Text}
		if u.Label != nil {
			data.Segments[i].Span = *u.Label
		}
	}
	return data
}
