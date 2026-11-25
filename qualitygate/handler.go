package qualitygate

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

const (
	// DefaultModel and DefaultEffort are what the gate's run is asked for
	// where config names neither. The gate reads an opening and answers a
	// short schema, so the smallest model does it.
	DefaultModel  = "haiku"
	DefaultEffort = "medium"
	// openingRunes bounds the item's opening that the prompt quotes.
	openingRunes = 2000
)

// NewHandler is the worker handler that runs the gate through port under
// modelstage's dispatch-once protocol, reading each task's parse through
// client.
func NewHandler(port modelport.Port, client *worker.Client, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Quality, nil },
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			return opening(ctx, client, task)
		},
		Keep: keep,
	}, settings, log)
}

// promptData is what the prompt quotes of the item.
type promptData struct {
	MediaType, Title, URL, Opening string
	Truncated                      bool
	Shown, Total                   int
}

// opening is the task's item as the prompt quotes it: its title and the
// opening of its newest parse.
func opening(ctx context.Context, client *worker.Client, task worker.Task) (promptData, error) {
	parse, err := client.TaskParse(ctx, task.ID, task.LeaseToken)
	if err != nil {
		return promptData{}, fmt.Errorf("read the parse: %w", err)
	}
	text, _ := store.JoinUnits(parse.Units)
	runes := []rune(text)
	data := promptData{MediaType: task.MediaType, Title: deref(task.Title), URL: deref(task.URL), Opening: text, Total: len(runes)}
	if len(runes) > openingRunes {
		data.Opening, data.Truncated, data.Shown = string(runes[:openingRunes]), true, openingRunes
	}
	return data, nil
}

// keep keeps the answer's quality and side guess in the output's own
// columns, and logs them with the verdict.
func keep(_ context.Context, _ prompts.Prompt, raw json.RawMessage, out *server.ReportOutput) ([]any, error) {
	answer, err := ReadAnswer(raw)
	if err != nil {
		return nil, err
	}
	out.Quality, out.SideGuess, out.SideConfidence = &answer.Quality, &answer.Side, &answer.SideConfidence
	return []any{"verdict", answer.Verdict, "quality", answer.Quality, "side", answer.Side, "side_confidence", answer.SideConfidence}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
