package qualitygate

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/lens"
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
	// FitLens is the lens whose context the fit weighs an item against. A
	// model read takes its side as its lens, and the fleet side's lens holds
	// the owner's work under way.
	FitLens = connectors.SideFleet
)

// NewHandler is the worker handler that runs the gate through port under
// modelstage's dispatch-once protocol, reading each task's parse through
// client and FitLens's context through contexts.
func NewHandler(port modelport.Port, client *worker.Client, contexts lens.Readers, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	return newHandler(Capability, port, client, contexts, settings, log)
}

// NewArmHandler is the worker handler that runs the arm's tasks the same way.
// Each asks for the model that its task names, at the gate's effort.
func NewArmHandler(port modelport.Port, client *worker.Client, contexts lens.Readers, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	return newHandler(ArmCapability, port, client, contexts, settings, log)
}

func newHandler(stage string, port modelport.Port, client *worker.Client, contexts lens.Readers, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: stage, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Quality, nil },
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			return opening(ctx, client, contexts, task, log)
		},
		Keep: keep,
	}, settings, log)
}

// promptData is what the prompt quotes of the item, and the owner's current
// work, which is empty where no reader is opened for FitLens.
type promptData struct {
	MediaType, Title, URL, Opening, Context string
	Truncated                               bool
	Shown, Total                            int
}

// opening is the task's item as the prompt quotes it: its title, the opening
// of its newest parse, and FitLens's context, which a read that fails leaves
// as the last read or empty, logged on log.
func opening(ctx context.Context, client *worker.Client, contexts lens.Readers, task worker.Task, log *slog.Logger) (promptData, error) {
	parse, err := client.TaskParse(ctx, task.ID, task.LeaseToken, store.ParsePick{})
	if err != nil {
		return promptData{}, fmt.Errorf("read the parse: %w", err)
	}
	text, _ := store.JoinUnits(parse.Units)
	runes := []rune(text)
	data := promptData{
		MediaType: task.MediaType, Title: deref(task.Title), URL: deref(task.URL), Opening: text, Total: len(runes),
		Context: contexts.Text(ctx, FitLens, log.With("task", task.ID)),
	}
	if len(runes) > openingRunes {
		data.Opening, data.Truncated, data.Shown = string(runes[:openingRunes]), true, openingRunes
	}
	return data, nil
}

// keep keeps the answer's quality, fit and side guess in the output's own
// columns, and logs them with the verdict.
func keep(_ context.Context, _ worker.Task, _ prompts.Prompt, raw json.RawMessage, out *server.ReportOutput) ([]any, error) {
	answer, err := ReadAnswer(raw)
	if err != nil {
		return nil, err
	}
	out.Quality, out.Fit, out.SideGuess, out.SideConfidence = &answer.Quality, answer.Fit, &answer.Side, &answer.SideConfidence
	fit := "none"
	if answer.Fit != nil {
		fit = fmt.Sprint(*answer.Fit)
	}
	return []any{"verdict", answer.Verdict, "quality", answer.Quality, "fit", fit, "side", answer.Side, "side_confidence", answer.SideConfidence}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
