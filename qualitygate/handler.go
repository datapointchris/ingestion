package qualitygate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/datapointchris/ingestion/modelport"
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
	// followFirst is how long a dispatched run waits before its task reads it
	// again, doubling each claim up to followMost.
	followFirst = 15 * time.Second
	followMost  = 15 * time.Minute
)

// Settings are what the gate asks its run for.
type Settings struct {
	Model  string
	Effort string
}

// NewHandler is the worker handler that runs the gate through port, reading
// each task's parse through client.
func NewHandler(port modelport.Port, client *worker.Client, settings Settings, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	if settings.Model == "" {
		settings.Model = DefaultModel
	}
	if settings.Effort == "" {
		settings.Effort = DefaultEffort
	}
	return handler{port: port, client: client, settings: settings, log: log}
}

type handler struct {
	port     modelport.Port
	client   *worker.Client
	settings Settings
	log      *slog.Logger
}

func (handler) Capability() string { return Capability }

// promptData is what the prompt quotes of the item.
type promptData struct {
	MediaType, Title, URL, Opening string
	Truncated                      bool
	Shown, Total                   int
}

// Run answers the gate on the task's item. Where the prompt in force already
// dispatched a run that has not answered, it reads that run, so a task whose
// lease lapsed or whose worker restarted never pays for a second. Where that
// run failed, the task runs again only once `tasks retry` queues it, and it
// dispatches again under a key that names the failed dispatch.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	own := h.own(task)
	log := h.log.With("task", task.ID, "item", deref(task.ItemKey), "adapter", h.port.Adapter())
	model, effort := h.settings.Model, h.settings.Effort
	var progress modelport.Progress
	var err error
	switch {
	case own != nil && own.State == store.OutputDone:
		return worker.Result{Outcome: server.OutcomeDone}
	case own != nil && own.State == store.OutputWaiting && own.DispatchID != nil:
		model, effort = cmp.Or(deref(own.Model), model), cmp.Or(deref(own.Effort), effort)
		progress, err = h.port.Follow(ctx, *own.DispatchID)
	default:
		var request modelport.Request
		if request, err = h.request(ctx, task); err == nil {
			if own != nil && own.State == store.OutputFailed {
				request.Replaces = deref(own.DispatchID)
			}
			progress, err = h.port.Start(ctx, request)
			log.Info("the quality gate sent its call", "dispatch", progress.Dispatch, "answered", progress.Answer != nil, "error", err)
		}
	}
	if err != nil {
		return h.refusal(task, own, err)
	}
	out := server.ReportOutput{
		Producer: Producer, ProducerVersion: prompts.Quality.Version, Model: &model, Effort: &effort,
		DispatchID: optional(progress.Dispatch),
	}
	a := progress.Answer
	if a == nil {
		out.State, out.Error = store.OutputWaiting, optional(progress.Waiting)
		result := worker.Result{Outcome: server.OutcomeRetry, RetryAfter: followAfter(task.Attempts)}
		if own == nil || own.State != store.OutputWaiting || deref(own.DispatchID) != progress.Dispatch || deref(own.Error) != progress.Waiting {
			result.Outputs = []server.ReportOutput{out}
		}
		return result
	}
	out.RunID, out.Tokens, out.AnsweredModel = optional(a.Run), a.Tokens, optional(a.Model)
	if a.State == modelport.Failed {
		reason := a.Failure
		if a.Detail != "" {
			reason += ": " + a.Detail
		}
		out.State, out.Error = store.OutputFailed, &reason
		failed := fmt.Sprintf("the quality gate's run failed: %s", a.Failure)
		log.Warn("the quality gate's run failed", "dispatch", progress.Dispatch, "run", a.Run, "failure", a.Failure)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &failed, Outputs: []server.ReportOutput{out}}
	}
	answer, err := readAnswer(a.Output)
	if err != nil {
		reason := err.Error()
		out.State, out.Error = store.OutputFailed, &reason
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{out}}
	}
	out.State, out.Answer = store.OutputDone, a.Output
	out.Quality, out.SideGuess, out.SideConfidence = &answer.Quality, &answer.Side, &answer.SideConfidence
	log.Info("the quality gate answered", "dispatch", progress.Dispatch, "run", a.Run, "verdict", answer.Verdict, "side", answer.Side)
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{out}}
}

// own is the task's output from the prompt in force, and nil where it holds
// none.
func (h handler) own(task worker.Task) *server.StageOutput {
	for i, o := range task.StageOutputs {
		if o.Producer == Producer && o.ProducerVersion == prompts.Quality.Version {
			return &task.StageOutputs[i]
		}
	}
	return nil
}

// request is the gate's call on the task's item: its title and the opening of
// its newest parse.
func (h handler) request(ctx context.Context, task worker.Task) (modelport.Request, error) {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken)
	if err != nil {
		return modelport.Request{}, fmt.Errorf("read the parse: %w", err)
	}
	text, _ := store.JoinUnits(parse.Units)
	runes := []rune(text)
	data := promptData{MediaType: task.MediaType, Title: deref(task.Title), URL: deref(task.URL), Opening: text, Total: len(runes)}
	if len(runes) > openingRunes {
		data.Opening, data.Truncated, data.Shown = string(runes[:openingRunes]), true, openingRunes
	}
	prompt, err := prompts.Quality.Render(data)
	if err != nil {
		return modelport.Request{}, fmt.Errorf("%w: %w", modelport.ErrRefused, err)
	}
	return modelport.Request{
		ItemKey: deref(task.ItemKey), Stage: store.StageQuality, PromptVersion: prompts.Quality.Version,
		Prompt: prompt, Schema: prompts.Quality.Schema, Model: h.settings.Model, Effort: h.settings.Effort,
	}, nil
}

// refusal is the result of a call that err ended. One that the account's
// usage limit refused spent nothing, and tries again once the limit resets.
// One that sending again meets the same way fails the task, and its output
// says why, so `tasks retry` sends it again once the cause is fixed. Any other
// tries again after its backoff.
func (h handler) refusal(task worker.Task, own *server.StageOutput, err error) worker.Result {
	reason := err.Error()
	if limit := (*modelport.LimitError)(nil); errors.As(err, &limit) {
		wait := followMost
		if !limit.ResetsAt.IsZero() {
			wait = max(time.Until(limit.ResetsAt), followFirst)
		}
		h.log.Warn("the account's usage limit refused the quality gate's call, which waits for its reset", "task", task.ID, "error", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: wait}
	}
	if !errors.Is(err, modelport.ErrRefused) {
		h.log.Warn("the quality gate's call did not go through, and tries again", "task", task.ID, "error", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: followAfter(task.Attempts)}
	}
	out := server.ReportOutput{Producer: Producer, ProducerVersion: prompts.Quality.Version, State: store.OutputFailed, Error: &reason}
	if own != nil {
		out.DispatchID, out.Model, out.Effort = own.DispatchID, own.Model, own.Effort
	}
	h.log.Warn("the quality gate's call was refused", "task", task.ID, "error", err)
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{out}}
}

// followAfter is how long a task waits before its claim reads its run again,
// after attempts claims.
func followAfter(attempts int) time.Duration {
	wait := followFirst
	for i := 1; i < attempts && wait < followMost; i++ {
		wait *= 2
	}
	return min(wait, followMost)
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
