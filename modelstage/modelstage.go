// Package modelstage is the worker's side of every model stage: the
// dispatch-once protocol that each stage's handler runs. A stage keeps only
// the prompt it reads an item under, what that prompt quotes, and how the
// run's answer becomes the output.
//
// A task's call is started once, under an idempotency key of its subject, the
// stage, the prompt's version and the parse that the prompt quotes. The
// subject is the task's item, or the prompt proposal that a prompt-proposal
// task drafts. The output names that parse, so a call over a newer parse is
// started once more, and only its answer replaces the older one. The dispatch
// is stored on the output in the write that ends the worker's lease, beside a
// done output as its replacement where one stands. Each later claim follows
// it until the run answers. So a waiting task holds no worker slot, and a lease that
// lapsed never pays for a second run. A run that failed is started again only
// once `tasks retry` queues its task, under a key that also names the failed
// dispatch, because a dispatch service may answer the old key with the
// failed run.
package modelstage

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// followFirst is how long a dispatched run waits before its task reads it
	// again, doubling each claim up to followMost.
	followFirst = 15 * time.Second
	followMost  = 15 * time.Minute
	// estimateFrom bounds the stored runs, newest first, whose median a
	// replay's estimate takes.
	estimateFrom = 50
)

// ErrKeepAgain marks a Keep error where the answer holds to its schema and a
// step after it failed, such as the scan of a suggestion. Where the run keeps
// a dispatch, the task follows that run again after its backoff, so the paid
// answer is kept without a second call. Where it keeps none, the output fails.
var ErrKeepAgain = errors.New("the answer waits to be kept")

// Settings are what a stage asks its run for.
type Settings struct {
	Model  string
	Effort string
}

// Stage is one model stage as its handler runs it.
type Stage struct {
	// Name is the store's stage, and the capability of each of its tasks.
	Name string
	// Producer names the stage's outputs, whose version is the prompt's.
	Producer string
	// Prompt is the prompt that task's item is read under, which names the
	// output. An error fails the task with no output, as a lens that this
	// build holds no prompt for does.
	Prompt func(task worker.Task) (prompts.Prompt, error)
	// Data is what the prompt quotes of task's item. It is read only where a
	// call starts. A stage whose prompt quotes a parse registers it with
	// store.QuotesParse, and reads the parse that the task's claim named with
	// worker.Client.QuotedParse, which the output names. An error marked
	// modelport.ErrRefused fails the task, and any other tries again after a
	// backoff.
	Data func(ctx context.Context, task worker.Task) (any, error)
	// Keep fills out from the answer that a run gave to prompt on task, and
	// returns the answer's fields that the answered line logs, never outsider
	// text. An error fails the output, as an answer that breaks prompt's
	// schema does, and `tasks retry` starts a new run. An error that wraps
	// ErrKeepAgain leaves the output waiting on its dispatch instead.
	Keep func(ctx context.Context, task worker.Task, prompt prompts.Prompt, answer json.RawMessage, out *server.ReportOutput) (logged []any, err error)
}

// NewHandler is the worker handler that runs stage's tasks through port under
// settings.
func NewHandler(port modelport.Port, stage Stage, settings Settings, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	return handler{port: port, stage: stage, settings: settings, log: log}
}

type handler struct {
	port     modelport.Port
	stage    Stage
	settings Settings
	log      *slog.Logger
}

func (h handler) Capability() string { return h.stage.Name }

// Run answers the stage on the task's item. Where the prompt in force already
// dispatched a run that has not answered, it reads that run. Where that run
// failed, the task runs again only once `tasks retry` queues it, and it
// dispatches again under a key that names the failed dispatch.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	log := h.log.With("task", task.ID, "item", deref(task.ItemKey), "stage", h.stage.Name, "adapter", h.port.Adapter())
	if task.ProposalID != nil {
		log = log.With("proposal", *task.ProposalID)
	}
	prompt, err := h.stage.Prompt(task)
	if err != nil {
		reason := err.Error()
		log.Warn("the task's item names no prompt this build holds", "error", err)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	own := h.own(task, prompt)
	model, effort := h.model(task), h.settings.Effort
	var progress modelport.Progress
	switch {
	case own != nil && own.State == store.OutputDone:
		return worker.Result{Outcome: server.OutcomeDone}
	case own != nil && own.State == store.OutputWaiting && own.DispatchID != nil:
		model, effort = cmp.Or(deref(own.Model), model), cmp.Or(deref(own.Effort), effort)
		progress, err = h.port.Follow(ctx, *own.DispatchID)
	default:
		var request modelport.Request
		if request, err = h.request(ctx, task, prompt); err == nil {
			if own != nil && own.State == store.OutputFailed {
				request.Replaces = deref(own.DispatchID)
			}
			progress, err = h.port.Start(ctx, request)
			log.Info("the stage sent its call", "dispatch", progress.Dispatch, "answered", progress.Answer != nil, "error", err)
		}
	}
	if err != nil {
		return h.refusal(task, prompt, own, err, log)
	}
	out := h.output(task, prompt, model, effort, progress.Dispatch)
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
		failed := fmt.Sprintf("the %s stage's run failed: %s", h.stage.Name, a.Failure)
		log.Warn("the stage's run failed", "dispatch", progress.Dispatch, "run", a.Run, "failure", a.Failure)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &failed, Outputs: []server.ReportOutput{out}}
	}
	kept := out
	logged, err := h.stage.Keep(ctx, task, prompt, a.Output, &kept)
	if errors.Is(err, ErrKeepAgain) && progress.Dispatch != "" {
		reason := err.Error()
		out.State, out.Error = store.OutputWaiting, &reason
		log.Warn("the run's answer was not kept yet, so the task reads the same run again", "dispatch", progress.Dispatch, "run", a.Run, "error", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: followAfter(task.Attempts), Outputs: []server.ReportOutput{out}}
	}
	if err != nil {
		reason := err.Error()
		out.State, out.Error = store.OutputFailed, &reason
		log.Warn("the run's answer was not kept, so its output fails", "dispatch", progress.Dispatch, "run", a.Run, "error", err)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{out}}
	}
	kept.State, kept.Answer = store.OutputDone, a.Output
	log.Info("the stage's run answered", append([]any{"dispatch", progress.Dispatch, "run", a.Run, "findings", len(kept.Findings)}, logged...)...)
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{kept}}
}

// own is the task's output from prompt that answers the task's note over the
// parse that the claim named, and nil where it holds none. An output over an
// older parse is no answer, so the task calls again over the parse named.
func (h handler) own(task worker.Task, prompt prompts.Prompt) *server.StageOutput {
	i := slices.IndexFunc(task.StageOutputs, func(o server.StageOutput) bool {
		return o.Producer == h.stage.Producer && o.ProducerVersion == prompt.Version && store.NoteAnswered(o.NoteDigest, task.Answers()) &&
			store.SameParse(store.InputOf(o.InputProducer, o.InputVersion), task.Parse)
	})
	if i < 0 {
		return nil
	}
	return &task.StageOutputs[i]
}

// request is the stage's call on the task's item under prompt.
func (h handler) request(ctx context.Context, task worker.Task, prompt prompts.Prompt) (modelport.Request, error) {
	data, err := h.stage.Data(ctx, task)
	if err != nil {
		return modelport.Request{}, err
	}
	text, err := prompt.Render(data)
	if err != nil {
		return modelport.Request{}, fmt.Errorf("%w: %w", modelport.ErrRefused, err)
	}
	request := modelport.Request{
		Subject: subject(task), Stage: h.stage.Name, PromptVersion: prompt.Version, NoteDigest: deref(store.NoteDigest(task.Answers())),
		Prompt: text, Schema: prompt.Schema, Model: h.model(task), Effort: h.settings.Effort,
	}
	if extractor, version := quoted(task); extractor != nil {
		request.Parse = *extractor + "@" + *version
	}
	return request, nil
}

// quoted is the extractor and version of the parse that the task's claim
// named as the one its prompt quotes, each nil where it named none.
func quoted(task worker.Task) (extractor, version *string) {
	if task.Parse == nil {
		return nil, nil
	}
	return &task.Parse.Extractor, &task.Parse.Version
}

// subject names what the task's call answers: its item, or the prompt
// proposal that it drafts.
func subject(task worker.Task) string {
	return cmp.Or(deref(task.ItemKey), deref(task.ProposalID))
}

// model is the model that the task's run is asked for: the task's own where
// it names one, as a measurement arm's does, else the stage's setting.
func (h handler) model(task worker.Task) string {
	return cmp.Or(deref(task.Model), h.settings.Model)
}

// output is the output that prompt names for the task's note over the parse
// that the claim named, asked of model at effort, waiting on dispatch.
func (h handler) output(task worker.Task, prompt prompts.Prompt, model, effort, dispatch string) server.ReportOutput {
	out := server.ReportOutput{
		Producer: h.stage.Producer, ProducerVersion: prompt.Version, NoteDigest: store.NoteDigest(task.Answers()),
		Model: &model, Effort: &effort, DispatchID: optional(dispatch),
	}
	out.InputProducer, out.InputVersion = quoted(task)
	if prompt.Lens != nil {
		name, version := prompt.Lens.Name, prompt.Lens.Version
		out.Lens, out.LensVersion = &name, &version
	}
	return out
}

// refusal is the result of a call that err ended. One that the account's
// usage limit refused spent nothing, and tries again once the limit resets.
// One that sending again meets the same way fails the task, and its output
// says why, so `tasks retry` sends it again once the cause is fixed. Any other
// tries again after its backoff.
func (h handler) refusal(task worker.Task, prompt prompts.Prompt, own *server.StageOutput, err error, log *slog.Logger) worker.Result {
	reason := err.Error()
	if limit := (*modelport.LimitError)(nil); errors.As(err, &limit) {
		wait := followMost
		if !limit.ResetsAt.IsZero() {
			wait = max(time.Until(limit.ResetsAt), followFirst)
		}
		log.Warn("the account's usage limit refused the stage's call, which waits for its reset", "error", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: wait}
	}
	if !errors.Is(err, modelport.ErrRefused) {
		log.Warn("the stage's call did not go through, and tries again", "error", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: followAfter(task.Attempts)}
	}
	out := h.output(task, prompt, "", "", "")
	out.Model, out.Effort, out.State, out.Error = nil, nil, store.OutputFailed, &reason
	if own != nil {
		out.DispatchID, out.Model, out.Effort = own.DispatchID, own.Model, own.Effort
	}
	log.Warn("the stage's call was refused", "error", err)
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

// Estimate is how many of items a replay of stage would run on, as due says of
// each item's records and whether it holds an open task at stage. Its tokens
// are the median by kind of the stage's newest stored runs times that count,
// and nil where the stage holds no stored run.
func Estimate(ctx context.Context, st *store.Store, stage string, items []store.Item,
	due func(r store.ItemRecords, open bool) bool,
) (int, *store.Tokens, error) {
	n := 0
	for _, item := range items {
		r, err := st.Item(ctx, item.Key)
		if err != nil {
			return 0, nil, err
		}
		open := slices.ContainsFunc(r.Tasks, func(t store.Task) bool {
			return t.Stage == stage && (t.State == store.TaskQueued || t.State == store.TaskLeased)
		})
		if due(r, open) {
			n++
		}
	}
	stored, err := st.StageTokens(ctx, stage, estimateFrom)
	if err != nil || len(stored) == 0 {
		return n, nil, err
	}
	median := store.MedianTokens(stored)
	return n, &store.Tokens{
		UncachedInput: median.UncachedInput * int64(n), CacheWrite: median.CacheWrite * int64(n),
		CacheRead: median.CacheRead * int64(n), Output: median.Output * int64(n),
	}, nil
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
