package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/store"
)

// ScopeTasksClaim admits a worker to claim tasks, extend their leases and
// report them.
const ScopeTasksClaim = "ingestion.tasks.claim"

const (
	defaultLease = 5 * time.Minute
	minLease     = 10 * time.Second
	maxLease     = time.Hour
	maxClaim     = 50
)

// errOutputsWithoutItem is a poll's report carrying outputs, which name no
// item to write them for.
var errOutputsWithoutItem = errors.New("a poll's task reports no outputs")

var (
	errDeliveryOutsideDeliver = errors.New("only a deliver task reports a delivery")
	errBadDelivery            = errors.New("the delivery is not one the store records")
)

// MaxListed is the most tasks one list answers.
const MaxListed = 1000

// ClaimRequest is a worker asking for up to Max tasks whose capability it
// holds, each leased for LeaseSeconds.
type ClaimRequest struct {
	WorkerID     string   `json:"worker_id"`
	Capabilities []string `json:"capabilities"`
	Version      string   `json:"version"`
	StartedTS    string   `json:"started_ts"`
	Max          int      `json:"max"`
	LeaseSeconds int      `json:"lease_seconds"`
}

// LeasedTask is one leased task, with the token for every heartbeat and
// report on it. A stage's task names its item. A poll names its connector,
// and its capability is poll:<source>, so each source's handler claims its
// own.
type LeasedTask struct {
	ID             string  `json:"id"`
	ItemKey        *string `json:"item_key"`
	Connector      *string `json:"connector"`
	Stage          string  `json:"stage"`
	Capability     string  `json:"capability"`
	Attempts       int     `json:"attempts"`
	LeaseToken     int64   `json:"lease_token"`
	LeaseExpiresTS string  `json:"lease_expires_ts"`
	// MediaType and BlobSHA256 are the item's, so a worker reads its bytes
	// holding no scope to read items. A poll carries neither, and BlobSHA256
	// is null where the item holds no bytes.
	MediaType  string  `json:"media_type"`
	BlobSHA256 *string `json:"blob_sha256"`
	// Zone, Title, URL and LandedTS are the item's too, so a deliverer writes
	// it holding no scope to read items. A deliver task's Title and URL are
	// its delivery's, as they were when it was first queued, so a retry sends
	// what the first attempt sent. A poll carries none of them.
	Zone     store.Zone `json:"zone,omitempty"`
	Title    *string    `json:"title"`
	URL      *string    `json:"url"`
	LandedTS string     `json:"landed_ts,omitempty"`
	// Destination names where a deliver task writes its item, and is null
	// for every other task.
	Destination *string `json:"destination"`
	// StageOutputs are the item's outputs at a model stage's task's stage,
	// so a task whose run is already dispatched follows that run rather than
	// starting another. Every other task carries none.
	StageOutputs []StageOutput `json:"stage_outputs,omitempty"`
	// Lens and Note are a model read's task's: the lens of the item's newest
	// read decision, and the note that a person wrote, which the read
	// answers. Every other task carries neither.
	Lens *string `json:"lens,omitempty"`
	Note *string `json:"note,omitempty"`
}

// StageOutput is one output that a model stage's task carries: which prompt
// wrote it, how far it got, and the dispatch it waits on. Error is why a
// waiting one has not started, or why a failed one failed.
type StageOutput struct {
	Producer        string  `json:"producer"`
	ProducerVersion string  `json:"producer_version"`
	State           string  `json:"state"`
	DispatchID      *string `json:"dispatch_id"`
	Model           *string `json:"model"`
	Effort          *string `json:"effort"`
	Error           *string `json:"error"`
}

// ClaimAnswer is every task that the claim leased, and none where nothing waits.
type ClaimAnswer struct {
	Tasks []LeasedTask `json:"tasks"`
}

// HeartbeatRequest extends a lease by LeaseSeconds from now.
type HeartbeatRequest struct {
	Token        int64 `json:"token"`
	LeaseSeconds int   `json:"lease_seconds"`
}

// Report outcomes.
const (
	OutcomeDone   = "done"
	OutcomeFailed = "failed"
	OutcomeRetry  = "retry"
)

// ReportRequest ends a lease. Outputs are the stage's records for the task's
// item, and Delivery is how a deliver task's write ended, each written in the
// same transaction that ends the lease.
type ReportRequest struct {
	Token             int64           `json:"token"`
	Outcome           string          `json:"outcome"`
	Error             *string         `json:"error,omitempty"`
	RetryAfterSeconds int             `json:"retry_after_seconds,omitempty"`
	Outputs           []ReportOutput  `json:"outputs,omitempty"`
	Delivery          *ReportDelivery `json:"delivery,omitempty"`
}

// ReportDelivery is how one write of a deliver task's item ended: its outcome,
// the destination's id for the record that holds the item, and the
// destination's answer where the delivery is pending or refused.
type ReportDelivery struct {
	Outcome       string  `json:"outcome"`
	DestinationID *string `json:"destination_id,omitempty"`
	Error         *string `json:"error,omitempty"`
}

// ReportOutput is one output a worker reports: each field of an outputs row
// that the stage's work decides, and a parse's units. The service takes the
// item and stage from the task and stamps the row, so a column the store adds
// for itself never changes what a worker sends.
type ReportOutput struct {
	Producer        string        `json:"producer"`
	ProducerVersion string        `json:"producer_version"`
	Lens            *string       `json:"lens,omitempty"`
	LensVersion     *string       `json:"lens_version,omitempty"`
	State           string        `json:"state"`
	BlobSHA256      *string       `json:"blob_sha256,omitempty"`
	DispatchID      *string       `json:"dispatch_id,omitempty"`
	RunID           *string       `json:"run_id,omitempty"`
	Model           *string       `json:"model,omitempty"`
	Effort          *string       `json:"effort,omitempty"`
	AnsweredModel   *string       `json:"answered_model,omitempty"`
	Tokens          *store.Tokens `json:"tokens,omitempty"`
	// InputProducer and InputVersion name the parse that a later stage read.
	InputProducer *string `json:"input_producer,omitempty"`
	InputVersion  *string `json:"input_version,omitempty"`
	// Findings and Spans are a scan's, and a model read's over its
	// suggestion sentence. A scan that found nothing sends an empty list,
	// which reads apart from a stage that sends none. RulesVersion is the
	// version of the rules that made them.
	Findings       []store.Finding `json:"findings"`
	RulesVersion   *string         `json:"rules_version,omitempty"`
	Quality        *int            `json:"quality,omitempty"`
	SideGuess      *string         `json:"side_guess,omitempty"`
	SideConfidence *float64        `json:"side_confidence,omitempty"`
	// Units are a parse's, numbered from 1 in order, which the service writes
	// as rows of parse_units under the producer and its version.
	Units  []store.Unit    `json:"units,omitempty"`
	Spans  []store.Span    `json:"spans"`
	Answer json.RawMessage `json:"answer,omitempty"`
	Error  *string         `json:"error,omitempty"`
}

// output is o as the row it writes for task, which names its item.
func (o ReportOutput) output(task store.Task) store.Output {
	return store.Output{
		ItemKey: *task.ItemKey, Stage: task.Stage, Producer: o.Producer, ProducerVersion: o.ProducerVersion,
		Lens: o.Lens, LensVersion: o.LensVersion, InputProducer: o.InputProducer, InputVersion: o.InputVersion,
		State: o.State, BlobSHA256: o.BlobSHA256,
		DispatchID: o.DispatchID, RunID: o.RunID, Model: o.Model, Effort: o.Effort, AnsweredModel: o.AnsweredModel, Tokens: o.Tokens,
		Findings: o.Findings, RulesVersion: o.RulesVersion, Quality: o.Quality,
		SideGuess: o.SideGuess, SideConfidence: o.SideConfidence, Spans: o.Spans, Answer: o.Answer, Error: o.Error,
	}
}

// TaskView is one task as the list shows it. It leaves out the lease token
// and the task's error, which can quote outsider text, and says in has_error
// whether one is kept.
type TaskView struct {
	ID             string  `json:"id"`
	ItemKey        *string `json:"item_key"`
	Connector      *string `json:"connector"`
	Destination    *string `json:"destination"`
	Stage          string  `json:"stage"`
	Capability     string  `json:"capability"`
	State          string  `json:"state"`
	HeldBy         *string `json:"held_by"`
	Lapsed         bool    `json:"lapsed"`
	LeaseHolder    *string `json:"lease_holder"`
	LeaseExpiresTS *string `json:"lease_expires_ts"`
	Attempts       int     `json:"attempts"`
	HasError       bool    `json:"has_error"`
	NotBeforeTS    *string `json:"not_before_ts"`
	CreatedTS      string  `json:"created_ts"`
	UpdatedTS      string  `json:"updated_ts"`
}

// TasksAnswer is what GET /api/v1/tasks answers:
// schemas/tasks-list.schema.json.
type TasksAnswer struct {
	Tasks   []TaskView     `json:"tasks"`
	Workers []store.Worker `json:"workers"`
	// WorkersHidden counts the workers not heard from within
	// WorkersHeardWithin, which the list leaves out unless asked for all.
	WorkersHidden int `json:"workers_hidden"`
}

// WorkersHeardWithin is how recently a worker was heard from for the list to
// show it by default. Each process start is a worker of its own, so the
// workers that stopped outnumber the live ones within a few releases.
const WorkersHeardWithin = 10 * time.Minute

// AfterStage runs in the write that ends an item's task done, after the
// stage's outputs, such as the scan and the gates that follow an item's parse.
// An error rolls the whole report back, so the task stays leased.
type AfterStage func(ctx context.Context, tx *store.Tx, task store.Task) error

// TaskRoutes are the routes for a worker to lease tasks through and read a
// task's input, and the list for a person to read. after runs in the write
// that ends each item's task done, and nil runs nothing.
func TaskRoutes(st *store.Store, log *slog.Logger, after AfterStage) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := taskHandlers{store: st, log: log, after: after}
	return func(r *Router) {
		r.Handle(RouteTaskClaims, h.claim)
		r.Handle(RouteTaskHeartbeats, h.heartbeat)
		r.Handle(RouteTaskReports, h.report)
		r.Handle(RouteTaskParse, h.parse)
		r.Handle(RouteTaskRetries, h.retry)
		r.Handle(RouteTasks, h.list)
	}
}

type taskHandlers struct {
	store *store.Store
	log   *slog.Logger
	after AfterStage
}

func (h taskHandlers) claim(w http.ResponseWriter, r *http.Request) {
	var body ClaimRequest
	if !decode(w, r, &body) {
		return
	}
	if body.WorkerID == "" || body.Version == "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a claim names its worker_id and version")
		return
	}
	if body.Max < 0 || body.Max > maxClaim {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "max is from 0 to %d, not %d", maxClaim, body.Max)
		return
	}
	lease, ok := leaseOf(w, body.LeaseSeconds)
	if !ok {
		return
	}
	tasks, err := h.store.Claim(r.Context(), store.Claim{
		Worker: store.Worker{ID: body.WorkerID, Capabilities: body.Capabilities, Version: body.Version, StartedTS: body.StartedTS},
		Max:    body.Max, Lease: lease,
	})
	if err != nil {
		h.internal(w, r, "claim tasks", err)
		return
	}
	answer := ClaimAnswer{Tasks: []LeasedTask{}}
	for _, t := range tasks {
		leased := LeasedTask{
			ID: t.ID, ItemKey: t.ItemKey, Connector: t.Connector, Stage: t.Stage, Capability: t.Capability, Attempts: t.Attempts,
			LeaseToken: *t.LeaseToken, LeaseExpiresTS: *t.LeaseExpiresTS, MediaType: t.Item.MediaType, BlobSHA256: t.Item.BlobSHA256,
			Zone: t.Item.Zone, Title: t.Item.Title, URL: t.Item.URL, LandedTS: t.Item.CreatedTS, Destination: t.Destination,
		}
		if t.Delivery != nil {
			leased.Title, leased.URL = t.Delivery.Title, t.Delivery.URL
		}
		if t.Read != nil {
			leased.Lens, leased.Note = t.Read.Lens, t.Read.Note
		}
		for _, o := range t.Outputs {
			leased.StageOutputs = append(leased.StageOutputs, StageOutput{
				Producer: o.Producer, ProducerVersion: o.ProducerVersion, State: o.State, DispatchID: o.DispatchID,
				Model: o.Model, Effort: o.Effort, Error: o.Error,
			})
		}
		answer.Tasks = append(answer.Tasks, leased)
	}
	for _, t := range answer.Tasks {
		h.log.Info("leased a task", "task", t.ID, "item", text(t.ItemKey), "connector", text(t.Connector), "stage", t.Stage,
			"worker", body.WorkerID, "token", t.LeaseToken)
	}
	JSON(w, http.StatusOK, answer)
}

func (h taskHandlers) heartbeat(w http.ResponseWriter, r *http.Request) {
	var body HeartbeatRequest
	if !decode(w, r, &body) {
		return
	}
	lease, ok := leaseOf(w, body.LeaseSeconds)
	if !ok {
		return
	}
	task, err := h.store.Heartbeat(r.Context(), r.PathValue("id"), body.Token, lease)
	if h.leaseRefused(w, r, err) {
		return
	}
	JSON(w, http.StatusOK, viewOf(task, h.store.Now()))
}

func (h taskHandlers) report(w http.ResponseWriter, r *http.Request) {
	var body ReportRequest
	if !decode(w, r, &body) {
		return
	}
	outcome := store.Outcome{Error: body.Error}
	switch body.Outcome {
	case OutcomeDone:
		outcome.State = store.TaskDone
	case OutcomeFailed:
		outcome.State = store.TaskFailed
	case OutcomeRetry:
		outcome.State = store.TaskQueued
		outcome.RetryAfter = time.Duration(body.RetryAfterSeconds) * time.Second
	default:
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "outcome is done, failed or retry, not %q", body.Outcome)
		return
	}
	task, err := h.store.Report(r.Context(), r.PathValue("id"), body.Token, outcome, func(tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil && len(body.Outputs) > 0 {
			return errOutputsWithoutItem
		}
		for _, o := range body.Outputs {
			if err := tx.PutOutputWithUnits(r.Context(), o.output(task), o.Units); err != nil {
				return err
			}
		}
		if d := body.Delivery; d != nil {
			if task.Stage != store.StageDeliver || task.Destination == nil {
				return errDeliveryOutsideDeliver
			}
			if err := tx.RecordDelivery(r.Context(), task, store.DeliveryAttempt{
				Outcome: d.Outcome, DestinationID: d.DestinationID, Error: d.Error,
			}); err != nil {
				return fmt.Errorf("%w: %w", errBadDelivery, err)
			}
		}
		if h.after != nil && outcome.State == store.TaskDone && task.ItemKey != nil {
			return h.after(r.Context(), tx, task)
		}
		return nil
	})
	if h.leaseRefused(w, r, err) {
		return
	}
	h.log.Info("a task ended", "task", task.ID, "item", text(task.ItemKey), "connector", text(task.Connector), "stage", task.Stage,
		"state", task.State, "outputs", len(body.Outputs))
	JSON(w, http.StatusOK, viewOf(task, h.store.Now()))
}

// retry queues a failed task again for a person. A model stage's task whose
// run failed then dispatches again, and one whose run waits follows it.
func (h taskHandlers) retry(w http.ResponseWriter, r *http.Request) {
	task, err := h.store.RetryTask(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNoTask):
		Refuse(w, http.StatusNotFound, CodeTaskNotFound, "no task %s", r.PathValue("id"))
		return
	case errors.Is(err, store.ErrTaskDone):
		Refuse(w, http.StatusConflict, CodeTaskDone, "task %s ended done, so nothing is retried: items replay runs its stage again", r.PathValue("id"))
		return
	case err != nil:
		h.internal(w, r, "retry a task", err)
		return
	}
	h.log.Info("a task was queued again", "task", task.ID, "item", text(task.ItemKey), "stage", task.Stage)
	JSON(w, http.StatusOK, TaskRetryAnswer{SchemaVersion: 1, Task: viewOf(task, h.store.Now())})
}

// TaskRetryAnswer is what POST /api/v1/tasks/{id}/retries answers:
// schemas/task-retry.schema.json.
type TaskRetryAnswer struct {
	SchemaVersion int      `json:"schema_version"`
	Task          TaskView `json:"task"`
}

// parse answers the newest done parse of the item whose task the caller
// leases, under the lease's token in the token query, so a worker reads the
// input of a task it holds and of no other.
func (h taskHandlers) parse(w http.ResponseWriter, r *http.Request) {
	token, err := strconv.ParseInt(r.URL.Query().Get("token"), 10, 64)
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "token is the lease token of the task, not %q", r.URL.Query().Get("token"))
		return
	}
	task, err := h.store.Leased(r.Context(), r.PathValue("id"), token)
	if h.leaseRefused(w, r, err) {
		return
	}
	if task.ItemKey == nil {
		Refuse(w, http.StatusNotFound, CodeParseNotFound, "task %s is for no item, so it has no parse", task.ID)
		return
	}
	p, err := h.store.Parse(r.Context(), *task.ItemKey, store.ParsePick{})
	switch {
	case errors.Is(err, store.ErrNoParse):
		Refuse(w, http.StatusNotFound, CodeParseNotFound, "%s holds no done parse", *task.ItemKey)
		return
	case err != nil:
		h.internal(w, r, "read a task's parse", err)
		return
	}
	units := p.Units
	if units == nil {
		units = []store.Unit{}
	}
	JSON(w, http.StatusOK, TaskParseAnswer{
		SchemaVersion: 1, Key: p.Item.Key, Extractor: p.Output.Producer, ExtractorVersion: p.Output.ProducerVersion, Units: units,
	})
}

// TaskParseAnswer is what GET /api/v1/tasks/{id}/parse answers:
// schemas/task-parse.schema.json. It carries the units alone, because the
// parse's whole text is the units joined.
type TaskParseAnswer struct {
	SchemaVersion    int          `json:"schema_version"`
	Key              string       `json:"key"`
	Extractor        string       `json:"extractor"`
	ExtractorVersion string       `json:"extractor_version"`
	Units            []store.Unit `json:"units"`
}

func (h taskHandlers) list(w http.ResponseWriter, r *http.Request) {
	q := store.TaskQuery{States: r.URL.Query()["state"]}
	for _, s := range q.States {
		switch s {
		case store.TaskQueued, store.TaskLeased, store.TaskDone, store.TaskFailed:
		default:
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "state is queued, leased, done or failed, not %q", s)
			return
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > MaxListed {
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "limit is from 1 to %d, not %q", MaxListed, raw)
			return
		}
		q.Limit = n
	}
	tasks, err := h.store.Tasks(r.Context(), q)
	if err != nil {
		h.internal(w, r, "list tasks", err)
		return
	}
	now := h.store.Now()
	since := store.Stamp(now.Add(-WorkersHeardWithin))
	switch r.URL.Query().Get("workers") {
	case "":
	case "all":
		since = ""
	default:
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "workers is all or absent, not %q", r.URL.Query().Get("workers"))
		return
	}
	workers, hidden, err := h.store.Workers(r.Context(), since)
	if err != nil {
		h.internal(w, r, "list workers", err)
		return
	}
	answer := TasksAnswer{Tasks: make([]TaskView, 0, len(tasks)), Workers: workers, WorkersHidden: hidden}
	if answer.Workers == nil {
		answer.Workers = []store.Worker{}
	}
	for _, t := range tasks {
		answer.Tasks = append(answer.Tasks, viewOf(t, now))
	}
	JSON(w, http.StatusOK, answer)
}

func viewOf(t store.Task, now time.Time) TaskView {
	return TaskView{
		ID: t.ID, ItemKey: t.ItemKey, Connector: t.Connector, Destination: t.Destination, Stage: t.Stage, Capability: t.Capability,
		State: t.State, HeldBy: t.HeldBy, Lapsed: t.Lapsed(now),
		LeaseHolder: t.LeaseHolder, LeaseExpiresTS: t.LeaseExpiresTS, Attempts: t.Attempts, HasError: t.Error != nil,
		NotBeforeTS: t.NotBeforeTS, CreatedTS: t.CreatedTS, UpdatedTS: t.UpdatedTS,
	}
}

// leaseRefused answers err's refusal and says whether it answered one: 404
// where the store holds no such task, 409 for a token that does not hold its
// lease, and 500 for any other failure.
func (h taskHandlers) leaseRefused(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNoTask):
		Refuse(w, http.StatusNotFound, CodeTaskNotFound, "no task %s", r.PathValue("id"))
	case errors.Is(err, store.ErrStaleLease):
		Refuse(w, http.StatusConflict, CodeStaleLease, "%v", err)
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusUnprocessableEntity, CodeItemNotFound, "%v", err)
	case errors.Is(err, errOutputsWithoutItem):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "task %s polls a connector, which carries no item, so it reports no outputs", r.PathValue("id"))
	case errors.Is(err, errDeliveryOutsideDeliver):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "task %s delivers nothing, so it reports no delivery", r.PathValue("id"))
	case errors.Is(err, errBadDelivery):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "%v", err)
	case errors.Is(err, store.ErrUnitsOutsideParse), errors.Is(err, store.ErrUnitsOutOfOrder):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "%v", err)
	default:
		h.internal(w, r, "update a task", err)
	}
	return true
}

func (h taskHandlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.log.ErrorContext(r.Context(), what, "error", err, "path", r.URL.Path)
	Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not %s", what)
}

func leaseOf(w http.ResponseWriter, seconds int) (time.Duration, bool) {
	if seconds == 0 {
		return defaultLease, true
	}
	lease := time.Duration(seconds) * time.Second
	if lease < minLease || lease > maxLease {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "lease_seconds is from %d to %d, not %d",
			int(minLease.Seconds()), int(maxLease.Seconds()), seconds)
		return 0, false
	}
	return lease, true
}

// decode reads r's JSON body into into, and says whether it could. An unknown
// field is refused with 422 naming it, apart from a body that does not parse,
// so a worker built ahead of the service can tell the two apart.
func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(into)
	if err == nil {
		return true
	}
	if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		field = strings.Trim(field, `"`)
		status := http.StatusUnprocessableEntity
		JSON(w, status, unknownField{
			refusal: refusal{
				Error: http.StatusText(status), Code: CodeUnknownField,
				Message: fmt.Sprintf("this route reads no field %s, so the caller may be built ahead of this service", field),
			},
			Field: field,
		})
		return false
	}
	Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the body is not one this route reads: %v", err)
	return false
}

// unknownField is the refusal of a body naming a field the route does not read.
type unknownField struct {
	refusal
	Field string `json:"field"`
}
