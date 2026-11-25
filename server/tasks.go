package server

import (
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
// report on it.
type LeasedTask struct {
	ID             string `json:"id"`
	ItemKey        string `json:"item_key"`
	Stage          string `json:"stage"`
	Capability     string `json:"capability"`
	Attempts       int    `json:"attempts"`
	LeaseToken     int64  `json:"lease_token"`
	LeaseExpiresTS string `json:"lease_expires_ts"`
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
// item, written in the same transaction that ends the lease.
type ReportRequest struct {
	Token             int64          `json:"token"`
	Outcome           string         `json:"outcome"`
	Error             *string        `json:"error,omitempty"`
	RetryAfterSeconds int            `json:"retry_after_seconds,omitempty"`
	Outputs           []ReportOutput `json:"outputs,omitempty"`
}

// ReportOutput is one output a worker reports: each field of an outputs row
// that the stage's work decides. The service takes the item and stage from
// the task and stamps the row, so a column the store adds for itself never
// changes what a worker sends.
type ReportOutput struct {
	Producer        string          `json:"producer"`
	ProducerVersion string          `json:"producer_version"`
	Lens            *string         `json:"lens,omitempty"`
	LensVersion     *string         `json:"lens_version,omitempty"`
	State           string          `json:"state"`
	BlobSHA256      *string         `json:"blob_sha256,omitempty"`
	DispatchID      *string         `json:"dispatch_id,omitempty"`
	RunID           *string         `json:"run_id,omitempty"`
	Tokens          *store.Tokens   `json:"tokens,omitempty"`
	Findings        []store.Finding `json:"findings,omitempty"`
	Quality         *int            `json:"quality,omitempty"`
	SideGuess       *string         `json:"side_guess,omitempty"`
	SideConfidence  *float64        `json:"side_confidence,omitempty"`
	Text            *string         `json:"text,omitempty"`
	Units           []store.Unit    `json:"units,omitempty"`
	Spans           []store.Span    `json:"spans,omitempty"`
	Answer          json.RawMessage `json:"answer,omitempty"`
	Error           *string         `json:"error,omitempty"`
}

// output is o as the row it writes for task.
func (o ReportOutput) output(task store.Task) store.Output {
	return store.Output{
		ItemKey: task.ItemKey, Stage: task.Stage, Producer: o.Producer, ProducerVersion: o.ProducerVersion,
		Lens: o.Lens, LensVersion: o.LensVersion, State: o.State, BlobSHA256: o.BlobSHA256,
		DispatchID: o.DispatchID, RunID: o.RunID, Tokens: o.Tokens, Findings: o.Findings, Quality: o.Quality,
		SideGuess: o.SideGuess, SideConfidence: o.SideConfidence, Text: o.Text, Units: o.Units, Spans: o.Spans,
		Answer: o.Answer, Error: o.Error,
	}
}

// TaskView is one task as the list shows it. It leaves out the lease token
// and the task's error, which can quote outsider text, and says in has_error
// whether one is kept.
type TaskView struct {
	ID             string  `json:"id"`
	ItemKey        string  `json:"item_key"`
	Stage          string  `json:"stage"`
	Capability     string  `json:"capability"`
	State          string  `json:"state"`
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

// TaskRoutes are the routes for a worker to lease tasks through, and the
// list for a person to read.
func TaskRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := taskHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle("POST /api/v1/tasks/claims", ScopeTasksClaim, h.claim)
		r.Handle("POST /api/v1/tasks/{id}/heartbeats", ScopeTasksClaim, h.heartbeat)
		r.Handle("POST /api/v1/tasks/{id}/reports", ScopeTasksClaim, h.report)
		r.Handle("GET /api/v1/tasks", "", h.list)
	}
}

type taskHandlers struct {
	store *store.Store
	log   *slog.Logger
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
		answer.Tasks = append(answer.Tasks, LeasedTask{
			ID: t.ID, ItemKey: t.ItemKey, Stage: t.Stage, Capability: t.Capability, Attempts: t.Attempts,
			LeaseToken: *t.LeaseToken, LeaseExpiresTS: *t.LeaseExpiresTS,
		})
	}
	for _, t := range answer.Tasks {
		h.log.Info("leased a task", "task", t.ID, "item", t.ItemKey, "stage", t.Stage, "worker", body.WorkerID, "token", t.LeaseToken)
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
		for _, o := range body.Outputs {
			if err := tx.PutOutput(r.Context(), o.output(task)); err != nil {
				return err
			}
		}
		return nil
	})
	if h.leaseRefused(w, r, err) {
		return
	}
	h.log.Info("a task ended", "task", task.ID, "item", task.ItemKey, "stage", task.Stage, "state", task.State, "outputs", len(body.Outputs))
	JSON(w, http.StatusOK, viewOf(task, h.store.Now()))
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
		ID: t.ID, ItemKey: t.ItemKey, Stage: t.Stage, Capability: t.Capability, State: t.State, Lapsed: t.Lapsed(now),
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
