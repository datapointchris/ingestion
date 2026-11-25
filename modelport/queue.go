package modelport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/store"
)

// DefaultAgent is the agent a queue dispatch names where config names none:
// one session with no tools that answers a caller's prompt to the caller's
// schema.
const DefaultAgent = "outside-text"

// queue dispatches each call to an HTTP dispatch service and follows it.
//
// A dispatch is POST /api/v1/dispatches/ naming the agent, the inputs prompt
// and schema, and the model and effort, under the request's Idempotency-Key.
// The service answers the dispatch, whose finished stamp says its run ended
// and whose waiting says why the run has not started. GET
// /api/v1/dispatches/{id}/ reads it again. Once it is finished, GET
// /api/v1/outside-text-answers/{id} answers the run: succeeded with
// structured_output, or failed with a failure code. A finished dispatch
// holding no answer never ran, and its dropped or refused says why.
type queue struct {
	api    string
	agent  string
	client *http.Client
}

// NewQueue is the queue adapter, dispatching agent on the service at api
// through client, which carries the service's token.
func NewQueue(api, agent string, client *http.Client) Port {
	if agent == "" {
		agent = DefaultAgent
	}
	return queue{api: strings.TrimRight(api, "/"), agent: agent, client: client}
}

func (queue) Adapter() string { return AdapterQueue }

// dispatchBody is what a dispatch sends.
type dispatchBody struct {
	Agent  string         `json:"agent"`
	Inputs map[string]any `json:"inputs"`
	Model  string         `json:"model,omitempty"`
	Effort string         `json:"effort,omitempty"`
}

// dispatch is the part of the service's dispatch that a caller reads.
type dispatch struct {
	ID       string    `json:"id"`
	Finished time.Time `json:"finished"`
	Waiting  string    `json:"waiting"`
	Held     string    `json:"held"`
	Refused  string    `json:"refused"`
	Dropped  []string  `json:"dropped"`
}

// storedAnswer is the part of the service's stored answer that a caller reads.
type storedAnswer struct {
	Run              string          `json:"run"`
	State            string          `json:"state"`
	Model            string          `json:"model"`
	Effort           string          `json:"effort"`
	Failure          string          `json:"failure"`
	FailureDetail    string          `json:"failure_detail"`
	StructuredOutput json.RawMessage `json:"structured_output"`
	Tokens           *struct {
		Input      int64 `json:"input"`
		CacheWrite int64 `json:"cache_write"`
		CacheRead  int64 `json:"cache_read"`
		Output     int64 `json:"output"`
	} `json:"tokens"`
}

func (q queue) Start(ctx context.Context, r Request) (Progress, error) {
	var schema any
	if err := json.Unmarshal(r.Schema, &schema); err != nil {
		return Progress{}, refused("the %s stage's schema is not JSON: %v", r.Stage, err)
	}
	body, err := json.Marshal(dispatchBody{
		Agent: q.agent, Inputs: map[string]any{"prompt": r.Prompt, "schema": schema}, Model: r.Model, Effort: r.Effort,
	})
	if err != nil {
		return Progress{}, err
	}
	var d dispatch
	err = q.call(ctx, http.MethodPost, "/api/v1/dispatches/", r.IdempotencyKey(), body, &d)
	if status := (*StatusError)(nil); errors.As(err, &status) && status.Status == http.StatusNotFound {
		err = fmt.Errorf("%w: %w", ErrRefused, status)
	}
	if err != nil {
		return Progress{}, fmt.Errorf("dispatch the %s stage of %s: %w", r.Stage, r.ItemKey, err)
	}
	if d.ID == "" {
		return Progress{}, errors.New("the dispatch service answered a dispatch with no id")
	}
	return q.progress(ctx, d)
}

func (q queue) Follow(ctx context.Context, id string) (Progress, error) {
	var d dispatch
	err := q.call(ctx, http.MethodGet, "/api/v1/dispatches/"+url.PathEscape(id)+"/", "", nil, &d)
	if status := (*StatusError)(nil); errors.As(err, &status) && status.Status == http.StatusNotFound {
		return Progress{}, refused("the dispatch service holds no dispatch %s", id)
	}
	if err != nil {
		return Progress{}, fmt.Errorf("read dispatch %s: %w", id, err)
	}
	if d.ID == "" {
		d.ID = id
	}
	return q.progress(ctx, d)
}

// progress is where d stands: waiting while its run has not ended, and the
// run's answer once it has.
func (q queue) progress(ctx context.Context, d dispatch) (Progress, error) {
	p := Progress{Dispatch: d.ID, Waiting: d.Waiting}
	if p.Waiting == "" {
		p.Waiting = d.Held
	}
	if d.Finished.IsZero() {
		return p, nil
	}
	var a storedAnswer
	err := q.call(ctx, http.MethodGet, "/api/v1/outside-text-answers/"+url.PathEscape(d.ID), "", nil, &a)
	var status *StatusError
	if errors.As(err, &status) && status.Status == http.StatusNotFound {
		reasons := append([]string{}, d.Dropped...)
		if d.Refused != "" {
			reasons = append(reasons, d.Refused)
		}
		detail := "the dispatch ended with no run and named no reason"
		if len(reasons) > 0 {
			detail = strings.Join(reasons, "; ")
		}
		p.Answer = &Answer{State: Failed, Failure: NotRun, Detail: detail}
		return p, nil
	}
	if err != nil {
		return Progress{}, fmt.Errorf("read the answer to dispatch %s: %w", d.ID, err)
	}
	answer := &Answer{Run: a.Run, State: a.State, Failure: a.Failure, Detail: a.FailureDetail, Model: a.Model, Effort: a.Effort}
	if a.Tokens != nil {
		answer.Tokens = &store.Tokens{UncachedInput: a.Tokens.Input, CacheWrite: a.Tokens.CacheWrite, CacheRead: a.Tokens.CacheRead, Output: a.Tokens.Output}
	}
	switch a.State {
	case Succeeded:
		answer.Output = a.StructuredOutput
	case Failed:
	default:
		return Progress{}, fmt.Errorf("the answer to dispatch %s is %q, neither succeeded nor failed", d.ID, a.State)
	}
	p.Answer = answer
	return p, nil
}

// StatusError is a dispatch service's answer other than success, with its
// message.
type StatusError struct {
	Status  int
	Message string
}

func (e *StatusError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("the dispatch service answered %d", e.Status)
	}
	return fmt.Sprintf("the dispatch service answered %d: %s", e.Status, e.Message)
}

// call sends one request and decodes its answer into into. It marks a 4xx
// other than 404, 408, 409 and 429 ErrRefused, because sending it again meets
// the same answer. A 409 is an identical call already queued or running,
// which a later try finds settled.
func (q queue) call(ctx context.Context, method, path, idempotencyKey string, body []byte, into any) error {
	var reader io.Reader = http.NoBody
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, q.api+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var refusal struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &refusal)
		status := &StatusError{Status: resp.StatusCode, Message: refusal.Message}
		switch {
		case resp.StatusCode == http.StatusNotFound:
			return status
		case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout &&
			resp.StatusCode != http.StatusConflict && resp.StatusCode != http.StatusTooManyRequests:
			return fmt.Errorf("%w: %w", ErrRefused, status)
		}
		return status
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("the dispatch service's answer is not JSON: %w", err)
	}
	return nil
}
