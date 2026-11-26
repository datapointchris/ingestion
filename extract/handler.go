package extract

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// blobRetry is how long a parse waits to read its bytes again after the
// service gave no answer.
const blobRetry = 30 * time.Second

// Handlers is a worker handler for each extractor of r that reads an item's
// bytes and runs here: each written in Go, and each whose program is
// installed. An extractor that ReadsURL runs through its source's handlers,
// as SourceHandler opens it. A worker claims only these extractors' tasks.
func (r *Registry) Handlers(client *worker.Client, log *slog.Logger) []worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	var out []worker.Handler
	for _, e := range r.all {
		if e.ReadsURL || !installed(e, log) {
			continue
		}
		out = append(out, handler{extractor: e, client: client})
	}
	return out
}

// SourceHandler is the worker handler of e, an extractor that ReadsURL, whose
// reader reaches its source at api through client. It is false where e's
// program is not installed here, so the worker claims none of its tasks.
func SourceHandler(e Extractor, api string, client *http.Client, log *slog.Logger) (worker.Handler, bool) {
	if log == nil {
		log = slog.Default()
	}
	if !installed(e, log) {
		return nil, false
	}
	return readHandler{extractor: e, read: e.Open(api, client)}, true
}

// installed says whether e runs here: it is written in Go, or its program is
// an executable file. It logs why one does not.
func installed(e Extractor, log *slog.Logger) bool {
	if e.Program == "" {
		return true
	}
	if info, err := os.Stat(e.Program); err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		log.Warn("an extractor's program is not installed here, so this worker claims none of its tasks",
			"extractor", e.Name, "program", e.Program)
		return false
	}
	return true
}

// handler runs one extractor's parse tasks: it reads the item's bytes through
// the service, extracts them, and reports the units.
type handler struct {
	extractor Extractor
	client    *worker.Client
}

func (h handler) Capability() string { return Capability(h.extractor) }

func (h handler) SlotLimit() int { return h.extractor.SlotLimit }

func (h handler) SlotGroup() (string, int) {
	return h.extractor.SlotGroup, SlotGroups[h.extractor.SlotGroup]
}

// Run parses the task's item. A parse that fails, or finds no text, fails
// its task with the reason kept, so the item waits in review rather than
// passing a gate on empty text. A blank unit keeps its place where the
// extractor is positional, and is dropped otherwise.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.BlobSHA256 == nil {
		return failed(h.extractor, "the item holds no bytes to parse")
	}
	body, _, err := h.client.Blob(ctx, *task.BlobSHA256)
	if err != nil {
		reason := fmt.Sprintf("read the item's bytes: %v", err)
		var refused *worker.StatusError
		if errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500 {
			return failed(h.extractor, reason)
		}
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: blobRetry}
	}
	defer func() { _ = body.Close() }()
	runCtx, cancel := bounded(ctx, h.extractor)
	defer cancel()
	units, err := h.extractor.Extract(runCtx, body)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return failed(h.extractor, fmt.Sprintf("%s ran past its %s limit", h.extractor.Name, h.extractor.Timeout))
	}
	if err != nil {
		return failed(h.extractor, err.Error())
	}
	return parsed(h.extractor, Read{Units: units})
}

// readHandler runs the parse tasks of an extractor that ReadsURL: it reads
// the item at its source, and reports the units, or a deferral.
type readHandler struct {
	extractor Extractor
	read      Reader
}

func (h readHandler) Capability() string { return Capability(h.extractor) }

func (h readHandler) SlotLimit() int { return h.extractor.SlotLimit }

func (h readHandler) SlotGroup() (string, int) {
	return h.extractor.SlotGroup, SlotGroups[h.extractor.SlotGroup]
}

// Run reads the task's item at its source. A read that its source asked to
// wait for queues again after that wait, spending no claim, until the task has
// waited ReadWaitCeiling. One that failed in a way a later read may not
// queues again until the task's claims reach ReadAttempts. Any other failure,
// such as the source refusing the item for good, fails the task with the
// source's refusal kept, so the item waits in review.
func (h readHandler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.ItemKey == nil {
		return failed(h.extractor, "the task names no item to read")
	}
	runCtx, cancel := bounded(ctx, h.extractor)
	defer cancel()
	read, err := h.read(runCtx, Item{Key: *task.ItemKey, URL: task.URL, MediaType: task.MediaType})
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return failed(h.extractor, fmt.Sprintf("%s ran past its %s limit", h.extractor.Name, h.extractor.Timeout))
	}
	var retry *RetryError
	if errors.As(err, &retry) {
		reason := retry.Error()
		waited := time.Duration(task.WaitedSeconds) * time.Second
		switch {
		case !retry.Bounded && waited < ReadWaitCeiling:
			return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: retry.After, Wait: true}
		case !retry.Bounded:
			return failed(h.extractor, fmt.Sprintf("%s, after waiting %s on the source in all", reason, waited.Round(time.Minute)))
		case task.Attempts < ReadAttempts:
			return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: retry.After}
		}
	}
	if err != nil {
		return failed(h.extractor, err.Error())
	}
	return parsed(h.extractor, read)
}

// parsed reports read as e's parse. Where read defers, the parse is reported
// deferred with no units. Otherwise a blank unit keeps its place where e is
// positional and is dropped where it is not, and a parse with no text fails.
func parsed(e Extractor, read Read) worker.Result {
	if read.Deferred {
		return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
			Producer: e.Name, ProducerVersion: e.Version, State: store.OutputDeferred, Answer: read.Answer,
		}}}
	}
	numbered := make([]store.Unit, 0, len(read.Units))
	texts := 0
	for _, u := range read.Units {
		blank := strings.TrimSpace(u.Text) == ""
		if !blank {
			texts++
		}
		if blank && !e.Positional {
			continue
		}
		numbered = append(numbered, store.Unit{N: len(numbered) + 1, Label: u.Label, Text: u.Text})
	}
	if texts == 0 {
		return failed(e, e.Name+" found no text in the item")
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: e.Name, ProducerVersion: e.Version, State: store.OutputDone, Units: numbered, Answer: read.Answer,
	}}}
}

// bounded is ctx ended at e's Timeout, where it sets one.
func bounded(ctx context.Context, e Extractor) (context.Context, context.CancelFunc) {
	if e.Timeout > 0 {
		return context.WithTimeout(ctx, e.Timeout)
	}
	return ctx, func() {}
}

// failed fails the task with reason, and records e's parse as failed with it.
func failed(e Extractor, reason string) worker.Result {
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{{
		Producer: e.Name, ProducerVersion: e.Version, State: store.OutputFailed, Error: &reason,
	}}}
}
