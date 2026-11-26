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

// Handlers is a worker handler for each extractor of r that runs here: each
// written in Go, and each whose program is installed. A worker claims only
// these extractors' tasks.
func (r *Registry) Handlers(client *worker.Client, log *slog.Logger) []worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	var out []worker.Handler
	for _, e := range r.all {
		if e.Program != "" {
			if info, err := os.Stat(e.Program); err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
				log.Warn("an extractor's program is not installed here, so this worker claims none of its tasks",
					"extractor", e.Name, "program", e.Program)
				continue
			}
		}
		out = append(out, handler{extractor: e, client: client})
	}
	return out
}

// handler runs one extractor's parse tasks: it reads the item's bytes through
// the service, extracts them, and reports the units.
type handler struct {
	extractor Extractor
	client    *worker.Client
}

func (h handler) Capability() string { return Capability(h.extractor) }

// Run parses the task's item. A parse that fails, or finds no text, fails
// its task with the reason kept, so the item waits in review rather than
// passing a gate on empty text. A blank unit keeps its place where the
// extractor is positional, and is dropped otherwise.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.BlobSHA256 == nil {
		return h.failed("the item holds no bytes to parse")
	}
	body, _, err := h.client.Blob(ctx, *task.BlobSHA256)
	if err != nil {
		reason := fmt.Sprintf("read the item's bytes: %v", err)
		var refused *worker.StatusError
		if errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500 {
			return h.failed(reason)
		}
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: blobRetry}
	}
	defer func() { _ = body.Close() }()
	runCtx, cancel := ctx, context.CancelFunc(func() {})
	if h.extractor.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, h.extractor.Timeout)
	}
	defer cancel()
	units, err := h.extractor.Extract(runCtx, body)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return h.failed(fmt.Sprintf("%s ran past its %s limit", h.extractor.Name, h.extractor.Timeout))
	}
	if err != nil {
		return h.failed(err.Error())
	}
	numbered := make([]store.Unit, 0, len(units))
	texts := 0
	for _, u := range units {
		blank := strings.TrimSpace(u.Text) == ""
		if !blank {
			texts++
		}
		if blank && !h.extractor.Positional {
			continue
		}
		numbered = append(numbered, store.Unit{N: len(numbered) + 1, Label: u.Label, Text: u.Text})
	}
	if texts == 0 {
		return h.failed(h.extractor.Name + " found no text in the item's bytes")
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: h.extractor.Name, ProducerVersion: h.extractor.Version, State: store.OutputDone, Units: numbered,
	}}}
}

// failed fails the task with reason, and records the parse as failed with it.
func (h handler) failed(reason string) worker.Result {
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{{
		Producer: h.extractor.Name, ProducerVersion: h.extractor.Version, State: store.OutputFailed, Error: &reason,
	}}}
}
