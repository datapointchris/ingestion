package scan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// Capability is the capability of every scan task.
	Capability = store.StageScan
	// Timeout bounds one scan of one parse. A scan past it fails its task.
	Timeout = 10 * time.Minute
	// parseRetry is how long a scan waits to read its parse again after the
	// service gave no answer.
	parseRetry = 30 * time.Second
)

// Handlers is the scan's worker handler where yr and the built rules are
// installed here, and none where either is missing, so such a worker claims
// no scan task.
func Handlers(client *worker.Client, log *slog.Logger) []worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	scanner, err := Open(Yr, RulesDir)
	if err != nil {
		log.Warn("the scanner is not installed here, so this worker claims no scan task", "error", err)
		return nil
	}
	log.Info("the worker scans", "rules", scanner.Version())
	return []worker.Handler{NewHandler(scanner, client, log)}
}

// NewHandler is the worker handler that scans with scanner, reading each
// task's parse through client.
func NewHandler(scanner *Scanner, client *worker.Client, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	return handler{scanner: scanner, client: client, log: log}
}

type handler struct {
	scanner *Scanner
	client  *worker.Client
	log     *slog.Logger
}

func (h handler) Capability() string { return Capability }

// Run scans the newest done parse of the task's item, and reports each
// finding beside the parse it read. A finding never fails the task or drops
// the item. Where the item holds no done parse yet, the task ends with
// nothing scanned, because the parse's own report queues a scan once it is
// done.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken)
	var refused *worker.StatusError
	switch {
	case errors.As(err, &refused) && refused.Code == server.CodeParseNotFound:
		h.log.Info("the item holds no done parse, so nothing was scanned", "task", task.ID, "item", task.ItemKey)
		return worker.Result{Outcome: server.OutcomeDone}
	case errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500:
		return h.failed(fmt.Sprintf("read the item's parse: %v", err), nil)
	case err != nil:
		reason := fmt.Sprintf("read the item's parse: %v", err)
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: parseRetry}
	}
	runCtx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	findings, spans, err := h.scanner.ScanUnits(runCtx, parse.Units)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return h.failed(fmt.Sprintf("the scan ran past its %s limit", Timeout), &parse)
	}
	if err != nil {
		return h.failed(err.Error(), &parse)
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: Producer, ProducerVersion: h.scanner.Version(), State: store.OutputDone,
		InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion, Findings: findings, Spans: spans,
	}}}
}

// failed fails the task with reason, and records the scan as failed with it
// beside the parse it read, where it read one.
func (h handler) failed(reason string, parse *server.ParseAnswer) worker.Result {
	out := server.ReportOutput{Producer: Producer, ProducerVersion: h.scanner.Version(), State: store.OutputFailed, Error: &reason}
	if parse != nil {
		out.InputProducer, out.InputVersion = &parse.Extractor, &parse.ExtractorVersion
	}
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{out}}
}

// QueueAfterParse queues a scan of the item in the write that ends its parse
// task, where the parse reported done. So every parsed item is scanned, and a
// parse that failed waits in review unscanned.
func QueueAfterParse(ctx context.Context, tx *store.Tx, task store.Task, outputs []store.Output) error {
	if task.Stage != store.StageParse {
		return nil
	}
	for _, o := range outputs {
		if o.State == store.OutputDone {
			_, err := tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageScan, Capability: Capability})
			return err
		}
	}
	return nil
}

// Replayable is the scan as a replay runs it again, such as over a new rule
// set: a task for each item whose media type one of extractors reads. An item
// whose parse is not done yet ends its scan with nothing scanned.
func Replayable(extractors *extract.Registry) server.Replayable {
	return server.Replayable{Capability: func(item store.Item) (string, bool) {
		_, ok := extractors.For(item.MediaType)
		return Capability, ok
	}}
}
