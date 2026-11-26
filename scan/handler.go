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
	// Timeout bounds one scan of one parse, and a scan past it runs again. A
	// 61 MiB parse, about the most a parse report carries, scanned in five
	// and a half minutes, and in under nine where its text held no line break.
	Timeout = 10 * time.Minute
	// Attempts bounds the claims of one scan task. A scan whose parse read
	// or yr run fails is queued again until its task's claims reach it, and
	// then fails.
	Attempts = 3
	// retryAfter is how long a scan waits to run again after one failed.
	retryAfter = 30 * time.Second
)

// Installed is the scanner over the yr and the built rules that the image
// installs, and nil where either is missing here. A worker with no scanner
// claims no scan task and no model read's task, because a model read's
// suggestion is scanned before it is kept.
func Installed(log *slog.Logger) *Scanner {
	if log == nil {
		log = slog.Default()
	}
	scanner, err := Open(Yr, RulesDir)
	if err != nil {
		log.Warn("the scanner is not installed here, so this worker claims no scan task and no model read's task", "error", err)
		return nil
	}
	log.Info("the worker scans", "rules", scanner.Version())
	return scanner
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
// done. A read that the service refuses fails the task. A read that it gave
// no answer to, or a scan that fails, runs again until the task's claims
// reach Attempts.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken, store.ParsePick{})
	var refused *worker.StatusError
	switch {
	case errors.As(err, &refused) && refused.Code == server.CodeParseNotFound:
		h.log.Info("the task's item holds no done parse, so nothing was scanned", "task", task.ID)
		return worker.Result{Outcome: server.OutcomeDone}
	case errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500:
		reason := fmt.Sprintf("read the item's parse: %v", err)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	case err != nil:
		return h.failed(task, fmt.Sprintf("read the item's parse: %v", err), nil)
	}
	runCtx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	findings, spans, err := h.scanner.ScanUnits(runCtx, parse.Units)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("the scan ran past its %s limit", Timeout)
	}
	if err != nil {
		return h.failed(task, err.Error(), &parse)
	}
	rules := h.scanner.Version()
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: Producer, ProducerVersion: store.ScanVersion(rules, parse.Extractor, parse.ExtractorVersion),
		State: store.OutputDone, InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion,
		Findings: findings, Spans: spans, RulesVersion: &rules,
	}}}
}

// failed queues the task again with reason while its claims are under
// Attempts, and otherwise fails it. A failed task records the scan as failed
// beside the parse it read, where it read one, and a done scan of that parse
// keeps its row.
func (h handler) failed(task worker.Task, reason string, parse *server.TaskParseAnswer) worker.Result {
	if task.Attempts < Attempts {
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: retryAfter}
	}
	result := worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	if parse != nil {
		result.Outputs = []server.ReportOutput{{
			Producer: Producer, ProducerVersion: store.ScanVersion(h.scanner.Version(), parse.Extractor, parse.ExtractorVersion),
			State: store.OutputFailed, Error: &reason, InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion,
		}}
	}
	return result
}

// QueueAfterParse queues a scan of the item in the write that ends its parse
// task done. So every parsed item is scanned. A parse that failed ends its
// task failed, so it waits in review unscanned.
func QueueAfterParse(ctx context.Context, tx *store.Tx, task store.Task) error {
	_, err := tx.Enqueue(ctx, store.Task{ItemKey: task.ItemKey, Stage: store.StageScan, Capability: Capability})
	return err
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
