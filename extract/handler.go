package extract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/publicweb"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// blobRetry is how long a parse waits to read its bytes again after the
// service gave no answer.
const blobRetry = 30 * time.Second

// pageRetry is how long a parse waits to GET its page again after a refusal
// that may clear, such as a 503, so that a site's brief outage or rate limit
// has likely passed.
const pageRetry = 15 * time.Minute

// Handlers is a worker handler for each extractor of r that reads an item's
// bytes and runs here: each written in Go, and each whose program is
// installed. The extractor that FetchesPage GETs a page through pages, which
// answers a page of each type that an extractor TakesFetchedPages for too. An
// extractor that ReadsURL runs through its source's handlers, as
// SourceHandler opens it. A worker claims only these extractors' tasks.
func (r *Registry) Handlers(client *worker.Client, pages publicweb.Reader, log *slog.Logger) []worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	var out []worker.Handler
	for _, e := range r.all {
		if e.ReadsURL || !installed(e, log) {
			continue
		}
		out = append(out, handler{extractor: e, client: client, pages: pages.Accepting(r.fetchedTypes...), handsOff: r.fetchedTypes})
	}
	return out
}

// mayClaimAgain says whether a read that may clear queues task again, which it
// does until the task's claims reach ReadAttempts.
func mayClaimAgain(task worker.Task) bool { return task.Attempts < ReadAttempts }

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
// the service, or fetches its page through pages, extracts them, and reports
// the units.
type handler struct {
	extractor Extractor
	client    *worker.Client
	pages     publicweb.Reader
	// handsOff are the media types of a fetched page whose parse is another
	// extractor's.
	handsOff []string
}

func (h handler) Capability() string { return Capability(h.extractor) }

func (h handler) SlotLimit() int { return h.extractor.SlotLimit }

func (h handler) SlotGroup() (string, int) {
	return h.extractor.SlotGroup, SlotGroups[h.extractor.SlotGroup]
}

// Run parses the task's item. Where the item holds no bytes and the
// extractor FetchesPage, it first fetches the item's page, and the result
// names the page that it kept whatever the parse's outcome. A fetched page of
// a type that another extractor takes is not read here. The parse ends
// deferred, and the service hands the item to that extractor's parse. A
// parse that fails, or finds no text, fails its task with the reason kept, so
// the item waits in review rather than passing a gate on empty text. A blank
// unit keeps its place where the extractor is positional, and is dropped
// otherwise.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	var body io.Reader
	var page *server.ReportPage
	switch {
	case task.BlobSHA256 != nil:
		blob, _, err := h.client.Blob(ctx, *task.BlobSHA256)
		if err != nil {
			return h.unreachedService("read the item's bytes", err)
		}
		defer func() { _ = blob.Close() }()
		body = blob
	case h.extractor.FetchesPage && task.URL != nil:
		fetched, kept, stopped := h.fetch(ctx, task)
		if stopped != nil {
			return *stopped
		}
		if kept.MediaType != "" {
			result := parsed(h.extractor, Read{Deferred: true})
			result.Page = &kept
			return result
		}
		body, page = bytes.NewReader(fetched), &kept
	default:
		return failed(h.extractor, "the item holds no bytes to parse")
	}
	result := h.extract(ctx, body)
	result.Page = page
	return result
}

// extract runs the extractor over body, and reports its parse.
func (h handler) extract(ctx context.Context, body io.Reader) worker.Result {
	runCtx, cancel := bounded(ctx, h.extractor)
	defer cancel()
	read, err := h.extractor.Extract(runCtx, body)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return failed(h.extractor, fmt.Sprintf("%s ran past its %s limit", h.extractor.Name, h.extractor.Timeout))
	}
	if err != nil {
		return failed(h.extractor, err.Error())
	}
	return parsed(h.extractor, read)
}

// fetch GETs the page at the task's URL through h.pages, and keeps it through
// the service's blob route. It answers the page's bytes and the page as the
// report names it, or the result that ends the parse in its place. The report
// names an HTML page's own title, and the type of a page that another
// extractor takes. A refusal that may clear queues the parse again after
// pageRetry, while mayClaimAgain holds. Any other refusal fails the parse with
// the refusal's reason.
func (h handler) fetch(ctx context.Context, task worker.Task) ([]byte, server.ReportPage, *worker.Result) {
	body, mediaType, refused := h.pages.Read(ctx, *task.URL)
	if refused != nil {
		reason := "the page was not fetched: " + refused.Reason
		if refused.MayClear && mayClaimAgain(task) {
			return nil, server.ReportPage{}, &worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: pageRetry}
		}
		result := failed(h.extractor, reason)
		return nil, server.ReportPage{}, &result
	}
	sum, err := worker.Keep(ctx, h.client, mediaType, body)
	if err != nil {
		result := h.unreachedService("keep the page", err)
		return nil, server.ReportPage{}, &result
	}
	page := server.ReportPage{BlobSHA256: sum}
	if base, _, _ := mime.ParseMediaType(mediaType); slices.Contains(h.handsOff, base) {
		page.MediaType = base
	} else {
		page.Title = publicweb.PageTitle(body)
	}
	return body, page, nil
}

// unreachedService ends a parse whose call to the service, doing what, failed
// with err. A refusal that a later call would meet again fails the parse, and
// any other call queues it again after blobRetry.
func (h handler) unreachedService(what string, err error) worker.Result {
	reason := fmt.Sprintf("%s: %v", what, err)
	var refused *worker.StatusError
	if errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500 {
		return failed(h.extractor, reason)
	}
	return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: blobRetry}
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
		case mayClaimAgain(task):
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
