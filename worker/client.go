package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// ErrStaleLease is the service refusing a heartbeat or report, because the
// lease lapsed and a later claim took the task.
var ErrStaleLease = errors.New("the service refused the lease's token")

// StatusError is the service refusing a call, other than a task call's 409.
type StatusError struct {
	Method string
	Path   string
	Status int
	// Code is the refusal's code, where its body carries one.
	Code string
	Body string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s answered %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// callTimeout bounds each call, so a service that takes the connection and
// never answers cannot hold a claim, a heartbeat or a report.
const callTimeout = 30 * time.Second

// Client calls the service's task routes.
type Client struct {
	base string
	http *http.Client
}

// NewClient is a client for the service at base, sending each request through
// hc, which carries the worker's token.
func NewClient(base string, hc *http.Client) *Client {
	return &Client{base: strings.TrimRight(base, "/"), http: hc}
}

// Claim asks for tasks.
func (c *Client) Claim(ctx context.Context, req server.ClaimRequest) (server.ClaimAnswer, error) {
	var answer server.ClaimAnswer
	err := c.post(ctx, "/api/v1/tasks/claims", req, &answer)
	return answer, err
}

// Heartbeat extends the lease on task id.
func (c *Client) Heartbeat(ctx context.Context, id string, req server.HeartbeatRequest) error {
	return c.post(ctx, "/api/v1/tasks/"+url.PathEscape(id)+"/heartbeats", req, nil)
}

// Report ends the lease on task id.
func (c *Client) Report(ctx context.Context, id string, req server.ReportRequest) error {
	return c.post(ctx, "/api/v1/tasks/"+url.PathEscape(id)+"/reports", req, nil)
}

// TaskParse reads the newest done parse that pick names of the item that task
// id is for, and the newest of any extractor where pick is empty. The worker
// names its lease by token, and the service answers only while that token
// holds the lease, refusing with ErrStaleLease otherwise. A task whose item
// holds no such parse answers a StatusError coded parse_not_found.
func (c *Client) TaskParse(ctx context.Context, id string, token int64, pick store.ParsePick) (server.TaskParseAnswer, error) {
	ctx, cancel := context.WithTimeout(ctx, blobTimeout)
	defer cancel()
	path := "/api/v1/tasks/" + url.PathEscape(id) + "/parse"
	query := url.Values{"token": {strconv.FormatInt(token, 10)}}
	if pick.Extractor != "" {
		query.Set("extractor", pick.Extractor)
	}
	if pick.Version != "" {
		query.Set("version", pick.Version)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+query.Encode(), http.NoBody)
	if err != nil {
		return server.TaskParseAnswer{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return server.TaskParseAnswer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusConflict:
		return server.TaskParseAnswer{}, fmt.Errorf("GET %s: %w", path, ErrStaleLease)
	default:
		return server.TaskParseAnswer{}, refusal(http.MethodGet, path, resp)
	}
	var answer server.TaskParseAnswer
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		return answer, fmt.Errorf("read the answer to GET %s: %w", path, err)
	}
	// A service that predates the pick answers its newest parse, which reads
	// like the one asked for.
	if (pick.Extractor != "" && answer.Extractor != pick.Extractor) || (pick.Version != "" && answer.ExtractorVersion != pick.Version) {
		return server.TaskParseAnswer{}, fmt.Errorf("GET %s answered the parse by %s %s rather than the one it asked for, so the service may predate the pick",
			path, answer.Extractor, answer.ExtractorVersion)
	}
	return answer, nil
}

// QuotedParse reads the parse that task's claim named as the one its stage's
// prompt quotes, so the call reads the parse that its output names. It fails
// where the claim named none, as where the item holds no done parse that the
// stage quotes, and the task tries again after its backoff.
func (c *Client) QuotedParse(ctx context.Context, task Task) (server.TaskParseAnswer, error) {
	if task.Parse == nil {
		return server.TaskParseAnswer{}, fmt.Errorf("the claim of the %s task %s named no parse that its prompt quotes", task.Stage, task.ID)
	}
	return c.TaskParse(ctx, task.ID, task.LeaseToken, store.ParsePick{Extractor: task.Parse.Extractor, Version: task.Parse.Version})
}

// blobTimeout bounds one blob's transfer, which may be a whole book.
const blobTimeout = 10 * time.Minute

// Blob opens the bytes named sum, and answers their media type. The caller
// closes the body.
func (c *Client) Blob(ctx context.Context, sum string) (io.ReadCloser, string, error) {
	ctx, cancel := context.WithTimeout(ctx, blobTimeout)
	path := "/api/v1/blobs/" + url.PathEscape(sum)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, http.NoBody)
	if err != nil {
		cancel()
		return nil, "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		defer cancel()
		defer func() { _ = resp.Body.Close() }()
		return nil, "", refusal(http.MethodGet, path, resp)
	}
	return cancelOnClose{ReadCloser: resp.Body, cancel: cancel}, resp.Header.Get("Content-Type"), nil
}

// PutBlob writes r's bytes under sum as mediaType. The service refuses bytes
// whose SHA-256 is not sum, and answers the row it holds for the same bytes
// sent again.
func (c *Client) PutBlob(ctx context.Context, sum, mediaType string, r io.Reader) (server.BlobAnswer, error) {
	ctx, cancel := context.WithTimeout(ctx, blobTimeout)
	defer cancel()
	path := "/api/v1/blobs/" + url.PathEscape(sum)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+path, r)
	if err != nil {
		return server.BlobAnswer{}, err
	}
	req.Header.Set("Content-Type", mediaType)
	resp, err := c.http.Do(req)
	if err != nil {
		return server.BlobAnswer{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return server.BlobAnswer{}, refusal(http.MethodPut, path, resp)
	}
	var answer server.BlobAnswer
	err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&answer)
	return answer, err
}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	defer c.cancel()
	return c.ReadCloser.Close()
}

// refusal is resp's refusal as a StatusError carrying its code.
func refusal(method, path string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	refused := &StatusError{Method: method, Path: path, Status: resp.StatusCode, Body: string(raw)}
	var answer struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(raw, &answer) == nil {
		refused.Code = answer.Code
	}
	return refused
}

func (c *Client) post(ctx context.Context, path string, body, into any) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return refusal(http.MethodPost, path, resp)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read the answer to POST %s: %w", path, err)
	}
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("POST %s: %w: %s", path, ErrStaleLease, raw)
	}
	if into == nil {
		return nil
	}
	return json.Unmarshal(raw, into)
}
