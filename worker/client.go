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
	"strings"

	"github.com/datapointchris/ingestion/server"
)

// ErrStaleLease is the service refusing a heartbeat or report, because the
// lease lapsed and a later claim took the task.
var ErrStaleLease = errors.New("the service refused the lease's token")

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

func (c *Client) post(ctx context.Context, path string, body, into any) error {
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
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return fmt.Errorf("read the answer to POST %s: %w", path, err)
	}
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("POST %s: %w: %s", path, ErrStaleLease, raw)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s answered %d: %s", path, resp.StatusCode, raw)
	}
	if into == nil {
		return nil
	}
	return json.Unmarshal(raw, into)
}
