package freshrss

import (
	"bufio"
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
	"sync"
	"time"
)

// apiTimeout bounds one call to the API.
const apiTimeout = time.Minute

// maxAnswerBytes bounds one answer that the client reads. A page of the
// reading list holds pageSize entries, and an answer past the bound fails the
// read.
const maxAnswerBytes = 64 << 20

// readingList is the stream of every entry in the person's subscriptions.
const readingList = "user/-/state/com.google/reading-list"

// StatusError is an answer that the API refused.
type StatusError struct {
	Status int
	// Message is the start of the answer's body, which FreshRSS writes as a
	// short sentence of its own.
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("FreshRSS answered %d: %s", e.Status, e.Message)
}

// SignedIn is the client that reaches the Google Reader API at api as login.
// It signs in through ClientLogin with password before its first call, and
// carries the token that the sign-in answers on each call after it, as
// `Authorization: GoogleLogin auth=<token>`. Where a call answers 401, it
// signs in again once and repeats the call.
func SignedIn(api, login, password string) *http.Client {
	return &http.Client{Timeout: apiTimeout, Transport: &signIn{
		api: strings.TrimSuffix(api, "/"), login: login, password: password, base: http.DefaultTransport,
	}}
}

type signIn struct {
	api, login, password string
	base                 http.RoundTripper

	mu    sync.Mutex
	token string
}

func (s *signIn) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := s.signedIn(req.Context(), "")
	if err != nil {
		return nil, err
	}
	resp, err := s.base.RoundTrip(withToken(req, token))
	if err != nil || resp.StatusCode != http.StatusUnauthorized || (req.Body != nil && req.Body != http.NoBody) {
		return resp, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	_ = resp.Body.Close()
	if token, err = s.signedIn(req.Context(), token); err != nil {
		return nil, err
	}
	return s.base.RoundTrip(withToken(req, token))
}

// signedIn is the token to call with. It signs in where the client holds
// none, or where it still holds refused, the token that a call answered 401.
func (s *signIn) signedIn(ctx context.Context, refused string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && s.token != refused {
		return s.token, nil
	}
	form := url.Values{"Email": {s.login}, "Passwd": {s.password}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.api+"/accounts/ClientLogin", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.base.RoundTrip(req)
	if err != nil {
		return "", fmt.Errorf("sign in to FreshRSS: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return "", fmt.Errorf("read FreshRSS's sign-in answer: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return "", fmt.Errorf("FreshRSS refused the API password of %s: %w", s.login, refusal(resp.StatusCode, body))
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("sign in to FreshRSS as %s: %w", s.login, refusal(resp.StatusCode, body))
	}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		if token, ok := strings.CutPrefix(scanner.Text(), "Auth="); ok && token != "" {
			s.token = token
			return token, nil
		}
	}
	return "", errors.New("FreshRSS's sign-in answer holds no Auth line")
}

func withToken(req *http.Request, token string) *http.Request {
	out := req.Clone(req.Context())
	out.Header.Set("Authorization", "GoogleLogin auth="+token)
	return out
}

func refusal(status int, body []byte) *StatusError {
	message := strings.TrimSpace(string(body[:min(len(body), 200)]))
	return &StatusError{Status: status, Message: message}
}

// Client calls the Google Reader API at its base.
type Client struct {
	api  string
	http *http.Client
}

// NewClient is the client of the API at api, which calls through client, as
// SignedIn opens one.
func NewClient(api string, client *http.Client) *Client {
	return &Client{api: strings.TrimSuffix(api, "/"), http: client}
}

// Order is the order in which a read answers entries, by crawl time.
type Order string

const (
	NewestFirst Order = "d"
	OldestFirst Order = "o"
)

// ReadingListQuery is one read of the reading list.
type ReadingListQuery struct {
	// Since is the earliest crawl time to read, which the API takes in whole
	// seconds. The API answers each entry crawled or changed since then.
	// Zero reads from the start.
	Since time.Time
	Order Order
	// Count is the most entries that one page answers.
	Count int
	// Continuation is the page's position past the first, as the page before
	// it answered.
	Continuation string
}

// Page is one page of a stream, as stream/contents answers it.
type Page struct {
	// Updated is when the server answered, in seconds since the epoch.
	Updated int64   `json:"updated"`
	Items   []Entry `json:"items"`
	// Continuation is the position of the next page, and "" on the last.
	Continuation string `json:"continuation"`
}

// Entry is one entry of a stream, in the shape that FreshRSS's API writes.
type Entry struct {
	// ID is the entry's id, as tag:google.com,2005:reader/item/<hex>.
	ID string `json:"id"`
	// CrawlTimeMsec is when the server added the entry, in milliseconds since
	// the epoch.
	CrawlTimeMsec millis `json:"crawlTimeMsec"`
	// Published is the feed's own time for the entry, in seconds since the
	// epoch.
	Published  int64     `json:"published"`
	Title      string    `json:"title"`
	Canonical  []Link    `json:"canonical"`
	Alternate  []Link    `json:"alternate"`
	Categories []string  `json:"categories"`
	Origin     Origin    `json:"origin"`
	Summary    *Contents `json:"summary"`
	Content    *Contents `json:"content"`
}

// Link is one link of an entry.
type Link struct {
	Href string `json:"href"`
}

// Origin is the feed that holds an entry.
type Origin struct {
	StreamID string `json:"streamId"`
	Title    string `json:"title"`
	HTMLURL  string `json:"htmlUrl"`
}

// Contents is an entry's content as the feed carried it, in HTML.
type Contents struct {
	Content string `json:"content"`
}

// millis is a time in milliseconds, which the API writes as a string.
type millis int64

func (m *millis) UnmarshalJSON(raw []byte) error {
	text := strings.Trim(string(raw), `"`)
	if text == "" || text == "null" {
		*m = 0
		return nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("a time in milliseconds is a whole number, not %s", raw)
	}
	*m = millis(n)
	return nil
}

// ReadingList reads one page of the reading list.
func (c *Client) ReadingList(ctx context.Context, q ReadingListQuery) (Page, error) {
	params := url.Values{"output": {"json"}, "r": {string(q.Order)}, "n": {strconv.Itoa(q.Count)}}
	if !q.Since.IsZero() {
		params.Set("ot", strconv.FormatInt(q.Since.Unix(), 10)) // codespell:ignore ot
	}
	if q.Continuation != "" {
		params.Set("c", q.Continuation)
	}
	route := c.api + "/reader/api/0/stream/contents/" + readingList + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, route, http.NoBody)
	if err != nil {
		return Page{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Page{}, fmt.Errorf("read FreshRSS's reading list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswerBytes+1))
	switch {
	case err != nil:
		return Page{}, fmt.Errorf("read FreshRSS's reading list: %w", err)
	case resp.StatusCode != http.StatusOK:
		return Page{}, fmt.Errorf("read FreshRSS's reading list: %w", refusal(resp.StatusCode, body))
	case len(body) > maxAnswerBytes:
		return Page{}, fmt.Errorf("a page of FreshRSS's reading list ran past %d bytes", maxAnswerBytes)
	}
	var page Page
	if err := json.Unmarshal(body, &page); err != nil {
		return Page{}, fmt.Errorf("FreshRSS's reading list is not a stream's page: %w", err)
	}
	return page, nil
}
