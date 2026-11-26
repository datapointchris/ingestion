package ypl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/extract"
)

// Client calls ypl's API, whose routes under /api/v1/youtube/ call YouTube on
// a caller's behalf. Every request to YouTube goes through ypl, so this is the
// only client of YouTube that ingestion holds.
type Client struct {
	api  string
	http *http.Client
}

// NewClient is the client that reaches ypl at api through a copy of client,
// which carries the service client's token. The copy follows no redirect, so
// a redirect is a refusal rather than an answer from wherever it points.
func NewClient(api string, client *http.Client) *Client {
	unredirected := *client
	unredirected.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{api: strings.TrimRight(api, "/"), http: &unredirected}
}

// PlaylistItem is one item of a playlist or of a channel's uploads, as YouTube
// held it at the read. ItemID is what a delete of the item names.
type PlaylistItem struct {
	ItemID       string  `json:"item_id"`
	VideoID      string  `json:"video_id"`
	Position     int64   `json:"position"`
	Title        string  `json:"title"`
	ChannelTitle *string `json:"channel_title"`
	// Unavailable is a video that YouTube withholds, such as a deleted or
	// private one.
	Unavailable bool `json:"unavailable"`
}

// Playlist is every item of a playlist, in order. PlaylistID is its YouTube
// id, though the read named it by its title.
type Playlist struct {
	PlaylistID string         `json:"playlist_id"`
	Items      []PlaylistItem `json:"items"`
}

// UploadsPage is one page of a channel's uploads, newest first. NextPageToken
// reads the page after, and is nil on the last.
type UploadsPage struct {
	Data          []PlaylistItem `json:"data"`
	NextPageToken *string        `json:"next_page_token"`
}

// Video is a video as YouTube reported its details at the read.
type Video struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Description  string `json:"description"`
	ChannelID    string `json:"channel_id"`
	ChannelTitle string `json:"channel_title"`
	// PublishedAt is when the video was published, in RFC 3339.
	PublishedAt     string `json:"published_at"`
	Privacy         string `json:"privacy"`
	DurationSeconds *int64 `json:"duration_seconds"`
	// LiveBroadcastContent is none, live or upcoming.
	LiveBroadcastContent string `json:"live_broadcast_content"`
	// The counts are 0 where the owner hides them.
	ViewCount    uint64 `json:"view_count"`
	LikeCount    uint64 `json:"like_count"`
	CommentCount uint64 `json:"comment_count"`
}

// Broadcast states that LiveBroadcastContent takes.
const (
	BroadcastLive     = "live"
	BroadcastUpcoming = "upcoming"
)

// Streaming says whether v is a stream that is on air or scheduled, which has
// no finished recording yet.
func (v Video) Streaming() bool {
	return v.LiveBroadcastContent == BroadcastLive || v.LiveBroadcastContent == BroadcastUpcoming
}

// MaxVideos is the most ids one read of video details names. It copies ypl's
// own limit, and a test holds it to the limit that ypl's suite writes.
const MaxVideos = 500

// callTimeout bounds one request to ypl's Data API routes, so a ypl that
// takes the connection and never answers frees the worker's slot. ypl waits
// for its turn at YouTube, then for YouTube's answer, and this outlasts both.
const callTimeout = 2 * time.Minute

// captionsTimeout bounds a read of a video's captions. ypl holds the read at
// its door for up to two minutes, then gives yt-dlp up to three to read the
// video's tracks and the one it picks, and this outlasts both.
const captionsTimeout = 6 * time.Minute

// audioWait bounds how long ypl takes to begin answering a read of a video's
// audio: up to two minutes at its door, then up to thirty for yt-dlp to
// download the audio, which ypl answers from a file once it has it whole. The
// answer's body then streams for as long as the caller's context allows.
const audioWait = 35 * time.Minute

// Error is ypl's refusal of a request: its status, its code for a client to
// branch on, and its sentence for a person. RetryAfter holds the refusal's
// Retry-After header as a wait, and is zero where ypl sent none.
type Error struct {
	Method, Path string
	Status       int
	Code         string
	Message      string
	RetryAfter   time.Duration
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("ypl answered %s %s with %d", e.Method, e.Path, e.Status)
	if e.Code != "" {
		msg += " " + e.Code
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg
}

// maxAnswer bounds the answer a read decodes: a playlist of 5,000 items, 500
// videos with their descriptions, or a long video's caption segments, fits
// well within it.
const maxAnswer = 64 << 20

// Captions is a video's captions as ypl read them: its details, its chapters,
// and the segments of the one caption track that ypl picks. Track is nil
// where the video has none, which leaves Segments empty.
type Captions struct {
	VideoID     string `json:"video_id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	// Language is the video's own as YouTube reports it, and nil where it
	// reports none.
	Language *string `json:"language"`
	// DurationSeconds is nil for a video with no length, such as a stream.
	DurationSeconds *int64            `json:"duration_seconds"`
	Track           *Track            `json:"track"`
	Segments        []extract.Segment `json:"segments"`
	Chapters        []Chapter         `json:"chapters"`
}

// Track is the caption track a read used: its language as YouTube files it,
// such as en or de-orig, and its kind, manual or automatic.
type Track struct {
	Language string `json:"language"`
	Kind     string `json:"kind"`
}

// Chapter is one of a video's chapters, which start and end at whole seconds.
type Chapter struct {
	StartSeconds int64  `json:"start_seconds"`
	EndSeconds   int64  `json:"end_seconds"`
	Title        string `json:"title"`
}

// Audio is a video's best audio as ypl streams it. Body ends with an error
// where the stream breaks before Size bytes.
type Audio struct {
	Body io.ReadCloser
	// MediaType is the audio's container, such as audio/webm.
	MediaType string
	// Size is the audio's length in bytes, and -1 where ypl sent none.
	Size int64
}

// PlaylistItems reads every item of playlist from YouTube now. playlist is a
// YouTube id, or the whole title or slug of a playlist that ypl holds.
func (c *Client) PlaylistItems(ctx context.Context, playlist string) (Playlist, error) {
	var out Playlist
	err := c.do(ctx, http.MethodGet, "/api/v1/youtube/playlists/"+url.PathEscape(playlist)+"/items", nil, http.StatusOK, &out)
	return out, err
}

// ChannelUploads reads one page of up to 50 of channel's uploads, newest
// first, after the page that pageToken names, or the first where it is "".
// channel is a channel id or an @handle.
func (c *Client) ChannelUploads(ctx context.Context, channel, pageToken string) (UploadsPage, error) {
	query := url.Values{}
	if pageToken != "" {
		query.Set("page_token", pageToken)
	}
	var out UploadsPage
	err := c.do(ctx, http.MethodGet, "/api/v1/youtube/channels/"+url.PathEscape(channel)+"/uploads", query, http.StatusOK, &out)
	return out, err
}

// Videos reads the details of each video in ids, MaxVideos to a request.
// YouTube leaves out a deleted video, a private one, and an id that no video
// has, so a caller finds each by its ID.
func (c *Client) Videos(ctx context.Context, ids []string) ([]Video, error) {
	out := []Video{}
	for start := 0; start < len(ids); start += MaxVideos {
		var page []Video
		query := url.Values{"ids": {strings.Join(ids[start:min(start+MaxVideos, len(ids))], ",")}}
		if err := c.do(ctx, http.MethodGet, "/api/v1/youtube/videos", query, http.StatusOK, &page); err != nil {
			return out, err
		}
		out = append(out, page...)
	}
	return out, nil
}

// DeletePlaylistItem deletes item from playlist on YouTube. ypl answers an item
// already gone as deleted.
func (c *Client) DeletePlaylistItem(ctx context.Context, playlist, item string) error {
	path := "/api/v1/youtube/playlists/" + url.PathEscape(playlist) + "/items/" + url.PathEscape(item)
	return c.do(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil)
}

// Captions reads video's captions through ypl's door: its details, its
// chapters, and the segments of the one track that ypl picks. A video with no
// track answers a nil Track. ypl refuses a video that no signed-out read
// reaches, such as a members-only one, with 422 video_unavailable.
func (c *Client) Captions(ctx context.Context, video string) (Captions, error) {
	var out Captions
	err := c.doWithin(ctx, captionsTimeout, http.MethodGet, "/api/v1/youtube/videos/"+url.PathEscape(video)+"/captions", nil, http.StatusOK, &out)
	return out, err
}

// Audio reads video's best audio through ypl's door. It waits up to audioWait
// for ypl to begin its answer, and the caller closes Body. ypl refuses as
// Captions does.
func (c *Client) Audio(ctx context.Context, video string) (*Audio, error) {
	path := "/api/v1/youtube/videos/" + url.PathEscape(video) + "/audio"
	ctx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(audioWait, cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.api+path, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.http.Do(req)
	began := timer.Stop()
	switch {
	case err != nil && !began:
		cancel()
		return nil, fmt.Errorf("GET %s: ypl began no answer within %s", path, audioWait)
	case err != nil:
		cancel()
		return nil, fmt.Errorf("GET %s: %w", path, err)
	case !began:
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("GET %s: ypl began no answer within %s", path, audioWait)
	case resp.StatusCode >= http.StatusMultipleChoices:
		defer cancel()
		defer func() { _ = resp.Body.Close() }()
		return nil, refusalOf(http.MethodGet, path, resp)
	case resp.StatusCode != http.StatusOK:
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("GET %s: ypl answered %d, not %d", path, resp.StatusCode, http.StatusOK)
	}
	return &Audio{Body: cancelOnClose{ReadCloser: resp.Body, cancel: cancel}, MediaType: resp.Header.Get("Content-Type"), Size: resp.ContentLength}, nil
}

// cancelOnClose ends its request's context once its body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	defer c.cancel()
	return c.ReadCloser.Close()
}

// do sends one request bounded by callTimeout, as doWithin does.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, want int, into any) error {
	return c.doWithin(ctx, callTimeout, method, path, query, want, into)
}

// doWithin sends one request bounded by within, and decodes its answer into
// into, where into is not nil. A status of 300 or more is an *Error. Any other
// status but want is an error too, because only want shows that the route
// ran.
func (c *Client) doWithin(ctx context.Context, within time.Duration, method, path string, query url.Values, want int, into any) error {
	ctx, cancel := context.WithTimeout(ctx, within)
	defer cancel()
	target := c.api + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, maxAnswer)
	if resp.StatusCode >= http.StatusMultipleChoices {
		return refusalOf(method, path, resp)
	}
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: ypl answered %d, not %d", method, path, resp.StatusCode, want)
	}
	if into == nil {
		return nil
	}
	if err := json.NewDecoder(body).Decode(into); err != nil {
		return fmt.Errorf("%s %s: read the answer: %w", method, path, err)
	}
	return nil
}

// refusalOf is ypl's refusal in resp, of a status of 300 or more, with the
// code and sentence its body names.
func refusalOf(method, path string, resp *http.Response) *Error {
	refusal := &Error{Method: method, Path: path, Status: resp.StatusCode, RetryAfter: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
	var answer struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, maxAnswer)).Decode(&answer) == nil {
		refusal.Code, refusal.Message = answer.Code, answer.Message
	}
	return refusal
}

// retryAfter is the wait a Retry-After header asks for, in seconds or as an
// HTTP date, and zero where it asks none.
func retryAfter(header string, now time.Time) time.Duration {
	if header == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(header, 10, 64); err == nil && seconds > 0 {
		return time.Duration(min(seconds, int64(math.MaxInt64/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(header); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
