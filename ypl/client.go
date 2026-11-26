package ypl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client calls ypl's API, whose routes under /api/v1/youtube/ call YouTube on
// a caller's behalf. Every request to YouTube goes through ypl, so this is the
// only client of YouTube that ingestion holds.
type Client struct {
	api  string
	http *http.Client
}

// NewClient is the client that reaches ypl at api through client, which
// carries the service client's token.
func NewClient(api string, client *http.Client) *Client {
	return &Client{api: strings.TrimRight(api, "/"), http: client}
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

// MaxVideos is the most ids one read of video details names.
const MaxVideos = 500

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

// maxAnswer bounds the answer a read decodes: a playlist of 5,000 items, or 500
// videos with their descriptions, fits well within it.
const maxAnswer = 64 << 20

// PlaylistItems reads every item of playlist from YouTube now. playlist is a
// YouTube id, or the whole title or slug of a playlist that ypl holds.
func (c *Client) PlaylistItems(ctx context.Context, playlist string) (Playlist, error) {
	var out Playlist
	err := c.do(ctx, http.MethodGet, "/api/v1/youtube/playlists/"+url.PathEscape(playlist)+"/items", nil, &out)
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
	err := c.do(ctx, http.MethodGet, "/api/v1/youtube/channels/"+url.PathEscape(channel)+"/uploads", query, &out)
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
		if err := c.do(ctx, http.MethodGet, "/api/v1/youtube/videos", query, &page); err != nil {
			return out, err
		}
		out = append(out, page...)
	}
	return out, nil
}

// DeletePlaylistItem deletes item from playlist on YouTube. ypl answers an item
// already gone as deleted.
func (c *Client) DeletePlaylistItem(ctx context.Context, playlist, item string) error {
	return c.do(ctx, http.MethodDelete, "/api/v1/youtube/playlists/"+url.PathEscape(playlist)+"/items/"+url.PathEscape(item), nil, nil)
}

// do sends one request and decodes its answer into into, where into is not
// nil. A status of 300 or more is an *Error.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, into any) error {
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
		refusal := &Error{Method: method, Path: path, Status: resp.StatusCode, RetryAfter: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
		var answer struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		}
		if json.NewDecoder(body).Decode(&answer) == nil {
			refusal.Code, refusal.Message = answer.Code, answer.Message
		}
		return refusal
	}
	if into == nil {
		return nil
	}
	if err := json.NewDecoder(body).Decode(into); err != nil {
		return fmt.Errorf("%s %s: read the answer: %w", method, path, err)
	}
	return nil
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

// refusedWith says whether err is ypl's refusal with status.
func refusedWith(err error, status int) bool {
	var refusal *Error
	return errors.As(err, &refusal) && refusal.Status == status
}
