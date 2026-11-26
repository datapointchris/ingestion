package ypl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/server"
)

// A channel's poll keeps the newest windowSize uploads as its window, and
// reads at most mostUploads past it before it stops with a gap.
const (
	windowSize  = 50
	mostUploads = 500
)

// What a poll passes over, by the name its log line counts it under.
const (
	// countUnavailable is a video that YouTube withholds, such as a deleted
	// or private one.
	countUnavailable = "unavailable"
	// countRepeated is a second item of a video that the playlist already
	// holds.
	countRepeated = "repeated"
	// countStreaming is a video on air or scheduled, which waits until its
	// recording is finished.
	countStreaming = "streaming"
	// countBaseline is each upload that a channel's first poll keeps as its
	// window.
	countBaseline = "baseline"
)

// Poller reads ypl for each ypl connector's poll.
type Poller struct{ client *Client }

// NewPoller is the poller that reads ypl through client.
func NewPoller(client *Client) Poller { return Poller{client: client} }

// Poll reads the connector's playlist or channel, as its source_parameters
// name it.
func (p Poller) Poll(ctx context.Context, poll polling.Poll) polling.Outcome {
	params := poll.Connector.SourceParameters
	if channel := params[ParameterChannel]; channel != "" {
		return p.pollChannel(ctx, channel, poll.Cursor)
	}
	if playlist := params[ParameterPlaylist]; playlist != "" {
		return p.pollPlaylist(ctx, playlist, poll.Connector.Once())
	}
	reason := fmt.Sprintf("connector %s names neither a playlist nor a channel in its source_parameters", poll.Connector.Name)
	return polling.Outcome{Error: &reason}
}

// pollPlaylist reports each available video that playlist holds, in its
// order. It keeps no cursor, because serve lands a video it already holds as
// nothing new, and a capture's playlist holds only what has not landed. A
// video on air or scheduled waits in the playlist until its recording is
// finished. A poll of a connector scheduled once is its last, so it lands
// such a video rather than leave it behind.
func (p Poller) pollPlaylist(ctx context.Context, playlist string, last bool) polling.Outcome {
	read, err := p.client.PlaylistItems(ctx, playlist)
	if err != nil {
		return failed(err)
	}
	out := polling.Outcome{Counts: map[string]int{}}
	var items []PlaylistItem
	held := map[string]bool{}
	for _, item := range read.Items {
		switch {
		case item.Unavailable:
			out.Counts[countUnavailable]++
		case held[item.VideoID]:
			out.Counts[countRepeated]++
		default:
			held[item.VideoID] = true
			items = append(items, item)
		}
	}
	videos, err := p.videos(ctx, items)
	if err != nil {
		return failed(err)
	}
	for _, item := range items {
		v, ok := videos[item.VideoID]
		switch {
		case !ok:
			out.Counts[countUnavailable]++
		case v.Streaming() && !last:
			out.Counts[countStreaming]++
		default:
			out.Arrivals = append(out.Arrivals, arrival(v, item.ItemID, map[string]string{FieldPlaylistID: read.PlaylistID}))
		}
	}
	return out
}

// channelCursor is a channel connector's position.
type channelCursor struct {
	// Channel is the channel that the window was read from, so a connector
	// declared again for another channel keeps a new baseline.
	Channel string `json:"channel"`
	// Window is the newest uploads' video ids at the last poll, newest first.
	Window []string `json:"window"`
	// Waiting is each upload that was on air or scheduled at the last poll,
	// which lands once its recording is finished.
	Waiting []string `json:"waiting"`
}

// pollChannel reports each upload that channel added since the last poll,
// oldest first. Its first poll keeps the newest uploads as its window and
// lands nothing, so a channel's back catalog never floods the review queue.
// A later poll reads newest first until it meets an upload in the window. It
// stops at mostUploads with a gap where it meets none, as where the channel
// deleted every upload in the window. An upload on air or scheduled waits
// in the cursor until its recording is finished.
func (p Poller) pollChannel(ctx context.Context, channel string, cursor *string) polling.Outcome {
	var was channelCursor
	if cursor != nil {
		if err := json.Unmarshal([]byte(*cursor), &was); err != nil {
			reason := fmt.Sprintf("the connector's cursor is not a %s channel's position: %v", Source, err)
			return polling.Outcome{Error: &reason}
		}
	}
	page, err := p.client.ChannelUploads(ctx, channel, "")
	if err != nil {
		return failed(err)
	}
	next := channelCursor{Channel: channel, Window: []string{}, Waiting: []string{}}
	for _, item := range page.Data[:min(windowSize, len(page.Data))] {
		next.Window = append(next.Window, item.VideoID)
	}
	out := polling.Outcome{Counts: map[string]int{}}
	if cursor == nil || was.Channel != channel {
		out.Counts[countBaseline] = len(next.Window)
		return withCursor(out, next)
	}

	inWindow := map[string]bool{}
	for _, id := range was.Window {
		inWindow[id] = true
	}
	var added []PlaylistItem
	met := false
read:
	for {
		for _, item := range page.Data {
			if inWindow[item.VideoID] {
				met = true
				break read
			}
			added = append(added, item)
		}
		if page.NextPageToken == nil || len(added) >= mostUploads {
			break
		}
		if page, err = p.client.ChannelUploads(ctx, channel, *page.NextPageToken); err != nil {
			return failed(err)
		}
	}
	if !met && page.NextPageToken != nil {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"read %d uploads without meeting the last poll's window, so any upload older than them and newer than the window is not read",
			len(added)))
	}

	slices.Reverse(added)
	candidates := []PlaylistItem{}
	for _, id := range was.Waiting {
		candidates = append(candidates, PlaylistItem{VideoID: id})
	}
	for _, item := range added {
		if item.Unavailable {
			out.Counts[countUnavailable]++
			continue
		}
		candidates = append(candidates, item)
	}
	videos, err := p.videos(ctx, candidates)
	if err != nil {
		return failed(err)
	}
	for _, item := range candidates {
		v, ok := videos[item.VideoID]
		switch {
		case !ok:
			out.Counts[countUnavailable]++
		case v.Streaming():
			out.Counts[countStreaming]++
			next.Waiting = append(next.Waiting, item.VideoID)
		default:
			out.Arrivals = append(out.Arrivals, arrival(v, "", nil))
		}
	}
	return withCursor(out, next)
}

// videos reads the details of each of items' videos, by id.
func (p Poller) videos(ctx context.Context, items []PlaylistItem) (map[string]Video, error) {
	out := map[string]Video{}
	if len(items) == 0 {
		return out, nil
	}
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.VideoID
	}
	read, err := p.client.Videos(ctx, ids)
	for _, v := range read {
		out[v.ID] = v
	}
	return out, err
}

// arrival is v as a poll reports it, keyed youtube/<id>. externalKey is the
// playlist item that held it, and "" for an upload. fields are what the
// connector's kind adds to the channel's.
func arrival(v Video, externalKey string, fields map[string]string) server.ReportArrival {
	a := server.ReportArrival{
		Key: itemkey.YouTube + "/" + v.ID, URL: WatchURL(v.ID), MediaType: MediaTypeVideo, Title: v.Title, ExternalKey: externalKey,
		Fields: map[string]string{FieldChannelID: v.ChannelID},
		Metrics: map[string]int64{
			"views": count(v.ViewCount), "likes": count(v.LikeCount), "comments": count(v.CommentCount),
		},
	}
	if v.ChannelTitle != "" {
		a.Fields[FieldChannelTitle] = v.ChannelTitle
	}
	for name, value := range fields {
		a.Fields[name] = value
	}
	if v.DurationSeconds != nil {
		a.Metrics["duration_s"] = *v.DurationSeconds
	}
	if _, err := time.Parse(time.RFC3339, v.PublishedAt); err == nil {
		published := v.PublishedAt
		a.SourceTS = &published
	}
	return a
}

// WatchURL is the address of video id's watch page.
func WatchURL(id string) string { return "https://www.youtube.com/watch?v=" + id }

func count(n uint64) int64 { return int64(min(n, math.MaxInt64)) }

func withCursor(out polling.Outcome, c channelCursor) polling.Outcome {
	raw, err := json.Marshal(c)
	if err != nil {
		reason := "write the channel's cursor: " + err.Error()
		return polling.Outcome{Error: &reason}
	}
	position := string(raw)
	out.Cursor = &position
	return out
}

// failed is a poll that err stopped. Where ypl asked the poll to wait, as
// while the day's quota is spent, the same poll tries again after that wait.
func failed(err error) polling.Outcome {
	reason := err.Error()
	out := polling.Outcome{Error: &reason}
	var refusal *Error
	if errors.As(err, &refusal) {
		out.RetryAfter = refusal.RetryAfter
	}
	return out
}
