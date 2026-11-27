// Package ypl is the YouTube source. ypl is a YouTube library service whose
// API holds the channel's credential, prices each call against the day's
// quota, and paces every read. Every request to YouTube goes through it, so
// this package is a client of ypl's API, never of YouTube.
//
// It polls two kinds of connector, named by the connector's
// source_parameters: a playlist, whose items land as they appear, and a
// channel, whose new uploads land after a first poll that keeps a baseline.
// It deletes a landed save from its playlist for a connector that declares
// after_landing delete, which a playlist polled on a schedule declares. It
// parses each video from its captions, and where it has none, from Whisper's
// transcript of its audio, each read through ypl. It reads again the views,
// likes, comments and length of each video that waits in review, in one
// details read of up to MaxVideos at a time, when serve's daily refresh asks.
package ypl

import (
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deletion"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/metricsrefresh"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/sources"
)

// Source is the source's name: the suffix of its tasks' capabilities, and its
// table's name under [source] in a worker's config.
const Source = "ypl"

// MediaTypeVideo is a YouTube video's media type. An item of it holds no
// bytes, and its key is youtube/<video id>.
const MediaTypeVideo = "application/x-youtube-video"

// A ypl connector's source_parameters name one of these two keys.
const (
	// ParameterPlaylist names the playlist to read: its YouTube id, or the
	// whole title or slug of a playlist that ypl holds.
	ParameterPlaylist = "playlist"
	// ParameterChannel is the channel whose uploads the connector reads: its
	// id or its @handle.
	ParameterChannel = "channel"
)

// A sighting carries these fields beside the item's title and URL.
const (
	FieldChannelID    = "channel_id"
	FieldChannelTitle = "channel_title"
	// FieldPlaylistID is the YouTube id of the playlist that held the video,
	// which the delete names.
	FieldPlaylistID = "playlist_id"
)

// A sighting of a video carries these metrics: its views, likes and comments,
// and its length in seconds where it has one.
const (
	MetricViews    = "views"
	MetricLikes    = "likes"
	MetricComments = "comments"
	MetricDuration = "duration_s"
)

// Scopes are what the source's service client asks for: what the poller
// reads, what the deleter writes, what the extractors read, and the comments
// that a person asks about. ypl's route table grants each, and a test holds
// them to the routes this package calls.
var Scopes = []string{
	"ypl.playlists.read", "ypl.channels.read", "ypl.videos.read", "ypl.playlist-items.write",
	"ypl.transcripts.read", "ypl.audio.read", "ypl.comments.read",
}

func init() {
	sources.Register(sources.Registration{
		Name: Source, Scopes: Scopes, Rules: rules,
		Poller:  func(api string, client *http.Client) polling.Poller { return NewPoller(NewClient(api, client)) },
		Deleter: func(api string, client *http.Client) deletion.Deleter { return NewDeleter(NewClient(api, client)) },
		Refresher: func(api string, client *http.Client) metricsrefresh.Refresher {
			return NewRefresher(NewClient(api, client))
		},
		Extractors: []extract.Extractor{CaptionsExtractor, WhisperExtractor},
		Comments:   &CommentsSource,
	})
	landing.RegisterSightingRoles(landing.SightingRoles{
		Channel: FieldChannelTitle, ChannelFormat: "%s", ChannelFields: []string{FieldChannelTitle, FieldChannelID},
		Length: MetricDuration, Views: MetricViews, Likes: MetricLikes, Comments: MetricComments,
	})
}

// channelName is the shape of a channel id or an @handle that ypl takes in a
// path. It copies ypl's own, and a test holds it to the pattern that ypl's
// suite writes.
var channelName = regexp.MustCompile(`^@?[A-Za-z0-9_.-]+$`)

// rules answers why c is not a ypl connector that can run.
func rules(c connectors.Connector) []string {
	var out []string
	for _, key := range slices.Sorted(maps.Keys(c.SourceParameters)) {
		if key != ParameterPlaylist && key != ParameterChannel {
			out = append(out, fmt.Sprintf("source_parameters: %s takes %s or %s, not %s", Source, ParameterPlaylist, ParameterChannel, key))
		}
	}
	playlist, channel := c.SourceParameters[ParameterPlaylist], c.SourceParameters[ParameterChannel]
	switch {
	case playlist == "" && channel == "":
		out = append(out, "source_parameters: name the playlist or the channel that the connector reads")
	case playlist != "" && channel != "":
		out = append(out, "source_parameters: a connector reads a playlist or a channel, not both")
	case channel != "":
		if !channelName.MatchString(channel) {
			out = append(out, fmt.Sprintf("source_parameters: channel is a channel id or an @handle, not %q", channel))
		}
		if c.AfterLanding == connectors.AfterLandingDelete {
			out = append(out, "after_landing: a channel's uploads belong to its owner, so nothing deletes them")
		}
		if c.Once() {
			out = append(out, "schedule: a channel's first poll keeps a baseline and lands nothing, so a channel polls on a schedule")
		}
	case !c.Once() && c.AfterLanding != connectors.AfterLandingDelete:
		out = append(out, "after_landing: a playlist polled on a schedule deletes what it lands, "+
			"or each poll reads the whole playlist again. Schedule it once to land a playlist that keeps its videos")
	}
	return out
}
