// Package ypl is the YouTube source. ypl is a YouTube library service whose
// API holds the channel's credential, prices each call against the day's
// quota, and paces every read. Every request to YouTube goes through it, so
// this package is a client of ypl's API, never of YouTube.
//
// It polls two kinds of connector, named by the connector's
// source_parameters: a playlist, whose items land as they appear, and a
// channel, whose new uploads land after a first poll that keeps a baseline.
// It deletes a landed save from its playlist for a connector that declares
// after_landing delete.
package ypl

import (
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deletion"
	"github.com/datapointchris/ingestion/polling"
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

// The scopes that the source's service client asks for: what the poller reads,
// and what the deleter writes.
var (
	PollScopes   = []string{"ypl.playlists.read", "ypl.channels.read", "ypl.videos.read"}
	DeleteScopes = []string{"ypl.playlist-items.write"}
)

func init() {
	connectors.RegisterSourceRules(Source, rules)
	polling.Register(polling.Registration{Source: Source, Scopes: PollScopes, Open: func(api string, client *http.Client) polling.Poller {
		return NewPoller(NewClient(api, client))
	}})
	deletion.Register(deletion.Registration{Source: Source, Scopes: DeleteScopes, Open: func(api string, client *http.Client) deletion.Deleter {
		return NewDeleter(NewClient(api, client))
	}})
}

// channelName is the shape of a channel id or an @handle, as ypl takes one.
var channelName = regexp.MustCompile(`^@?[A-Za-z0-9_.-]+$`)

// rules answers why c is not a ypl connector that can run.
func rules(c connectors.Connector) []string {
	var out []string
	if c.Mode != connectors.ModePoll {
		out = append(out, fmt.Sprintf("mode: a %s connector polls, and is not a %s", Source, c.Mode))
	}
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
	}
	return out
}
