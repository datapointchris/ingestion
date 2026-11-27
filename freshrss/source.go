// Package freshrss is the FreshRSS source, a client of FreshRSS's Google
// Reader API. A worker signs in with the API password of the person whose
// subscriptions it reads, and polls their reading list past the newest crawl
// time that it read. The first poll keeps that crawl time as a baseline and
// lands nothing, so no entry crawled before the connector ran ever lands.
//
// Each new entry lands under its link's key, with the page that the link
// names as its bytes. The worker GETs the page itself, and where the page
// refuses it, lands the entry's own content from the feed instead. A
// sighting names the feed and its label, which a routing rule matches.
package freshrss

import (
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/sources"
	"github.com/datapointchris/ingestion/store"
)

// Source is the source's name: the suffix of its tasks' capabilities, and its
// table's name under [source] in a worker's config.
const Source = "freshrss"

// A sighting carries these fields beside the item's title and URL.
const (
	// FieldFeedStreamID is the feed's stream id, such as feed/12.
	FieldFeedStreamID = "feed_stream_id"
	// FieldFeedTitle is the feed's title as the person's FreshRSS names it.
	FieldFeedTitle = "feed_title"
	// FieldFeedLabel is the label of the category that holds the feed.
	FieldFeedLabel = "feed_label"
	// FieldBody says where the item's bytes came from: BodyPage or BodyFeed.
	FieldBody = "body"
	// FieldPageRefused says why the page's GET failed, where the bytes are
	// BodyFeed, such as "HTTP 403".
	FieldPageRefused = "page_refused"
)

// The values of FieldBody.
const (
	// BodyPage is the page that the entry's link names, as the worker's GET
	// read it.
	BodyPage = "page"
	// BodyFeed is the entry's own content from the feed, which is often a
	// teaser.
	BodyFeed = "feed"
)

func init() {
	sources.Register(sources.Registration{
		Name: Source, Rules: rules, Login: SignedIn,
		Poller: func(api string, client *http.Client) polling.Poller {
			return NewPoller(NewClient(api, client), NewPageReader(PageClient()))
		},
	})
	landing.RegisterSightingRoles(landing.SightingRoles{
		Channel: FieldFeedTitle, ChannelFormat: "%s", ChannelFields: []string{FieldFeedTitle, FieldFeedStreamID},
	})
}

// rules answers why c is not a freshrss connector that can run.
func rules(c connectors.Connector) []string {
	var out []string
	for _, key := range slices.Sorted(maps.Keys(c.SourceParameters)) {
		out = append(out, fmt.Sprintf("source_parameters: %s reads the whole reading list, so it takes no %s", Source, key))
	}
	if c.Once() {
		out = append(out, "schedule: the first poll keeps a baseline and lands nothing, so a "+Source+" connector polls on a schedule")
	}
	if c.Zone != store.Outside {
		out = append(out, fmt.Sprintf("zone: a feed's entries are written by outsiders, so the zone is %s, not %s", store.Outside, c.Zone))
	}
	return out
}
