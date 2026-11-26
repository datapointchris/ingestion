// Package redditexport reads an export that the Reddit bookmarklet wrote, in
// the form a store keeps it, and splits it into one save each. Reddit answers
// its JSON only to a logged-in page, so a browser downloads each export and a
// person drops the file.
//
// The stored form is the bookmarklet's JSON with its keys sorted, its HTML
// characters escaped, a machine field added, and a large file split into parts.
// A part is a whole export that its part field places, so a JSON decoder reads
// every one of those alike.
//
// Land lands an export as an item of its own and each save as its child, so a
// connector that receives exports calls it rather than composing the two. The
// package registers Land as the drop handler of Format. Where a drop connector
// names Format, each file dropped into it lands through Land.
package redditexport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// Format is the format a drop connector names to land each file it takes as
// a Reddit export: the export as an item, and each save as its child.
const Format = "reddit-export"

// Zone is the zone of every item that Land lands, because Reddit's users wrote
// it. A connector naming Format declares it, or the set is refused.
const Zone = store.Outside

func init() {
	connectors.RegisterDropHandler(Format, Zone, landDrop)
	landing.RegisterSourceTime(MediaTypeSave, saveCreated)
}

// landDrop lands a dropped file through Land, under the connector's name and
// tags, and answers the export's arrival, then each save's.
func landDrop(ctx context.Context, l *landing.Lander, d connectors.Drop) ([]landing.Landed, error) {
	landed, err := Land(ctx, l, Drop{Connector: d.Connector.Name, Name: d.Name, Tags: d.Connector.Tags, Raw: d.Body})
	if err != nil {
		return nil, err
	}
	return append([]landing.Landed{landed.Export}, landed.Saves...), nil
}

// threadNeverFetched is why a top post carries no thread where its export
// holds no entry for it.
const threadNeverFetched = "the export ended before this thread was fetched"

// The kinds an export names, one per bookmarklet mode.
const (
	KindSaved  = "reddit-saved"
	KindTop    = "reddit-top"
	KindThread = "reddit-thread"
)

// The media types of an export and of one save split from it.
const (
	MediaTypeExport = "application/x-reddit-export+json"
	MediaTypeSave   = "application/x-reddit-save+json"
)

// FieldSubreddit names the field of a save's sighting that holds its
// subreddit, as Reddit spells it, for a routing rule's match to name.
const FieldSubreddit = "subreddit"

// Export is one export. Which fields are set depends on Kind.
type Export struct {
	Kind         string    `json:"kind"`
	Exporter     string    `json:"exporter"`
	ExportedAt   time.Time `json:"exported_at"`
	ExportedFrom string    `json:"exported_from"`
	Complete     bool      `json:"complete"`
	Error        string    `json:"error,omitempty"`
	// Machine is the box that stored the export.
	Machine string `json:"machine,omitempty"`
	// Part places a stored part within the file it was split from.
	Part *Part `json:"part,omitempty"`

	User      string            `json:"user,omitempty"`
	Requested int               `json:"requested,omitempty"`
	Items     []json.RawMessage `json:"items,omitempty"`
	// FirstPosition is the saved-list position of Items[0] where a part
	// starts partway down the list.
	FirstPosition int `json:"first_position,omitempty"`

	Period     string      `json:"period,omitempty"`
	Subreddits []Subreddit `json:"subreddits,omitempty"`

	Fullname string          `json:"fullname,omitempty"`
	Listing  json.RawMessage `json:"listing,omitempty"`
}

// Part places a stored part within the file it was split from.
type Part struct {
	Index int `json:"index"`
	Of    int `json:"of"`
}

// Subreddit is one subreddit's top posts and each post's thread, by fullname.
type Subreddit struct {
	Name      string                     `json:"name"`
	Limit     int                        `json:"limit"`
	FirstRank int                        `json:"first_rank,omitempty"`
	Listing   listing                    `json:"listing"`
	Threads   map[string]json.RawMessage `json:"threads"`
}

type listing struct {
	Data struct {
		Children []json.RawMessage `json:"children"`
	} `json:"data"`
}

// thing is as much of a post (t3) or a comment (t1) as a save is named and
// dated by. CreatedUTC is the Unix seconds when it was posted.
type thing struct {
	Kind string `json:"kind"`
	Data struct {
		Name       string  `json:"name"`
		Subreddit  string  `json:"subreddit"`
		Title      string  `json:"title"`
		LinkTitle  string  `json:"link_title"`
		Permalink  string  `json:"permalink"`
		CreatedUTC float64 `json:"created_utc"`
	} `json:"data"`
}

// created is when t was posted, and zero where Reddit gave no time.
func (t thing) created() time.Time {
	if t.Data.CreatedUTC <= 0 {
		return time.Time{}
	}
	whole, frac := math.Modf(t.Data.CreatedUTC)
	return time.Unix(int64(whole), int64(frac*1e9)).UTC()
}

// saveCreated reads a save's bytes, as Save.Bytes holds them, and answers
// when its post or comment was posted.
func saveCreated(body io.Reader) (time.Time, error) {
	var entry saved
	if err := json.NewDecoder(body).Decode(&entry); err != nil {
		return time.Time{}, err
	}
	var t thing
	if err := json.Unmarshal(entry.Child, &t); err != nil {
		return time.Time{}, err
	}
	return t.created(), nil
}

// saved is one entry of a saved export: the save and its thread, which is
// null where Reddit refused it and ThreadError says how.
type saved struct {
	Child       json.RawMessage `json:"child"`
	Thread      json.RawMessage `json:"thread"`
	ThreadError string          `json:"thread_error,omitempty"`
}

// Parse reads one export, and refuses a file that the bookmarklet did not write.
func Parse(raw []byte) (Export, error) {
	var e Export
	if err := json.Unmarshal(raw, &e); err != nil {
		return e, fmt.Errorf("not a Reddit export: %w", err)
	}
	switch e.Kind {
	case KindSaved, KindTop, KindThread:
	default:
		return e, fmt.Errorf("not a Reddit export: kind %q is not %s, %s or %s", e.Kind, KindSaved, KindTop, KindThread)
	}
	if e.ExportedAt.IsZero() {
		return e, fmt.Errorf("the %s export names no exported_at", e.Kind)
	}
	return e, nil
}

// Save is one post or comment that an export carries, with its thread.
type Save struct {
	// Fullname is Reddit's name for the thing, such as t3_aq7ggd.
	Fullname  string
	Subreddit string
	// Title is a post's title, or for a comment the title of its post.
	Title string
	// URL is the thing's permalink on reddit.com.
	URL string
	// Created is when the thing was posted, and zero where Reddit gave no
	// time. Reddit's saved listing holds no time of the save itself, and a
	// thing is saved no earlier than it was posted.
	Created time.Time
	// Bytes is the save as a JSON object of child, the thing as Reddit gave
	// it, and thread, its thread's listing. A save from a saved export is the
	// export's entry unchanged.
	Bytes []byte
}

// Key is the save's item key, reddit/<fullname>.
func (s Save) Key() (string, error) { return itemkey.FromRedditFullname(s.Fullname) }

// Saves splits e into one save per post or comment it carries, in its order.
func (e Export) Saves() ([]Save, error) {
	var out []Save
	switch e.Kind {
	case KindSaved:
		for i, raw := range e.Items {
			var entry saved
			if err := json.Unmarshal(raw, &entry); err != nil {
				return nil, fmt.Errorf("saved entry %d: %w", i+1, err)
			}
			s, err := saveOf(entry.Child, raw)
			if err != nil {
				return nil, fmt.Errorf("saved entry %d: %w", i+1, err)
			}
			out = append(out, s)
		}
	case KindTop:
		for _, sub := range e.Subreddits {
			for i, child := range sub.Listing.Data.Children {
				var t thing
				if err := json.Unmarshal(child, &t); err != nil {
					return nil, fmt.Errorf("r/%s's post %d: %w", sub.Name, i+1, err)
				}
				thread, fetched := sub.Threads[t.Data.Name]
				entry := saved{Child: child, Thread: thread}
				var failed struct {
					Error string `json:"error"`
				}
				switch {
				case !fetched:
					entry.ThreadError = threadNeverFetched
				case json.Unmarshal(thread, &failed) == nil && failed.Error != "":
					entry.Thread, entry.ThreadError = nil, failed.Error
				}
				raw, err := json.Marshal(entry)
				if err != nil {
					return nil, err
				}
				s, err := saveOf(child, raw)
				if err != nil {
					return nil, fmt.Errorf("r/%s's post %d: %w", sub.Name, i+1, err)
				}
				out = append(out, s)
			}
		}
	case KindThread:
		var parts []listing
		if err := json.Unmarshal(e.Listing, &parts); err != nil || len(parts) == 0 || len(parts[0].Data.Children) == 0 {
			return nil, fmt.Errorf("the thread export holds no post: %v", err)
		}
		child := parts[0].Data.Children[0]
		raw, err := json.Marshal(saved{Child: child, Thread: e.Listing})
		if err != nil {
			return nil, err
		}
		s, err := saveOf(child, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Drop is one stored export, as a connector delivers it.
type Drop struct {
	// Connector names the connector that delivered the export.
	Connector string
	// Name is the connector's own name for the export, such as its file's.
	Name string
	// Tags are the connector's tags for the export and every save in it.
	Tags []string
	Raw  []byte
}

// Landed is what one export's landing wrote: the export's own item, then
// each save's in the export's order.
type Landed struct {
	Export landing.Landed
	Saves  []landing.Landed
}

// Land lands d as an item of its own, keyed by its bytes, then each save it
// carries as that item's child under reddit/<fullname>. A save's sighting
// records the export's key as its cursor, which names the drop it came in,
// and its subreddit under FieldSubreddit. A save's source time is when its
// thing was posted. Every arrival takes Zone. A save
// that an earlier export landed keeps that export as its parent, and its first
// bytes.
//
// Land refuses a file that is not a whole export before it writes anything,
// and marks that refusal with connectors.Refused. An export whose complete is
// false is refused naming the bookmarklet's error, so a fetch that failed
// partway is exported again rather than taken for this month's saves.
func Land(ctx context.Context, l *landing.Lander, d Drop) (Landed, error) {
	e, err := Parse(d.Raw)
	if err != nil {
		return Landed{}, connectors.Refused(err)
	}
	if !e.Complete {
		reason := e.Error
		if reason == "" {
			reason = "it names no error"
		}
		return Landed{}, connectors.Refused(fmt.Errorf("the %s export did not finish, so export again and drop the new file: %s", e.Kind, reason))
	}
	saves, err := e.Saves()
	if err != nil {
		return Landed{}, connectors.Refused(err)
	}
	keys := make([]string, len(saves))
	for i, s := range saves {
		if keys[i], err = s.Key(); err != nil {
			return Landed{}, connectors.Refused(err)
		}
	}
	var out Landed
	out.Export, err = l.Land(ctx, landing.Arrival{
		Connector: d.Connector, Zone: Zone, MediaType: MediaTypeExport,
		ExternalKey: d.Name, Tags: d.Tags, Body: bytes.NewReader(d.Raw),
	})
	if err != nil {
		return out, fmt.Errorf("land the export: %w", err)
	}
	for i, s := range saves {
		key := keys[i]
		var fields map[string]string
		if s.Subreddit != "" {
			fields = map[string]string{FieldSubreddit: s.Subreddit}
		}
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: d.Connector, Zone: Zone, Key: key, MediaType: MediaTypeSave,
			ExternalKey: s.Fullname, Tags: d.Tags, Fields: fields, CursorPosition: out.Export.Key, Title: s.Title, URL: s.URL,
			SourceTime: s.Created, Parent: out.Export.Key, Body: bytes.NewReader(s.Bytes),
		})
		if err != nil {
			return out, fmt.Errorf("land %s: %w", key, err)
		}
		out.Saves = append(out.Saves, landed)
	}
	return out, nil
}

func saveOf(child, raw json.RawMessage) (Save, error) {
	var t thing
	if err := json.Unmarshal(child, &t); err != nil {
		return Save{}, err
	}
	if t.Data.Name == "" {
		return Save{}, fmt.Errorf("a %s names no fullname", t.Kind)
	}
	title := t.Data.Title
	if t.Kind == "t1" {
		title = t.Data.LinkTitle
	}
	s := Save{Fullname: t.Data.Name, Subreddit: t.Data.Subreddit, Title: title, Created: t.created(), Bytes: raw}
	if t.Data.Permalink != "" {
		s.URL = "https://www.reddit.com" + t.Data.Permalink
	}
	return s, nil
}
