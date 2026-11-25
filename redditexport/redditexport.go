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
// connector that receives exports calls it rather than composing the two.
package redditexport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

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

// thing is as much of a post (t3) or a comment (t1) as a save is named by.
type thing struct {
	Kind string `json:"kind"`
	Data struct {
		Name      string `json:"name"`
		Subreddit string `json:"subreddit"`
		Title     string `json:"title"`
		LinkTitle string `json:"link_title"`
		Permalink string `json:"permalink"`
	} `json:"data"`
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
// and its subreddit under FieldSubreddit. Every arrival is outside, because
// Reddit's users wrote it. A save that an
// earlier export landed keeps that export as its parent, and its first bytes.
func Land(ctx context.Context, l *landing.Lander, d Drop) (Landed, error) {
	e, err := Parse(d.Raw)
	if err != nil {
		return Landed{}, err
	}
	saves, err := e.Saves()
	if err != nil {
		return Landed{}, err
	}
	var out Landed
	out.Export, err = l.Land(ctx, landing.Arrival{
		Connector: d.Connector, Zone: store.Outside, MediaType: MediaTypeExport,
		ExternalKey: d.Name, Tags: d.Tags, Body: bytes.NewReader(d.Raw),
	})
	if err != nil {
		return out, fmt.Errorf("land the export: %w", err)
	}
	for _, s := range saves {
		key, err := s.Key()
		if err != nil {
			return out, err
		}
		var fields map[string]string
		if s.Subreddit != "" {
			fields = map[string]string{FieldSubreddit: s.Subreddit}
		}
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: d.Connector, Zone: store.Outside, Key: key, MediaType: MediaTypeSave,
			ExternalKey: s.Fullname, Tags: d.Tags, Fields: fields, CursorPosition: out.Export.Key, Title: s.Title, URL: s.URL,
			Parent: out.Export.Key, Body: bytes.NewReader(s.Bytes),
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
	s := Save{Fullname: t.Data.Name, Subreddit: t.Data.Subreddit, Title: title, Bytes: raw}
	if t.Data.Permalink != "" {
		s.URL = "https://www.reddit.com" + t.Data.Permalink
	}
	return s, nil
}
