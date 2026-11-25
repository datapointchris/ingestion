// Package redditexport reads an export that the Reddit bookmarklet wrote, in
// the form a store keeps it, and splits it into one save each. Reddit answers
// its JSON only to a logged-in page, so a browser downloads each export and a
// person drops the file.
//
// The stored form is the bookmarklet's JSON with its keys sorted, its HTML
// characters escaped, a machine field added, and any file over 8 MiB split
// into parts that each carry part. A JSON decoder reads every one of those
// alike.
package redditexport

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
)

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
	// Position is the save's place in the saved list, or its rank among its
	// subreddit's top posts.
	Position int
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
			s.Position = max(e.FirstPosition, 1) + i
			out = append(out, s)
		}
	case KindTop:
		for _, sub := range e.Subreddits {
			for i, child := range sub.Listing.Data.Children {
				var t thing
				if err := json.Unmarshal(child, &t); err != nil {
					return nil, fmt.Errorf("r/%s's post %d: %w", sub.Name, i+1, err)
				}
				entry := saved{Child: child, Thread: sub.Threads[t.Data.Name]}
				var failed struct {
					Error string `json:"error"`
				}
				if json.Unmarshal(entry.Thread, &failed) == nil && failed.Error != "" {
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
				s.Position = max(sub.FirstRank, 1) + i
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
		s.Position = 1
		out = append(out, s)
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
