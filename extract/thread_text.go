package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/datapointchris/ingestion/redditexport"
)

// ThreadText parses a Reddit save: its post, then each comment of its thread
// depth first, in the order Reddit sorted them. A comment that its author
// deleted or a moderator removed is left out, and its replies are kept. Each
// unit's label is its author.
var ThreadText = Extractor{
	Name: "thread-text", Version: "1", MediaType: redditexport.MediaTypeSave,
	Extract: func(ctx context.Context, body io.Reader) (Read, error) {
		units, err := threadText(ctx, body)
		return Read{Units: units}, err
	},
}

// save is a Reddit save as redditexport splits it: the thing saved, and its
// thread as Reddit answers it, a listing of the post and then a listing of its
// comments. A save whose thread Reddit refused holds null.
type save struct {
	Child  thing           `json:"child"`
	Thread json.RawMessage `json:"thread"`
}

// thing is a post (t3), a comment (t1), or the placeholder (more) that stands
// for unloaded comments.
type thing struct {
	Kind string `json:"kind"`
	Data struct {
		Author     string  `json:"author"`
		Title      string  `json:"title"`
		Selftext   string  `json:"selftext"`
		IsSelf     bool    `json:"is_self"`
		URL        string  `json:"url"`
		Body       string  `json:"body"`
		LinkTitle  string  `json:"link_title"`
		LinkAuthor string  `json:"link_author"`
		Replies    replies `json:"replies"`
	} `json:"data"`
}

type listing struct {
	Data struct {
		Children []thing `json:"children"`
	} `json:"data"`
}

// replies is a comment's replies: a listing, or the empty string that Reddit
// writes where there are none.
type replies struct{ listing }

func (r *replies) UnmarshalJSON(raw []byte) error {
	if len(raw) > 0 && raw[0] == '"' {
		return nil
	}
	return json.Unmarshal(raw, &r.listing)
}

func threadText(_ context.Context, body io.Reader) ([]Unit, error) {
	var s save
	if err := json.NewDecoder(body).Decode(&s); err != nil {
		return nil, fmt.Errorf("not a Reddit save: %w", err)
	}
	var thread []listing
	if len(s.Thread) > 0 && string(s.Thread) != "null" {
		if err := json.Unmarshal(s.Thread, &thread); err != nil {
			return nil, fmt.Errorf("the save's thread is not Reddit's pair of listings: %w", err)
		}
	}
	var units []Unit
	if post, ok := postOf(s.Child, thread); ok {
		units = append(units, post)
	}
	switch {
	case len(thread) > 1:
		units = comments(thread[1].Data.Children, units)
	case s.Child.Kind == "t1":
		units = comments([]thing{s.Child}, units)
	}
	return units, nil
}

// postOf is the unit of the save's post: from its thread, or else from the
// save itself, which for a comment names only its post's title.
func postOf(child thing, thread []listing) (Unit, bool) {
	if len(thread) > 0 && len(thread[0].Data.Children) > 0 && thread[0].Data.Children[0].Kind == "t3" {
		child = thread[0].Data.Children[0]
	}
	d := child.Data
	var parts []string
	switch child.Kind {
	case "t3":
		parts = append(parts, d.Title)
		if kept(d.Selftext) {
			parts = append(parts, d.Selftext)
		}
		if !d.IsSelf && d.URL != "" {
			parts = append(parts, d.URL)
		}
	case "t1":
		parts, d.Author = append(parts, d.LinkTitle), d.LinkAuthor
	}
	text := strings.TrimSpace(strings.Join(parts, "\n\n"))
	return Unit{Label: author(d.Author), Text: text}, text != ""
}

// comments appends each comment below children to units, depth first.
func comments(children []thing, units []Unit) []Unit {
	for _, c := range children {
		if c.Kind != "t1" {
			continue
		}
		if kept(c.Data.Body) {
			units = append(units, Unit{Label: author(c.Data.Author), Text: c.Data.Body})
		}
		units = comments(c.Data.Replies.Data.Children, units)
	}
	return units
}

// kept says whether text holds words rather than nothing or Reddit's mark of
// a deletion or removal.
func kept(text string) bool {
	text = strings.TrimSpace(text)
	return text != "" && text != "[deleted]" && text != "[removed]"
}

// author is name as a label, u/<name>, and nil where the account is deleted.
func author(name string) *string {
	if name == "" || name == "[deleted]" {
		return nil
	}
	label := "u/" + name
	return &label
}
