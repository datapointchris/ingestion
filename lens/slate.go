package lens

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Slate is the context reader of a slate tracker: its focus in force and its
// active initiatives, read under SlateScopes. Each initiative in the list
// carries its name, summary and pursuits, which is what a prompt quotes, so
// the reader asks no initiative's own route.
const Slate = "slate"

// SlateScopes is what the slate reader's service client asks for.
var SlateScopes = []string{"slate.records.read"}

func init() {
	Register(Slate, Registration{Scopes: SlateScopes, Open: func(api string, client *http.Client) Reader {
		return Cached(slateReader{api: strings.TrimRight(api, "/"), client: client})
	}})
}

type slateReader struct {
	api    string
	client *http.Client
}

// slateFocus is the part of GET /api/v1/focus that the reader keeps.
type slateFocus struct {
	Focus *struct {
		Statement string `json:"statement"`
		UntilDate string `json:"until_date"`
		Shares    []struct {
			Pursuit slatePursuit `json:"pursuit"`
			Share   float64      `json:"share"`
		} `json:"shares"`
	} `json:"focus"`
}

// slateInitiative is the part of each row of GET /api/v1/initiatives that
// the reader keeps.
type slateInitiative struct {
	Number   int            `json:"number"`
	Name     string         `json:"name"`
	Summary  string         `json:"summary"`
	Pursuits []slatePursuit `json:"pursuits"`
}

type slatePursuit struct {
	Number int    `json:"number"`
	Name   string `json:"name"`
}

func (r slateReader) Read(ctx context.Context) (Context, error) {
	var focus slateFocus
	if err := r.get(ctx, "/api/v1/focus", &focus); err != nil {
		return Context{}, err
	}
	var initiatives []slateInitiative
	if err := r.get(ctx, "/api/v1/initiatives?status=active", &initiatives); err != nil {
		return Context{}, err
	}
	var c Context
	if f := focus.Focus; f != nil {
		c.Focus = &Focus{Statement: f.Statement, Until: f.UntilDate}
		for _, s := range f.Shares {
			c.Focus.Shares = append(c.Focus.Shares, Share{Pursuit: s.Pursuit.Name, Share: s.Share})
		}
	}
	for _, i := range initiatives {
		in := Initiative{Number: i.Number, Name: i.Name, Summary: i.Summary}
		for _, p := range i.Pursuits {
			in.Pursuits = append(in.Pursuits, p.Name)
		}
		c.Initiatives = append(c.Initiatives, in)
	}
	return c, nil
}

// get reads path from slate's API into into, and says why where slate
// answers other than 200.
func (r slateReader) get(ctx context.Context, path string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.api+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s answered %s: %s", path, resp.Status, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		return fmt.Errorf("decode GET %s: %w", path, err)
	}
	return nil
}
