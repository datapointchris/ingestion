// Package push hands one item that the service holds straight to a program on
// this machine. psyche is the one such program. Its relate reads the item's
// parse, the findings of the scan that read it and, where asked, what the
// service derived from the parse, as one parse-input document on stdin.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Program is the program that a push to psyche runs, found on PATH.
const Program = "psyche"

// SchemaVersion is the version of psyche's parse-input schema that Write
// writes. psyche reads its newest version and the one before it.
const SchemaVersion = 1

// suggestionKinds maps each of ingestion's own rules that flags what the text
// suggests a reader run, open or paste to the kind psyche lists it under.
var suggestionKinds = map[string]string{
	"ingestion.command": "command",
	"ingestion.link":    "link",
	"ingestion.code":    "code",
}

// ErrNotScanned is a parse that no scan has read, which a push never hands on.
var ErrNotScanned = errors.New("no scan has read the parse")

// ErrNoText is a parse whose text is whitespace alone, which leaves psyche
// nothing to relate.
var ErrNoText = errors.New("the parse holds no text")

// Document is one parsed source as `psyche relate --parse` reads it.
type Document struct {
	SchemaVersion    int                   `json:"schema_version"`
	Key              string                `json:"key"`
	URL              *string               `json:"url"`
	Title            *string               `json:"title"`
	Zone             store.Zone            `json:"zone"`
	MediaType        string                `json:"media_type"`
	Extractor        string                `json:"extractor"`
	ExtractorVersion string                `json:"extractor_version"`
	Text             string                `json:"text"`
	Units            []store.Unit          `json:"units"`
	RulesVersion     *string               `json:"rules_version"`
	Findings         []server.ParseFinding `json:"findings"`
	Suggested        []Suggestion          `json:"suggested"`
	Enriched         *Enriched             `json:"enriched,omitempty"`
}

// Suggestion is one command, link or piece of code that the text suggests,
// from the scan's finding that matched it.
type Suggestion struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
	Unit *int   `json:"unit"`
}

// Enriched is what the service derived from the parse. Each field is absent
// where no stage answered it.
type Enriched struct {
	Summary    *string  `json:"summary,omitempty"`
	Idea       *string  `json:"idea,omitempty"`
	Suggestion *string  `json:"suggestion,omitempty"`
	Genre      *string  `json:"genre,omitempty"`
	Domains    []string `json:"domains,omitempty"`
	Themes     []string `json:"themes,omitempty"`
	Quality    *int     `json:"quality,omitempty"`
}

// Write is the document for parse, carrying enriched where it is not nil. Each
// finding of a suggestion rule moves to Suggested, and every other stays a
// finding.
func Write(parse server.ParseAnswer, enriched *Enriched) (Document, error) {
	if parse.RulesVersion == nil {
		return Document{}, ErrNotScanned
	}
	if strings.TrimSpace(parse.Text) == "" {
		return Document{}, ErrNoText
	}
	doc := Document{
		SchemaVersion: SchemaVersion, Key: parse.Key, URL: parse.URL, Title: parse.Title, Zone: parse.Zone,
		MediaType: parse.MediaType, Extractor: parse.Extractor, ExtractorVersion: parse.ExtractorVersion, Text: parse.Text,
		Units: parse.Units, RulesVersion: parse.RulesVersion, Findings: []server.ParseFinding{}, Suggested: []Suggestion{},
		Enriched: enriched,
	}
	if doc.Units == nil {
		doc.Units = []store.Unit{}
	}
	for _, f := range parse.Findings {
		kind, ok := suggestionKinds[f.Rule]
		switch {
		case !ok:
			doc.Findings = append(doc.Findings, f)
		case f.Span != "":
			doc.Suggested = append(doc.Suggested, Suggestion{Kind: kind, Text: f.Span, Unit: f.Unit})
		}
	}
	return doc, nil
}

// EnrichedFrom is what item's done outputs derived from its parse: the newest
// quality gate answer's idea, genre, domains, themes and quality, and the newest
// model read's summary and suggestion. It is nil where neither stage answered.
func EnrichedFrom(item server.ItemAnswer) *Enriched {
	var e Enriched
	answered := false
	if raw := newestDone(item.Outputs, store.StageQuality); raw != nil {
		// Quality decodes as a number, because a model may answer 7.0.
		var a struct {
			Genre   string   `json:"genre"`
			Idea    string   `json:"idea"`
			Domains []string `json:"domains"`
			Themes  []string `json:"themes"`
			Quality float64  `json:"quality"`
		}
		if json.Unmarshal(raw, &a) == nil {
			answered = true
			e.Genre, e.Idea, e.Domains, e.Themes = nonEmpty(a.Genre), nonEmpty(a.Idea), a.Domains, a.Themes
			if a.Quality >= 1 && a.Quality <= 10 {
				q := int(a.Quality)
				e.Quality = &q
			}
		}
	}
	if raw := newestDone(item.Outputs, store.StageModelRead); raw != nil {
		var a modelread.Answer
		if json.Unmarshal(raw, &a) == nil && (a.Summary != nil || a.Suggestion != nil) {
			answered = true
			e.Summary, e.Suggestion = a.Summary, a.Suggestion
		}
	}
	if !answered {
		return nil
	}
	return &e
}

// newestDone is the answer of the last done output at stage. The service lists
// an item's outputs oldest first.
func newestDone(outputs []server.OutputView, stage string) json.RawMessage {
	var newest json.RawMessage
	for _, o := range outputs {
		if o.Stage == stage && o.State == store.OutputDone && len(o.Answer) > 0 {
			newest = o.Answer
		}
	}
	return newest
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Relate runs `psyche relate --parse -` on this machine with doc on its stdin,
// adding --json where asJSON is set. psyche writes to stdout and stderr as it
// runs.
func Relate(ctx context.Context, doc Document, asJSON bool, stdout, stderr io.Writer) error {
	path, err := exec.LookPath(Program)
	if err != nil {
		return fmt.Errorf("a push to %s runs %s relate on this machine, and %w", Program, Program, err)
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	args := []string{"relate", "--parse", "-"}
	if asJSON {
		args = append(args, "--json")
	}
	c := exec.CommandContext(ctx, path, args...)
	c.Stdin, c.Stdout, c.Stderr = bytes.NewReader(body), stdout, stderr
	err = c.Run()
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit) && exit.ExitCode() == 2:
		return fmt.Errorf("%s relate refused the document with exit 2, naming why above. "+
			"A %s that reads no parse-input schema_version %d refuses it by its version", Program, Program, SchemaVersion)
	case errors.As(err, &exit):
		return fmt.Errorf("%s relate failed with exit %d", Program, exit.ExitCode())
	case err != nil:
		return fmt.Errorf("run %s relate: %w", Program, err)
	}
	return nil
}
