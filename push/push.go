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
	"time"

	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Program is the program that a push to psyche runs, found on PATH. A link
// that a push lands is placed at the destination of the same name.
const Program = "psyche"

// Connector is the push connector that a new link lands through, where the
// push names no other.
const Connector = "cli-push"

// Wait is how long a push waits for an item's parse and the scan of it, where
// it names no other wait. A captions parse waits its turn at ypl's door before
// its read, so Wait leaves room for the longest door wait and captions read
// that ypl publishes, with the claims and the scan after them.
const Wait = 10 * time.Minute

// WaitingOn names the stage that item waits at on its way to a scanned parse,
// store.StageParse or store.StageScan. Where that stage failed, failure says
// why, so a push stops waiting. A parse that defers to another extractor
// still waits.
func WaitingOn(item server.ItemAnswer) (stage, failure string) {
	stage = store.StageParse
	if output(item, stage, store.OutputDone) != nil {
		stage = store.StageScan
	}
	if output(item, stage, store.OutputDone) != nil {
		return stage, ""
	}
	failed := output(item, stage, store.OutputFailed)
	switch {
	case failed == nil:
		return stage, ""
	case failed.Error == nil:
		return stage, "its run failed and gave no reason"
	}
	return stage, *failed.Error
}

// output is item's output of stage in state, and nil where it holds none.
func output(item server.ItemAnswer, stage, state string) *server.OutputView {
	for i, o := range item.Outputs {
		if o.Stage == stage && o.State == state {
			return &item.Outputs[i]
		}
	}
	return nil
}

// SchemaVersion is the version of psyche's parse-input schema that Write
// writes, the one that testdata/parse-input.schema.json holds.
const SchemaVersion = 1

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
// finding of a rule that scan.Suggests names moves to Suggested, and every
// other stays a finding.
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
		kind, ok := scan.Suggests(f.Rule)
		switch {
		case !ok:
			doc.Findings = append(doc.Findings, f)
		case f.Span != "":
			doc.Suggested = append(doc.Suggested, Suggestion{Kind: kind, Text: f.Span, Unit: f.Unit})
		}
	}
	return doc, nil
}

// Enrichment is what --enriched adds to the document, and why each part that
// it leaves out is missing.
type Enrichment struct {
	// Enriched is nil where no stage's answer could be read.
	Enriched *Enriched
	// From names each stage whose answer Enriched carries.
	From []string
	// Left says, in a sentence each, which part was left out and why.
	Left []string
}

// The stages that --enriched reads, as a person names them.
const (
	FromQualityGate = "the quality gate"
	FromModelRead   = "the model read"
)

// Enrich is what the service's current answers on item add: the quality
// gate's answer and the model read's summary and suggestion. The model read's
// sentence goes only where no scan finding lies over it, because psyche's
// document holds no place for a finding in it. The summary and the quality
// gate's words go unscanned. Each restates the source text, which psyche
// reads whole beside its findings.
func Enrich(item server.ItemAnswer) Enrichment {
	var (
		e   Enrichment
		out Enriched
		key = item.Item.Key
	)
	switch q, why := quality(item); {
	case why != "":
		e.Left = append(e.Left, why)
	default:
		out.Genre, out.Idea, out.Domains, out.Themes = nonEmpty(q.Genre), nonEmpty(q.Idea), q.Domains, q.Themes
		if q.Quality >= 1 && q.Quality <= 10 {
			out.Quality = &q.Quality
		} else {
			e.Left = append(e.Left, fmt.Sprintf("%s's quality gate answered quality %d, outside 1 to 10, so the quality is left out", key, q.Quality))
		}
		e.From = append(e.From, FromQualityGate)
		e.Left = append(e.Left, newerWaits(item, store.StageQuality, "quality gate answer")...)
	}
	switch read, why := modelRead(item); {
	case why != "":
		e.Left = append(e.Left, why)
	default:
		out.Summary = read.Summary
		if read.Suggestion != nil {
			if rules := findingRules(item.ModelRead.Findings); len(rules) > 0 {
				e.Left = append(e.Left, fmt.Sprintf("%s's model read suggestion is left out, because the scan flagged it: %s", key, strings.Join(rules, ", ")))
			} else {
				out.Suggestion = read.Suggestion
			}
		}
		e.From = append(e.From, FromModelRead)
		e.Left = append(e.Left, newerWaits(item, store.StageModelRead, "model read")...)
	}
	if len(e.From) > 0 {
		e.Enriched = &out
	}
	return e
}

// quality is item's quality gate answer, or why it carries none.
func quality(item server.ItemAnswer) (qualitygate.Answer, string) {
	key := item.Item.Key
	if len(item.QualityAnswer) == 0 || string(item.QualityAnswer) == "null" {
		return qualitygate.Answer{}, key + " holds no quality gate answer" + missingWords(item, store.StageQuality)
	}
	q, err := qualitygate.DecodeAnswer(item.QualityAnswer)
	if err != nil {
		return q, fmt.Sprintf("%s's quality gate answer does not read, so it is left out: %v", key, err)
	}
	if q.Genre == "" && q.Idea == "" && len(q.Domains) == 0 && len(q.Themes) == 0 && q.Quality == 0 {
		return q, key + "'s quality gate answer holds none of the fields a push reads, so it is left out"
	}
	return q, ""
}

// modelRead is item's newest model read's answer, or why it carries none.
func modelRead(item server.ItemAnswer) (modelread.Answer, string) {
	key := item.Item.Key
	if item.ModelRead == nil {
		return modelread.Answer{}, key + " holds no model read" + missingWords(item, store.StageModelRead)
	}
	var a modelread.Answer
	if err := json.Unmarshal(item.ModelRead.Answer, &a); err != nil {
		return a, fmt.Sprintf("%s's model read does not read, so it is left out: %v", key, err)
	}
	if a.Summary == nil && a.Suggestion == nil {
		return a, key + "'s model read holds neither a summary nor a suggestion"
	}
	return a, ""
}

// missingWords says why item holds no done answer at stage: a run that waits
// there, or one that failed.
func missingWords(item server.ItemAnswer, stage string) string {
	if item.Waiting != nil && item.Waiting.Stage == stage {
		return " yet: " + item.Waiting.Message
	}
	for i := len(item.Outputs) - 1; i >= 0; i-- {
		if o := item.Outputs[i]; o.Stage == stage && o.State == store.OutputFailed && o.Error != nil {
			return ", because its run failed: " + *o.Error
		}
	}
	return ""
}

// newerWaits says that a newer run waits at stage, where one does, so the
// push carries the answer before it.
func newerWaits(item server.ItemAnswer, stage, answer string) []string {
	if item.Waiting == nil || item.Waiting.Stage != stage {
		return nil
	}
	return []string{fmt.Sprintf("%s's newer %s waits (%s), so the push carries the one before it", item.Item.Key, answer, item.Waiting.Message)}
}

// findingRules are the rules of findings, each once, in order.
func findingRules(findings []server.FindingView) []string {
	var rules []string
	for _, f := range findings {
		if !contains(rules, f.Rule) {
			rules = append(rules, f.Rule)
		}
	}
	return rules
}

func contains(list []string, s string) bool {
	for _, have := range list {
		if have == s {
			return true
		}
	}
	return false
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
		return fmt.Errorf("%s relate exited 2: it refused its command line or the document, and its own message says which, on stdout under --json. "+
			"`%s relate --help` lists --parse where it reads a parse, and the push wrote parse-input schema_version %d", Program, Program, SchemaVersion)
	case errors.As(err, &exit):
		return fmt.Errorf("%s relate failed: %s", Program, exit.ProcessState)
	case err != nil:
		return fmt.Errorf("run %s relate: %w", Program, err)
	}
	return nil
}
