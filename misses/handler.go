package misses

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// DefaultModel and DefaultEffort are what a draft's run is asked for
	// where config names neither. A draft weighs a whole prompt against a
	// week's misses, once a week, so it asks for more than the gate does.
	DefaultModel  = "sonnet"
	DefaultEffort = "medium"
)

// Answer is a draft, as prompts/misses/answer.schema.json holds it.
type Answer struct {
	Pattern string `json:"pattern"`
	Change  string `json:"change"`
	Edits   []Edit `json:"edits"`
	Answers []int  `json:"answers"`
}

// Edit replaces the one passage of the prompt that Before quotes with After.
type Edit struct {
	Before string `json:"before"`
	After  string `json:"after"`
}

// NewHandler is the worker handler that drafts each proposal through port
// under modelstage's dispatch-once protocol.
func NewHandler(port modelport.Port, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Misses, nil },
		Data:   data,
		Keep:   keep,
	}, settings, log)
}

// promptData is what the misses prompt quotes.
type promptData struct {
	Prompt, PromptVersion, Template string
	DropFloor, GuessFloor           string
	Count                           int
	Window, Misses                  string
}

// target is the prompt that the task's proposal changes, as this build holds
// it. Where this build's version of the prompt differs from the proposal's,
// the draft would quote a prompt that did not answer the misses, so target
// refuses it.
func target(task worker.Task) (prompts.Prompt, *server.LeasedProposal, error) {
	p := task.Proposal
	if p == nil {
		return prompts.Prompt{}, nil, fmt.Errorf("%w: task %s carries no prompt proposal", modelport.ErrRefused, task.ID)
	}
	changed, ok := prompts.Changeable[p.Prompt]
	if !ok {
		return prompts.Prompt{}, nil, fmt.Errorf("%w: proposal %s changes the %s prompt, which no draft changes", modelport.ErrRefused,
			deref(task.ProposalID), p.Prompt)
	}
	if changed.Version != p.PromptVersion {
		return prompts.Prompt{}, nil, fmt.Errorf("%w: proposal %s changes the %s prompt at %s, and this build holds it at %s",
			modelport.ErrRefused, deref(task.ProposalID), p.Prompt, p.PromptVersion, changed.Version)
	}
	return changed, p, nil
}

// data is what the prompt quotes of the task's proposal: the prompt that it
// changes, and its misses, one JSON object a line.
func data(_ context.Context, task worker.Task) (any, error) {
	changed, p, err := target(task)
	if err != nil {
		return nil, err
	}
	var misses []json.RawMessage
	if err := json.Unmarshal(p.Misses, &misses); err != nil {
		return nil, fmt.Errorf("%w: proposal %s's misses: %w", modelport.ErrRefused, deref(task.ProposalID), err)
	}
	lines := make([]string, len(misses))
	for i, m := range misses {
		var line bytes.Buffer
		if err := json.Compact(&line, m); err != nil {
			return nil, fmt.Errorf("%w: proposal %s's miss %d: %w", modelport.ErrRefused, deref(task.ProposalID), i+1, err)
		}
		lines[i] = line.String()
	}
	window := "through " + p.LabeledThrough
	if p.LabeledAfter != nil {
		window = "after " + *p.LabeledAfter + " and " + window
	}
	return promptData{
		Prompt: p.Prompt, PromptVersion: p.PromptVersion, Template: changed.Text(),
		DropFloor: fmt.Sprintf("%.2f", qualitygate.DropFloor), GuessFloor: fmt.Sprintf("%.2f", route.GuessFloor),
		Count: len(lines), Window: window, Misses: strings.Join(lines, "\n"),
	}, nil
}

// keep checks a draft: it holds to its schema, each edit's before occurs once
// in the prompt that it changes with no two overlapping, and each miss it
// answers is one of the proposal's. Applying its edits then gives the
// proposed prompt.
func keep(_ context.Context, task worker.Task, prompt prompts.Prompt, raw json.RawMessage, _ *server.ReportOutput) ([]any, error) {
	if err := prompt.Check(raw); err != nil {
		return nil, err
	}
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, fmt.Errorf("decode the draft: %w", err)
	}
	changed, p, err := target(task)
	if err != nil {
		return nil, err
	}
	if err := applies(changed.Text(), a.Edits); err != nil {
		return nil, fmt.Errorf("the draft's edits do not apply to the %s prompt at %s: %w", p.Prompt, p.PromptVersion, err)
	}
	for _, n := range a.Answers {
		if n > p.MissCount {
			return nil, fmt.Errorf("the draft answers miss %d, and the proposal holds %d", n, p.MissCount)
		}
	}
	return []any{"edits", len(a.Edits), "answers", len(a.Answers), "misses", p.MissCount}, nil
}

// errEditsOverlap is two edits whose passages share text.
var errEditsOverlap = errors.New("two edits replace overlapping passages")

// applies says why edits do not apply to text, and nil where each before
// occurs once in text and no two overlap.
func applies(text string, edits []Edit) error {
	type span struct{ from, to int }
	spans := make([]span, len(edits))
	for i, e := range edits {
		if n := strings.Count(text, e.Before); n != 1 {
			return fmt.Errorf("edit %d's before occurs %d times, not once", i+1, n)
		}
		from := strings.Index(text, e.Before)
		spans[i] = span{from, from + len(e.Before)}
	}
	slices.SortFunc(spans, func(a, b span) int { return cmp.Compare(a.from, b.from) })
	for i := 1; i < len(spans); i++ {
		if spans[i].from < spans[i-1].to {
			return errEditsOverlap
		}
	}
	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
