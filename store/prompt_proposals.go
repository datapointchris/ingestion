package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// StagePromptProposal drafts a change to a model stage's prompt from the
// quality gate's misses against a person's word. Its task names a prompt
// proposal and no item, so it sits on no item's line.
const StagePromptProposal = "prompt-proposal"

// PromptProposal is one draft of a change to a model stage's prompt, as
// prompt_proposals holds it. Serve writes the prompt and the projection that
// the draft reads, and the task's run writes its StageRun, as a model stage
// writes an item's output.
type PromptProposal struct {
	ID string `json:"id"`
	// Prompt names the stage whose prompt the draft changes, PromptVersion
	// the version it changes, and PromptText that version's template, so a
	// worker of another build drafts against the same text.
	Prompt        string `json:"prompt"`
	PromptVersion string `json:"prompt_version"`
	PromptText    string `json:"prompt_text"`
	// LabeledAfter and LabeledThrough bound when each miss was labeled: after
	// the first, which is nil where no earlier draft is done, and at or before
	// the second.
	LabeledAfter   *string `json:"labeled_after"`
	LabeledThrough string  `json:"labeled_through"`
	MissCount      int     `json:"miss_count"`
	// Misses is the projection that the draft reads, one object per miss.
	Misses json.RawMessage `json:"misses"`
	Zone   Zone            `json:"zone"`
	StageRun
	CreatedTS string `json:"created_ts"`
	UpdatedTS string `json:"updated_ts"`
}

// AddPromptProposal writes p as a new proposal waiting on its draft, under a
// fresh id, and answers it as written.
func (t *Tx) AddPromptProposal(ctx context.Context, p PromptProposal) (PromptProposal, error) {
	p.ID, p.State = randomHex(16), OutputWaiting
	p.CreatedTS, p.UpdatedTS = t.stamp(), t.stamp()
	return p, t.put(ctx, table("prompt_proposals"), p)
}

// keepProposalRun writes run onto the proposal under id that a task drafts.
func (t *Tx) keepProposalRun(ctx context.Context, id string, run StageRun) error {
	p, ok, err := t.PromptProposal(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("a task drafts prompt proposal %s, which the store does not hold", id)
	}
	p.StageRun = run
	return t.PutPromptProposal(ctx, p)
}

// failProposalRun fails the run of the proposal under id for reason, where
// the task that drafts it ended without a report, and keeps the dispatch that
// a retry replaces.
func (t *Tx) failProposalRun(ctx context.Context, id, reason string) error {
	p, ok, err := t.PromptProposal(ctx, id)
	if err != nil || !ok || p.State == OutputDone {
		return err
	}
	p.State, p.Error = OutputFailed, &reason
	return t.PutPromptProposal(ctx, p)
}

// PutPromptProposal writes p again in place, keeping when it was created.
func (t *Tx) PutPromptProposal(ctx context.Context, p PromptProposal) error {
	p.CreatedTS, p.UpdatedTS = t.stamp(), t.stamp()
	return t.put(ctx, table("prompt_proposals"), p)
}

// PromptProposal reads the proposal under id as this write sees it, and
// false where the store holds none.
func (t *Tx) PromptProposal(ctx context.Context, id string) (PromptProposal, bool, error) {
	return first[PromptProposal](ctx, t.tx, table("prompt_proposals"), `id = ?`, id)
}

// NewestPromptProposal reads the proposal whose misses were labeled latest,
// as this write sees it, and false where the store holds none.
func (t *Tx) NewestPromptProposal(ctx context.Context) (PromptProposal, bool, error) {
	return first[PromptProposal](ctx, t.tx, table("prompt_proposals"), `1 ORDER BY labeled_through DESC, created_ts DESC`)
}

// NewestPromptProposal reads the proposal whose misses were labeled latest,
// and false where the store holds none.
func (s *Store) NewestPromptProposal(ctx context.Context) (PromptProposal, bool, error) {
	return first[PromptProposal](ctx, s.read, table("prompt_proposals"), `1 ORDER BY labeled_through DESC, created_ts DESC`)
}

// NewestDonePromptProposal reads the done proposal whose misses were labeled
// latest, and false where no draft is done. Its labeled_through is where the
// next draft's misses start, so a failed draft's misses roll into the next.
func (s *Store) NewestDonePromptProposal(ctx context.Context) (PromptProposal, bool, error) {
	return first[PromptProposal](ctx, s.read, table("prompt_proposals"), `state = ? ORDER BY labeled_through DESC, created_ts DESC`, OutputDone)
}

// PromptProposals reads every proposal, the one whose misses were labeled
// latest first.
func (s *Store) PromptProposals(ctx context.Context) ([]PromptProposal, error) {
	return readInto[PromptProposal](ctx, s.read, table("prompt_proposals"), `1 ORDER BY labeled_through DESC, created_ts DESC`)
}

// DraftOutstanding reads the prompt-proposal task that is queued or leased,
// and false where none is.
func (t *Tx) DraftOutstanding(ctx context.Context) (Task, bool, error) {
	return first[Task](ctx, t.tx, table("tasks"), `stage = ? AND state IN ('queued', 'leased') ORDER BY created_ts`, StagePromptProposal)
}

// MarksBetween reads every mark made after after, through through, the
// oldest first. An empty after reads from the first mark.
func (s *Store) MarksBetween(ctx context.Context, after, through string) ([]Mark, error) {
	return readInto[Mark](ctx, s.read, table("marks"), `marked_ts > ? AND marked_ts <= ? ORDER BY marked_ts, item_key, sequence`, after, through)
}
