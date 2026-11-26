package store

import (
	"context"
	"encoding/json"
)

// StageMisses drafts a change to a model stage's prompt from the quality
// gate's misses against a person's word. Its task names a prompt proposal and
// no item, so it sits on no item's line.
const StageMisses = "misses"

// PromptProposal is one draft of a change to a model stage's prompt, as
// prompt_proposals holds it. Serve writes the projection the draft reads, and
// the misses task's run writes the rest, as a model stage writes its output.
type PromptProposal struct {
	ID string `json:"id"`
	// Prompt names the stage whose prompt the draft changes, and
	// PromptVersion the version it changes.
	Prompt        string `json:"prompt"`
	PromptVersion string `json:"prompt_version"`
	// LabeledAfter and LabeledThrough bound when each miss was labeled: after
	// the first, which is nil for the first proposal, and at or before the
	// second.
	LabeledAfter   *string `json:"labeled_after"`
	LabeledThrough string  `json:"labeled_through"`
	MissCount      int     `json:"miss_count"`
	// Misses is the projection that the draft reads, one object per miss.
	Misses json.RawMessage `json:"misses"`
	Zone   Zone            `json:"zone"`
	// Producer and ProducerVersion name the drafting prompt, and are nil
	// until a run is sent.
	Producer        *string `json:"producer"`
	ProducerVersion *string `json:"producer_version"`
	// State is waiting, done or failed, as an output's is.
	State         string          `json:"state"`
	DispatchID    *string         `json:"dispatch_id"`
	RunID         *string         `json:"run_id"`
	Model         *string         `json:"model"`
	Effort        *string         `json:"effort"`
	AnsweredModel *string         `json:"answered_model"`
	Tokens        *Tokens         `json:"tokens"`
	Answer        json.RawMessage `json:"answer"`
	Error         *string         `json:"error"`
	CreatedTS     string          `json:"created_ts"`
	UpdatedTS     string          `json:"updated_ts"`
}

// AddPromptProposal writes p as a new proposal waiting on its draft, under a
// fresh id, and answers it as written.
func (t *Tx) AddPromptProposal(ctx context.Context, p PromptProposal) (PromptProposal, error) {
	p.ID, p.State = randomHex(16), OutputWaiting
	p.CreatedTS, p.UpdatedTS = t.stamp(), t.stamp()
	return p, t.put(ctx, table("prompt_proposals"), p)
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

// PromptProposals reads every proposal, the one whose misses were labeled
// latest first.
func (s *Store) PromptProposals(ctx context.Context) ([]PromptProposal, error) {
	return readInto[PromptProposal](ctx, s.read, table("prompt_proposals"), `1 ORDER BY labeled_through DESC, created_ts DESC`)
}

// DraftOutstanding reads the misses task that is queued or leased, and false
// where none is.
func (t *Tx) DraftOutstanding(ctx context.Context) (Task, bool, error) {
	return first[Task](ctx, t.tx, table("tasks"), `stage = ? AND state IN ('queued', 'leased') ORDER BY created_ts`, StageMisses)
}

// MarksBetween reads every mark made after after, through through, the
// oldest first. An empty after reads from the first mark.
func (s *Store) MarksBetween(ctx context.Context, after, through string) ([]Mark, error) {
	return readInto[Mark](ctx, s.read, table("marks"), `marked_ts > ? AND marked_ts <= ? ORDER BY marked_ts, item_key, sequence`, after, through)
}
