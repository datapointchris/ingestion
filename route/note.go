package route

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/store"
)

// Draft is what the read gate reads of the note stage's answer, as
// prompts/note/answer.schema.json holds it: the questions for a model read to
// answer, why the item was likely saved, and how sure the draft is.
type Draft struct {
	Questions  []string `json:"questions"`
	WhySaved   string   `json:"why_saved"`
	Confidence float64  `json:"confidence"`
}

// DraftOf decodes the draft that the note stage's output o answered.
func DraftOf(o store.Output) (Draft, error) {
	var d Draft
	if err := json.Unmarshal(o.Answer, &d); err != nil {
		return Draft{}, fmt.Errorf("decode the note's draft: %w", err)
	}
	return d, nil
}

// Note is the draft as the note that a model read answers: its questions,
// then why the item was likely saved.
func (d Draft) Note() string {
	var b strings.Builder
	for _, q := range d.Questions {
		fmt.Fprintf(&b, "- %s\n", strings.TrimSpace(q))
	}
	if why := strings.TrimSpace(d.WhySaved); why != "" {
		fmt.Fprintf(&b, "\nWhy it was likely saved: %s\n", why)
	}
	return strings.TrimSpace(b.String())
}

// NoteOf is the note that a model read under read answers, and whether a
// model drafted it: the person's note where read carries one, the note of
// draft, the item's newest done draft, where read took it, and nil otherwise.
// A draft never reaches a decision's note, so a read takes it by its cause.
func NoteOf(read store.Decision, draft *store.Output) (*string, bool) {
	if store.NoteDigest(read.Note) != nil {
		return read.Note, false
	}
	if read.Cause != CauseNoteDraft || draft == nil {
		return nil, false
	}
	d, err := DraftOf(*draft)
	if err != nil {
		return nil, false
	}
	note := d.Note()
	return &note, true
}

func init() { store.ReadAwaitsNote = awaitsNote }

// awaitsNote says whether read waits on the note stage's draft before a model
// reads the item: it reads by a model under a lens whose note prompt this
// build holds, it carries no person's note and takes no draft, and the item
// holds no done model read under its lens, so an item a model already read
// under that lens is never drafted.
func awaitsNote(read store.Decision, outputs []store.Output) bool {
	if read.Verdict != connectors.ReadModel || read.ByParent() || store.NoteDigest(read.Note) != nil || read.Cause == CauseNoteDraft {
		return false
	}
	if _, ok := prompts.Note[text(read.Lens)]; !ok {
		return false
	}
	return !slices.ContainsFunc(outputs, func(o store.Output) bool { return readDoneUnder(read, o) })
}

// noteDue says whether the read gate queues the note stage on an item whose
// newest read decision is read: the read waits on a draft, no note task of the
// item is open, and the item holds no done draft. A done draft decided the
// read once, so a later placement with no note drafts nothing again.
func noteDue(read store.Decision, outputs []store.Output, open bool) bool {
	return !open && awaitsNote(read, outputs) && store.NewestDone(outputs, store.StageNote) == nil
}

// takeDraft is the read decision of a person's placement with no note, by
// side, where read reads the item by a model and the item holds a done draft:
// read, taking that draft as its note, and nil otherwise. So a placement of a
// drafted item answers the draft rather than waiting on one that no task
// drafts again, and one from note-unclear review takes the draft below the
// floor.
func takeDraft(ctx context.Context, tx *store.Tx, key string, read, side store.Decision) (*store.Decision, error) {
	if read.Verdict != connectors.ReadModel {
		return nil, nil
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || store.NewestDone(outputs, store.StageNote) == nil {
		return nil, err
	}
	read.DecidedBy, read.Cause, read.ClientID = side.DecidedBy, CauseNoteDraft, side.ClientID
	return &read, nil
}

// AfterDraft is the hook that serve runs in the write that ends a note task
// done. Where the item's newest read still waits on a draft, it appends the
// read gate's decision on the newest done draft: a read by a model of the
// draft, as note-draft, where its confidence reaches floor, and a review, as
// note-unclear, below it. That decision then brings the item's model read
// and its chapters in line. A read that a person's note or placement decided
// while the draft ran is left as it is.
func AfterDraft(floor float64) func(context.Context, *store.Tx, store.Task) error {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil {
			return nil
		}
		key := *task.ItemKey
		outputs, err := tx.Outputs(ctx, key)
		if err != nil {
			return err
		}
		draft := store.NewestDone(outputs, store.StageNote)
		if draft == nil {
			return nil
		}
		side, _, err := tx.NewestDecision(ctx, key, store.GateSide)
		if err != nil {
			return err
		}
		read, ok, err := tx.NewestDecision(ctx, key, store.GateRead)
		if err != nil || !ok || !onASide(side.Verdict) || !awaitsNote(read, outputs) {
			return err
		}
		d, err := DraftOf(*draft)
		if err != nil {
			return fmt.Errorf("the note's draft on %s: %w", key, err)
		}
		confidence := d.Confidence
		decision := store.Decision{
			ItemKey: key, Gate: store.GateRead, Verdict: connectors.ReadModel, DecidedBy: ByModel(draft.Run()), Confidence: &confidence,
			Cause: CauseNoteDraft, Lens: read.Lens,
		}
		if confidence < floor {
			decision.Verdict, decision.Cause = VerdictReview, CauseNoteUnclear
		}
		written, err := tx.AddDecision(ctx, decision)
		if err != nil {
			return err
		}
		if err := reconcileModelRead(ctx, tx, key, side, written); err != nil {
			return err
		}
		return PassToChapters(ctx, tx, key)
	}
}
