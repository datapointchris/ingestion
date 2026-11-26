// Package qualitygate is the model stage that reads an item's title and
// opening, before anyone reads it whole, and answers its genre, domains,
// themes, a 1 to 10 quality estimate, a one-line idea, a verdict and a guess
// at its side. Its prompt and answer schema are prompts.Quality.
//
// The service queues an item's quality task in the write that ends its scan,
// where no tag or rule places it, so a rule placed before the model always
// wins. A worker holding a model port runs the task. It stores the dispatch
// in the write that ends its lease, and follows the dispatch on each later
// claim until the run answers, so the task holds no worker slot while the run
// waits. That dispatch-once protocol is modelstage's. When the task ends
// done, the service appends the gate's decision: pass, drop where the model
// dropped the item at or above DropFloor, or review. The side gate then reads
// that decision and the side guess.
//
// The arm asks the same prompt of another model, over items a person placed
// from review, and keeps each answer at its own stage. It decides nothing, so
// `ingestion gates agreement` can set two models' answers side by side.
package qualitygate

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	// Capability is the capability of every quality task.
	Capability = store.StageQuality
	// ArmCapability is the capability of every arm task: the gate's prompt
	// asked of the model that the task names, as a measurement.
	ArmCapability = store.StageQualityArm
	// Producer names the quality gate's outputs, whose version is the
	// prompt's.
	Producer = "quality-gate"
	// DropFloor is the confidence at or above which the model's drop drops
	// an item. Below it, the item waits in review.
	DropFloor = 0.8
)

// VerdictPass is the gate's decision that an item earns a full read. Its drop
// and review are the side gate's verdicts of the same names.
const VerdictPass = "pass"

// The verdicts the model answers.
const (
	AnswerPass   = "pass"
	AnswerDrop   = "drop"
	AnswerUnsure = "unsure"
)

// Causes of the gate's decision.
const (
	CausePass           = "pass"
	CauseDrop           = "drop"
	CauseUnsure         = "unsure"
	CauseBelowDropFloor = "below-drop-floor"
)

// Answer is the model's answer, as prompts/quality/answer.schema.json holds
// it. ReadAnswer decodes it.
type Answer struct {
	Genre             string   `json:"genre"`
	Domains           []string `json:"domains"`
	Themes            []string `json:"themes"`
	Quality           int      `json:"quality"`
	Idea              string   `json:"idea"`
	Verdict           string   `json:"verdict"`
	VerdictConfidence float64  `json:"verdict_confidence"`
	Side              string   `json:"side"`
	SideConfidence    float64  `json:"side_confidence"`
}

// ReadAnswer checks raw against the answer's schema and decodes it.
func ReadAnswer(raw json.RawMessage) (Answer, error) {
	if err := prompts.Quality.Check(raw); err != nil {
		return Answer{}, err
	}
	return DecodeAnswer(raw)
}

// DecodeAnswer decodes a stored answer without checking it against this
// build's schema, so a CLI reads a service of another release. The schema
// counts 7.0 as an integer, so quality decodes as a number and is kept whole.
func DecodeAnswer(raw json.RawMessage) (Answer, error) {
	var a struct {
		Answer
		Quality float64 `json:"quality"`
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the quality answer: %w", err)
	}
	a.Answer.Quality = int(a.Quality)
	return a.Answer, nil
}

// Decision is the gate's decision on an answer that run gave: pass, drop
// where the model dropped the item at or above DropFloor, and review where
// it was unsure or its drop fell below the floor.
func Decision(a Answer, run string) store.Decision {
	confidence := a.VerdictConfidence
	d := store.Decision{Gate: store.GateQuality, DecidedBy: route.ByModel(run), Confidence: &confidence}
	switch {
	case a.Verdict == AnswerPass:
		d.Verdict, d.Cause = VerdictPass, CausePass
	case a.Verdict == AnswerDrop && confidence >= DropFloor:
		d.Verdict, d.Cause = route.VerdictDrop, CauseDrop
	case a.Verdict == AnswerDrop:
		d.Verdict, d.Cause = route.VerdictReview, CauseBelowDropFloor
	default:
		d.Verdict, d.Cause = route.VerdictReview, CauseUnsure
	}
	return d
}

// QueueAfterScan queues the item's quality task in the write that ends its
// scan done, where the scan read the newest parse, no tag or rule places the
// item, and the item never held a quality task. A connector's first fill
// holds it past the first item. A later scan queues none. The gate runs again
// only through a replay that a person confirms, or through `tasks retry` on a
// failed task.
func QueueAfterScan(ctx context.Context, tx *store.Tx, task store.Task) error {
	key := *task.ItemKey
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || !store.ReadingParseScanned(outputs) {
		return err
	}
	if held, err := tx.HasTask(ctx, key, store.StageQuality); err != nil || held {
		return err
	}
	if side, sided, err := tx.NewestDecision(ctx, key, store.GateSide); err != nil || (sided && !route.Provisional(side)) {
		return err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		// The side gate sends the item to review as gate-error. No quality
		// task is queued, so after an apply the item waits as no-guess.
		return nil
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return err
	}
	if _, ruled := route.BeforeModel(set, sightings); ruled {
		return nil
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageQuality, Capability: Capability})
	return err
}

// DecideAfterAnswer appends the gate's decision in the write that ends the
// item's quality task done, from the answer of its newest done output. It
// appends nothing where the newest decision already came from that run.
func DecideAfterAnswer(ctx context.Context, tx *store.Tx, task store.Task) error {
	key := *task.ItemKey
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return err
	}
	newest := store.NewestDone(outputs, store.StageQuality)
	if newest == nil {
		return nil
	}
	a, err := ReadAnswer(newest.Answer)
	if err != nil {
		return fmt.Errorf("read the quality answer on %s: %w", key, err)
	}
	d := Decision(a, newest.Run())
	if held, ok, err := tx.NewestDecision(ctx, key, store.GateQuality); err != nil {
		return err
	} else if ok && held.DecidedBy == d.DecidedBy && held.Verdict == d.Verdict {
		return nil
	}
	d.ItemKey = key
	_, err = tx.AddDecision(ctx, d)
	return err
}

// due says whether a replay runs the gate on an item: the gate is unanswered,
// and the item's side decision, where it has one, is provisional.
func due(r store.ItemRecords, open bool) bool {
	side, sided := r.NewestDecision(store.GateSide)
	return unanswered(r.Outputs, open) && (!sided || route.Provisional(side))
}

// measurementDue says whether a measurement replay runs the gate on an item:
// the gate is unanswered, and the item is labeled, as a person placed it after
// the gates sent it to review. The answer measures the gate against that
// placement.
func measurementDue(r store.ItemRecords, open bool) bool {
	_, labeled := r.ReviewPlacement()
	return unanswered(r.Outputs, open) && labeled
}

// armDue says whether the arm runs on an item: the item is labeled, the gate
// answered it under the prompt in force, and no arm has. So each arm answer
// sits beside the gate's answer to the same prompt.
func armDue(r store.ItemRecords, open bool) bool {
	if open || !store.ReadingParseScanned(r.Outputs) {
		return false
	}
	if _, labeled := r.ReviewPlacement(); !labeled {
		return false
	}
	return answered(r.Outputs, store.StageQuality) && !answered(r.Outputs, store.StageQualityArm)
}

// unanswered says whether the gate may run on an item: its newest parse is
// scanned, no quality task of it is open, and it holds no done answer of the
// prompt in force.
func unanswered(outputs []store.Output, open bool) bool {
	return !open && store.ReadingParseScanned(outputs) && !answered(outputs, store.StageQuality)
}

// answered says whether outputs hold a done answer of the prompt in force at
// stage.
func answered(outputs []store.Output, stage string) bool {
	return slices.ContainsFunc(outputs, func(o store.Output) bool {
		return o.Stage == stage && o.Producer == Producer && o.ProducerVersion == prompts.Quality.Version && o.State == store.OutputDone
	})
}

// Replayable is the gate as a replay runs it again, such as with a new
// prompt, over the items it judges. The replay first answers how many items it
// would run on and the median tokens by kind of the stage's newest stored
// runs, times that count, and queues only once a person confirms.
//
// Its PlacedByPerson runs the gate over the labeled items, which the gate
// otherwise never judges again. Its answer and its decision are kept, and the
// side gate leaves the placement, its delivery and its model read as they
// are, because a person's placement is final.
func Replayable(st *store.Store) server.Replayable {
	measured := replayable(st, store.StageQuality, nil, measurementDue)
	r := replayable(st, store.StageQuality, nil, due)
	r.PlacedByPerson = &measured
	return r
}

// ArmReplayable is the arm as a replay runs it: the gate's prompt, asked of
// the model that the replay names, over each labeled item that the gate
// answered under that prompt. Each answer is kept at the arm's own stage
// beside the gate's, and appends no decision, so the item's phase, placement
// and delivery stay as they are. One arm answers an item under each prompt.
func ArmReplayable(st *store.Store) server.Replayable {
	return server.Replayable{OnModel: func(model string) server.Replayable {
		return replayable(st, store.StageQualityArm, &model, armDue)
	}}
}

// replayable is a replay at stage over each item that isDue says it runs on,
// each task asking for model where it is set.
func replayable(st *store.Store, stage string, model *string, isDue func(r store.ItemRecords, open bool) bool) server.Replayable {
	return server.Replayable{
		Capability: func(store.Item) (string, bool) { return stage, true },
		Queue: func(ctx context.Context, tx *store.Tx, item store.Item) (bool, error) {
			outputs, err := tx.Outputs(ctx, item.Key)
			if err != nil {
				return false, err
			}
			decisions, err := tx.Decisions(ctx, item.Key)
			if err != nil {
				return false, err
			}
			_, open, err := tx.OpenTask(ctx, item.Key, stage)
			if err != nil || !isDue(store.ItemRecords{Outputs: outputs, Decisions: decisions}, open) {
				return false, err
			}
			_, err = tx.Enqueue(ctx, store.Task{ItemKey: &item.Key, Stage: stage, Capability: stage, Model: model})
			return err == nil, err
		},
		Estimate: func(ctx context.Context, items []store.Item) (int, *store.Tokens, error) {
			return modelstage.Estimate(ctx, st, stage, items, isDue)
		},
	}
}
