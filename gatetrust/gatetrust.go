// Package gatetrust is how far a person trusts the quality gate's drops. A
// family is one connector under one version of the gate's prompt, so a new
// prompt starts every family again at rung 0. A family climbs the earnedtrust
// ladder on a person's word about the gate's answers. A mark of low-quality
// agrees with a drop and disagrees with a keep. A mark of read agrees with a
// keep and disagrees with a drop. A person who places an item that the gate
// would drop on any destination disagrees with the drop. Any disagreement
// returns the family to rung 0.
//
// While its family stands on rung 0, every item that the gate drops waits in
// review, marked gate-would-drop. A family whose straight run reaches Rule's
// Run asks a person to promote it, and one yes does. A promoted family shows
// its rung's share of the gate's drops, picked by a seeded draw over the
// family and the item key, and the side gate drops the rest with their rows
// kept. This is a continuous sampling plan, as earnedtrust names it. Its rule
// is earnedtrust's cautious pace, because a hidden item gives the person no
// prompt to undo it.
package gatetrust

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/datapointchris/earnedtrust"
	"github.com/datapointchris/earnedtrust/seededhash"

	"github.com/datapointchris/ingestion/store"
)

// Rule is the ladder that every family climbs, at earnedtrust's cautious
// pace. A straight run of CostlyUndo agreements by a person earns asking, a
// person's yes climbs to rung 1, and each rung shows a lower share of the
// gate's drops. Only a person's word counts, so the gate never promotes
// itself.
var Rule = cautiousByPerson()

func cautiousByPerson() earnedtrust.Rule {
	r := earnedtrust.PaceRule(earnedtrust.PaceCautious)
	r.ByPerson, r.PersonPromotes = true, true
	return r
}

// The words that a person marks an item with.
const (
	// MarkLowQuality says the item held nothing worth keeping.
	MarkLowQuality = "low-quality"
	// MarkRead says the item was read and gave what was wanted.
	MarkRead = "read"
)

// Marks lists every mark.
var Marks = []string{MarkLowQuality, MarkRead}

// How a mark reads against the gate's decision.
const (
	Agreed    = "agreed"
	Disagreed = "disagreed"
	Neither   = "neither"
)

// What moves a family's standing, as its last_change records.
const (
	// ChangeGateAnswer is the gate's first answer under the family, which
	// starts it at rung 0.
	ChangeGateAnswer = "gate-answer"
	ChangeMark       = "mark"
	// ChangePlacement is a person's placement of an item that the gate would
	// drop.
	ChangePlacement = "placement"
	ChangePromotion = "promotion"
)

// The quality gate's verdicts, as its decisions write them. qualitygate and
// route name the same words.
const (
	verdictPass = "pass"
	verdictDrop = "drop"
)

// ErrNoFamily is a promotion naming a family the store holds no standing of.
var ErrNoFamily = errors.New("no such gate family")

// Family is the name of connector's family under the gate's prompt version.
func Family(connector, promptVersion string) string { return connector + "@" + promptVersion }

// Judgment is the quality gate's newest decision on an item, and the version
// of the prompt that answered it.
type Judgment struct {
	store.Decision
	PromptVersion string
}

// JudgmentOn reads the gate's newest decision on the item under key, and the
// connectors of the item's sightings, the first seen first. A book's chapter
// takes its book's answer and its book's connectors. It is false where the
// gate has decided nothing.
func JudgmentOn(ctx context.Context, tx *store.Tx, key string) (Judgment, []string, bool, error) {
	item, ok, err := tx.Item(ctx, key)
	if err != nil || !ok {
		return Judgment{}, nil, false, err
	}
	if item.PlacedThroughParent() {
		key = *item.ParentKey
	}
	d, ok, err := tx.NewestDecision(ctx, key, store.GateQuality)
	if err != nil || !ok {
		return Judgment{}, nil, false, err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return Judgment{}, nil, false, err
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return Judgment{}, nil, false, err
	}
	return Judgment{Decision: d, PromptVersion: PromptVersion(outputs, d)}, Connectors(sightings), true, nil
}

// PromptVersion is the version of the gate's prompt whose answer d came
// from: the done quality output among outputs whose run d names, and "" where
// none is.
func PromptVersion(outputs []store.Output, d store.Decision) string {
	run, _ := strings.CutPrefix(d.DecidedBy, "model:")
	for _, o := range outputs {
		if o.Stage == store.StageQuality && o.State == store.OutputDone && o.Run() == run {
			return o.ProducerVersion
		}
	}
	return ""
}

// Connectors names each connector of sightings once, in their order.
func Connectors(sightings []store.Sighting) []string {
	out := make([]string, 0, len(sightings))
	for _, s := range sightings {
		if !slices.Contains(out, s.Connector) {
			out = append(out, s.Connector)
		}
	}
	return out
}

// Start is a family's standing before any person's word: rung 0, where every
// drop shows.
func Start(connector, promptVersion string) store.GateTrust {
	s := Rule.Start()
	return store.GateTrust{
		Family: Family(connector, promptVersion), Connector: connector, PromptVersion: promptVersion,
		Rung: s.Rung, Run: s.Run, StraightRun: s.StraightRun, Share: s.Share, Threshold: Rule.Run, FirstShare: Rule.ShareAt(1),
		LastChange: ChangeGateAnswer,
	}
}

// Ensure writes a standing at rung 0 for each family of connectors under
// promptVersion that holds none, in the write that appends the gate's
// decision, so every family that judged an item is listed.
func Ensure(ctx context.Context, tx *store.Tx, key, promptVersion string, connectors []string) error {
	if promptVersion == "" {
		return nil
	}
	for _, c := range connectors {
		if _, ok, err := tx.GateTrust(ctx, Family(c, promptVersion)); err != nil || ok {
			if err != nil {
				return err
			}
			continue
		}
		g := Start(c, promptVersion)
		g.LastItemKey = &key
		if err := tx.PutGateTrust(ctx, g); err != nil {
			return err
		}
	}
	return nil
}

// Shows says whether the gate's drop of the item under key shows in review
// under the families of connectors at promptVersion. It shows where any of
// those families has not earned hiding it: one with no standing, one on rung
// 0, or one whose seeded draw over the family and the key falls within the
// share its rung shows. The strictest family wins, as the strictest zone does.
func Shows(ctx context.Context, tx *store.Tx, key, promptVersion string, connectors []string) (bool, error) {
	if promptVersion == "" || len(connectors) == 0 {
		return true, nil
	}
	for _, c := range connectors {
		g, ok, err := tx.GateTrust(ctx, Family(c, promptVersion))
		if err != nil {
			return true, err
		}
		if !ok || drawn(g, key) {
			return true, nil
		}
	}
	return false, nil
}

// drawn says whether g's share of drops holds the item under key.
func drawn(g store.GateTrust, key string) bool {
	return g.Share >= 1 || seededhash.Fraction(g.Family, key) < g.Share
}

// Agreement is how mark reads against the gate's verdict: low-quality agrees
// with a drop, read with a pass, and each disagrees with the other. A review,
// or no verdict, is neither.
func Agreement(mark, verdict string) string {
	switch {
	case verdict == verdictDrop && mark == MarkLowQuality, verdict == verdictPass && mark == MarkRead:
		return Agreed
	case verdict == verdictDrop && mark == MarkRead, verdict == verdictPass && mark == MarkLowQuality:
		return Disagreed
	}
	return Neither
}

// Marking is a person's mark on one item.
type Marking struct {
	Mark     string
	Login    string
	ClientID string
}

// Mark appends m on the item under key, with the gate's newest decision on it
// and whether m agrees, and moves the standing of each family of that
// decision. A mark that repeats the item's newest mark by the same person
// appends nothing and moves nothing. It answers the mark, each family's
// standing after it, and store.ErrNoItem where the store holds no such item.
func Mark(ctx context.Context, tx *store.Tx, key string, m Marking) (store.Mark, []store.GateTrust, error) {
	if _, ok, err := tx.Item(ctx, key); err != nil {
		return store.Mark{}, nil, err
	} else if !ok {
		return store.Mark{}, nil, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	}
	by := "person:" + m.Login
	judgment, connectors, judged, err := JudgmentOn(ctx, tx, key)
	if err != nil {
		return store.Mark{}, nil, err
	}
	if newest, ok, err := tx.NewestMark(ctx, key); err != nil {
		return store.Mark{}, nil, err
	} else if ok && newest.Mark == m.Mark && newest.DecidedBy == by {
		trust, err := standings(ctx, tx, newest.Families)
		return newest, trust, err
	}
	mark := store.Mark{ItemKey: key, Mark: m.Mark, DecidedBy: by, Agreement: Neither, Families: []string{}}
	if m.ClientID != "" {
		mark.ClientID = &m.ClientID
	}
	if judged {
		mark.GateVerdict, mark.GateDecidedBy = &judgment.Verdict, &judgment.DecidedBy
		if judgment.PromptVersion != "" {
			mark.PromptVersion = &judgment.PromptVersion
		}
		mark.Agreement = Agreement(m.Mark, judgment.Verdict)
	}
	var trust []store.GateTrust
	if mark.Agreement != Neither {
		if trust, err = label(ctx, tx, key, judgment.PromptVersion, connectors, mark.Agreement, ChangeMark); err != nil {
			return store.Mark{}, nil, err
		}
		for _, g := range trust {
			mark.Families = append(mark.Families, g.Family)
		}
	}
	mark, err = tx.AddMark(ctx, mark)
	return mark, trust, err
}

// Disagree moves each family of the gate's newest decision on the item under
// key back to rung 0, in the write that a person places the item on a
// destination where the gate would drop it.
func Disagree(ctx context.Context, tx *store.Tx, key string) ([]store.GateTrust, error) {
	judgment, connectors, judged, err := JudgmentOn(ctx, tx, key)
	if err != nil || !judged {
		return nil, err
	}
	return label(ctx, tx, key, judgment.PromptVersion, connectors, Disagreed, ChangePlacement)
}

// label moves the standing of each family of connectors under promptVersion
// by one person's agreement or disagreement, and answers each as written.
func label(ctx context.Context, tx *store.Tx, key, promptVersion string, connectors []string, agreement, change string) ([]store.GateTrust, error) {
	if promptVersion == "" {
		return nil, nil
	}
	out := make([]store.GateTrust, 0, len(connectors))
	for _, c := range connectors {
		g, ok, err := tx.GateTrust(ctx, Family(c, promptVersion))
		if err != nil {
			return nil, err
		}
		if !ok {
			g = Start(c, promptVersion)
		}
		d := earnedtrust.Decision{ByPerson: true, Agreed: agreement == Agreed, Disagreed: agreement == Disagreed, Promoted: g.Promoted()}
		g = next(g, d)
		g.LastChange, g.LastItemKey = change, &key
		if err := tx.PutGateTrust(ctx, g); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

// Promote records a person's yes on family, so it climbs to rung 1 once its
// straight run reaches the threshold, at once where it already has. A family
// already promoted is answered as it stands. It answers ErrNoFamily where the
// store holds no standing of family.
func Promote(ctx context.Context, tx *store.Tx, family, login, clientID string) (store.GateTrust, error) {
	g, ok, err := tx.GateTrust(ctx, family)
	if err != nil {
		return store.GateTrust{}, err
	}
	if !ok {
		return store.GateTrust{}, fmt.Errorf("%s: %w", family, ErrNoFamily)
	}
	if g.Promoted() {
		return g, nil
	}
	by, now := "person:"+login, store.Stamp(tx.Now())
	g.PromotedBy, g.PromotedTS = &by, &now
	if clientID != "" {
		g.PromotedClientID = &clientID
	}
	g = next(g, earnedtrust.Decision{Promoted: true})
	g.LastChange, g.LastItemKey = ChangePromotion, nil
	return g, tx.PutGateTrust(ctx, g)
}

// next is g after d under Rule. A promoted family whose run reached the
// threshold on rung 0 climbs to rung 1 in the same step, as Rule.Standing
// climbs it after its last decision.
func next(g store.GateTrust, d earnedtrust.Decision) store.GateTrust {
	s := Rule.Next(earnedtrust.Standing{Rung: g.Rung, Run: g.Run, StraightRun: g.StraightRun, Share: g.Share}, d)
	if g.Promoted() {
		s = Rule.Next(s, earnedtrust.Decision{Promoted: true})
	}
	g.Rung, g.Run, g.StraightRun, g.Share = s.Rung, s.Run, s.StraightRun, s.Share
	g.Threshold, g.FirstShare = Rule.Run, Rule.ShareAt(1)
	g.AsksPromotion = !g.Promoted() && s.StraightRun >= Rule.Run
	return g
}

// standings reads each of families' standing that the store holds.
func standings(ctx context.Context, tx *store.Tx, families []string) ([]store.GateTrust, error) {
	out := make([]store.GateTrust, 0, len(families))
	for _, f := range families {
		g, ok, err := tx.GateTrust(ctx, f)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, g)
		}
	}
	return out, nil
}
