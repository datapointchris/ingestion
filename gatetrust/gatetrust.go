// Package gatetrust is how far a person trusts the quality gate's drops. A
// family is one connector under one version of the gate's prompt, answered by
// one model at one effort. So a new prompt, model or effort starts its
// families at rung 0. Each family is an earnedtrust.Family, and its standing
// is what earnedtrust reads from the family's decisions and its promotion.
//
// A mark of low-quality agrees with a drop and disagrees with a pass. A mark
// of read agrees with a pass and disagrees with a drop. Only an agreement on
// a drop climbs, because a family earns hiding drops: a pass shows its item
// whatever the family's standing. A person who places on a destination an
// item whose drop the gate showed or hid disagrees with the drop. Any
// disagreement returns the family to rung 0. An item that the gate answered
// again counts once, by the words against its newest answer.
//
// While its family stands on rung 0, every item that the gate drops waits in
// review, marked gate-would-drop. A family whose straight run reaches its
// threshold asks a person to promote it, and one yes does. A promoted family
// shows its rung's share of the gate's drops, picked by a seeded draw over
// the family and the gate's answer, and the side gate drops the rest with
// their rows kept. Only a drop that the gate answered after the yes climbs
// past it. This is a continuous sampling plan, as earnedtrust names it. Its
// rule is earnedtrust's cautious pace, because a hidden item gives the person
// no prompt to undo it.
package gatetrust

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/datapointchris/earnedtrust"

	"github.com/datapointchris/ingestion/store"
)

// Rule is the ladder that every family climbs: earnedtrust's cautious pace.
// earnedtrust counts only a person's word on a family, so the gate never
// promotes itself.
var Rule = earnedtrust.PaceRule(earnedtrust.PaceCautious)

// familyOf is the earnedtrust family named name. It is sampled, so a
// promoted family still shows a share of its drops.
func familyOf(name string) earnedtrust.Family {
	return earnedtrust.Family{Name: name, Rule: Rule, Sampled: true}
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

// The kinds of a person's word that a family's decisions hold.
const (
	KindMark      = "mark"
	KindPlacement = "placement"
)

// What moves a family's standing, as its last_change records.
const (
	// ChangeGateAnswer is the gate's answer: its first under the family,
	// which starts it at rung 0, or a later one on an item that a person gave
	// a word on, after which only the words against the later answer count.
	ChangeGateAnswer = "gate-answer"
	ChangeMark       = "mark"
	// ChangePlacement is a person's placement on a destination of an item
	// whose drop the gate showed or hid.
	ChangePlacement = "placement"
	ChangePromotion = "promotion"
	// ChangeRule is a release whose rule reads the family's decisions to
	// another standing.
	ChangeRule = "rule"
)

// ErrNoFamily is a promotion naming a family the store holds no standing of.
var ErrNoFamily = errors.New("no such gate family")

// DefaultSetting stands for a model or an effort that a run left to its
// adapter.
const DefaultSetting = "default"

// Answerer is what answered one of the gate's decisions: the version of its
// prompt, the model that its run reports answering on, else the model that
// the run asked for, and the effort that the run asked at. A new model or
// effort is a new family, so it earns its own trust from rung 0.
type Answerer struct {
	PromptVersion string
	Model         string
	Effort        string
}

// AnswererOf is what answered the gate's output o.
func AnswererOf(o store.Output) Answerer {
	return Answerer{
		PromptVersion: o.ProducerVersion,
		Model:         cmp.Or(text(o.AnsweredModel), text(o.Model), DefaultSetting),
		Effort:        cmp.Or(text(o.Effort), DefaultSetting),
	}
}

// Answered is what answered d: the done quality output among outputs whose
// run d names. It is the zero Answerer where none is.
func Answered(outputs []store.Output, d store.Decision) Answerer {
	run, ok := d.ModelRun()
	if !ok {
		return Answerer{}
	}
	for _, o := range outputs {
		if o.Stage == store.StageQuality && o.State == store.OutputDone && o.Run() == run {
			return AnswererOf(o)
		}
	}
	return Answerer{}
}

// Family is the name of connector's family under what answered:
// <connector>@<prompt version>@<model>@<effort>.
func Family(connector string, a Answerer) string {
	return connector + "@" + a.PromptVersion + "@" + a.Model + "@" + a.Effort
}

// Judgment is the quality gate's newest decision on an item, and what
// answered it.
type Judgment struct {
	store.Decision
	Answerer Answerer
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
	return Judgment{Decision: d, Answerer: Answered(outputs, d)}, Connectors(sightings), true, nil
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
func Start(connector string, a Answerer) store.GateTrust {
	g := store.GateTrust{
		Family: Family(connector, a), Connector: connector, PromptVersion: a.PromptVersion, Model: a.Model, Effort: a.Effort,
		LastChange: ChangeGateAnswer,
	}
	return withStanding(g, familyOf(g.Family).Standing(nil, false))
}

// Ensure writes a standing at rung 0 for each family of connectors under a
// that holds none, in the write that appends the gate's decision, so every
// family that judged an item is listed.
func Ensure(ctx context.Context, tx *store.Tx, key string, a Answerer, connectors []string) error {
	if a.PromptVersion == "" {
		return nil
	}
	for _, c := range connectors {
		if _, ok, err := tx.GateTrust(ctx, Family(c, a)); err != nil || ok {
			if err != nil {
				return err
			}
			continue
		}
		g := Start(c, a)
		g.LastItemKey = &key
		if err := tx.PutGateTrust(ctx, g); err != nil {
			return err
		}
	}
	return nil
}

// Shows says whether the gate's drop in answer, its decided_by, shows in
// review under the families of connectors that a answered for. It shows
// where any of those families has not earned hiding it: one with no
// standing, or one whose earnedtrust standing puts the answer to a person.
// The strictest family wins, as the strictest zone does. The review queue
// holds every drop it is shown, so no drop waits there for room.
func Shows(ctx context.Context, tx *store.Tx, answer string, a Answerer, connectors []string) (bool, error) {
	if a.PromptVersion == "" || len(connectors) == 0 {
		return true, nil
	}
	for _, c := range connectors {
		g, ok, err := tx.GateTrust(ctx, Family(c, a))
		if err != nil {
			return true, err
		}
		if !ok || standingOf(g).Handle(answer, 0) != earnedtrust.HandlingAlone {
			return true, nil
		}
	}
	return false, nil
}

// Agreement is how mark reads against the gate's verdict: low-quality agrees
// with a drop, read with a pass, and each disagrees with the other. A review,
// or no verdict, is neither.
func Agreement(mark, verdict string) string {
	switch {
	case verdict == store.VerdictDrop && mark == MarkLowQuality, verdict == store.VerdictPass && mark == MarkRead:
		return Agreed
	case verdict == store.VerdictDrop && mark == MarkRead, verdict == store.VerdictPass && mark == MarkLowQuality:
		return Disagreed
	}
	return Neither
}

// counts says whether a word that reads as agreement against the gate's
// verdict moves a family: any disagreement, and an agreement with a drop.
func counts(agreement, verdict string) bool {
	return agreement == Disagreed || (agreement == Agreed && verdict == store.VerdictDrop)
}

// Marking is one mark on one item. DecidedBy is person:<login>, or
// service:<client id> where a service holding the marks scope names no
// person. Either counts as a person's word, since the scope is granted only
// to a client that relays one.
type Marking struct {
	Mark      string
	DecidedBy string
	ClientID  string
}

// Mark appends m on the item under key, with the gate's newest decision on it
// and whether m agrees, and appends a decision to each family of that answer
// where m counts. A mark that repeats the item's newest mark, by the same
// hand on the same answer of the gate, appends nothing and moves nothing. It
// answers the mark, each family's standing after it, and store.ErrNoItem
// where the store holds no such item.
func Mark(ctx context.Context, tx *store.Tx, key string, m Marking) (store.Mark, []store.GateTrust, error) {
	if _, ok, err := tx.Item(ctx, key); err != nil {
		return store.Mark{}, nil, err
	} else if !ok {
		return store.Mark{}, nil, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	}
	judgment, connectors, judged, err := JudgmentOn(ctx, tx, key)
	if err != nil {
		return store.Mark{}, nil, err
	}
	answered := ""
	if judged {
		answered = judgment.DecidedBy
	}
	if newest, ok, err := tx.NewestMark(ctx, key); err != nil {
		return store.Mark{}, nil, err
	} else if ok && newest.Mark == m.Mark && newest.DecidedBy == m.DecidedBy && text(newest.GateDecidedBy) == answered {
		trust, err := standings(ctx, tx, newest.Families)
		return newest, trust, err
	}
	mark := store.Mark{ItemKey: key, Mark: m.Mark, DecidedBy: m.DecidedBy, ClientID: orNil(m.ClientID), Agreement: Neither, Families: []string{}}
	if judged {
		mark.GateVerdict, mark.GateDecidedBy = &judgment.Verdict, &judgment.DecidedBy
		if v := judgment.Answerer.PromptVersion; v != "" {
			mark.PromptVersion = &v
		}
		mark.Agreement = Agreement(m.Mark, judgment.Verdict)
	}
	var trust []store.GateTrust
	if counts(mark.Agreement, judgment.Verdict) {
		word := store.GateTrustDecision{Kind: KindMark, Agreement: mark.Agreement, DecidedBy: m.DecidedBy, ClientID: mark.ClientID}
		if trust, err = record(ctx, tx, key, judgment, connectors, word, ChangeMark); err != nil {
			return store.Mark{}, nil, err
		}
		for _, g := range trust {
			mark.Families = append(mark.Families, g.Family)
		}
	}
	mark, err = tx.AddMark(ctx, mark)
	return mark, trust, err
}

// Disagree appends a disagreement to each family of the gate's newest
// decision on the item under key, in the write that decidedBy places the
// item on a destination where the gate showed or hid its drop.
func Disagree(ctx context.Context, tx *store.Tx, key, decidedBy, clientID string) ([]store.GateTrust, error) {
	judgment, connectors, judged, err := JudgmentOn(ctx, tx, key)
	if err != nil || !judged {
		return nil, err
	}
	word := store.GateTrustDecision{Kind: KindPlacement, Agreement: Disagreed, DecidedBy: decidedBy, ClientID: orNil(clientID)}
	return record(ctx, tx, key, judgment, connectors, word, ChangePlacement)
}

// record appends word to each family of connectors under what answered the
// judgment, and writes each family's standing after it. The word climbs past
// a family's promotion only where the gate answered the item after it.
func record(ctx context.Context, tx *store.Tx, key string, j Judgment, connectors []string, word store.GateTrustDecision,
	change string,
) ([]store.GateTrust, error) {
	if j.Answerer.PromptVersion == "" {
		return nil, nil
	}
	out := make([]store.GateTrust, 0, len(connectors))
	for _, c := range connectors {
		g, ok, err := tx.GateTrust(ctx, Family(c, j.Answerer))
		if err != nil {
			return nil, err
		}
		if !ok {
			g = Start(c, j.Answerer)
			if err := tx.PutGateTrust(ctx, g); err != nil {
				return nil, err
			}
		}
		word.Family, word.Model, word.Effort, word.ItemKey, word.GateDecidedBy = g.Family, g.Model, g.Effort, key, j.DecidedBy
		word.Promoted = g.Promoted() && *g.PromotedTS <= j.DecidedTS
		if _, err := tx.AddGateTrustDecision(ctx, word); err != nil {
			return nil, err
		}
		if g, err = reread(ctx, tx, g); err != nil {
			return nil, err
		}
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
	by, now := store.DecidedByPerson(login), store.Stamp(tx.Now())
	g.PromotedBy, g.PromotedTS, g.PromotedClientID = &by, &now, orNil(clientID)
	if g, err = reread(ctx, tx, g); err != nil {
		return store.GateTrust{}, err
	}
	g.LastChange, g.LastItemKey = ChangePromotion, nil
	return g, tx.PutGateTrust(ctx, g)
}

// Settle reads every family's standing again from its decisions, and writes
// each one that differs from its row, as a change of rule. serve runs it
// once it listens, so a release that changes the pace places every family
// again from its history. It answers how many families moved.
func Settle(ctx context.Context, tx *store.Tx) (int, error) {
	families, err := tx.GateTrusts(ctx)
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, held := range families {
		g, err := reread(ctx, tx, held)
		if err != nil {
			return moved, err
		}
		if sameStanding(g, held) {
			continue
		}
		g.LastChange, g.LastItemKey = ChangeRule, nil
		if err := tx.PutGateTrust(ctx, g); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

// Reanswered reads again the standing of each family of connectors under a
// that holds a person's word on the item under key, in the write that appends
// the gate's newer answer on it, and writes each one that moved. The item's
// words against an earlier answer then stop counting, so the item counts
// once, under its newest answer.
func Reanswered(ctx context.Context, tx *store.Tx, key string, a Answerer, connectors []string) error {
	if a.PromptVersion == "" {
		return nil
	}
	for _, c := range connectors {
		held, ok, err := tx.GateTrust(ctx, Family(c, a))
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		logged, err := tx.GateTrustDecisions(ctx, held.Family)
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(logged, func(d store.GateTrustDecision) bool { return d.ItemKey == key }) {
			continue
		}
		g, err := reread(ctx, tx, held)
		if err != nil {
			return err
		}
		if sameStanding(g, held) {
			continue
		}
		g.LastChange, g.LastItemKey = ChangeGateAnswer, &key
		if err := tx.PutGateTrust(ctx, g); err != nil {
			return err
		}
	}
	return nil
}

// reread is g with the standing that earnedtrust reads from g's decisions
// that count and its promotion.
func reread(ctx context.Context, tx *store.Tx, g store.GateTrust) (store.GateTrust, error) {
	logged, err := tx.GateTrustDecisions(ctx, g.Family)
	if err != nil {
		return g, err
	}
	if logged, err = counted(ctx, tx, g, logged); err != nil {
		return g, err
	}
	decisions := make([]earnedtrust.Decision, len(logged))
	for i, d := range logged {
		decisions[i] = earnedtrust.Decision{ByPerson: true, Agreed: d.Agreement == Agreed, Disagreed: d.Agreement == Disagreed, Promoted: d.Promoted}
	}
	return withStanding(g, familyOf(g.Family).Standing(decisions, g.Promoted())), nil
}

// counted is the words among logged, g's decisions, that count on its ladder.
// Where the gate's newest answer on a word's item came from g's answerer, the
// word counts only where it was given against that answer. So an item that
// the gate answered again, such as over a new parse, counts once, under its
// newest answer. A word on an item whose newest answer came from another
// family counts as it was given.
func counted(ctx context.Context, tx *store.Tx, g store.GateTrust, logged []store.GateTrustDecision) ([]store.GateTrustDecision, error) {
	answerer := Answerer{PromptVersion: g.PromptVersion, Model: g.Model, Effort: g.Effort}
	newest := map[string]string{}
	out := make([]store.GateTrustDecision, 0, len(logged))
	for _, d := range logged {
		answer, read := newest[d.ItemKey]
		if !read {
			j, _, judged, err := JudgmentOn(ctx, tx, d.ItemKey)
			if err != nil {
				return nil, err
			}
			if judged && j.Answerer == answerer {
				answer = j.DecidedBy
			}
			newest[d.ItemKey] = answer
		}
		if answer == "" || d.GateDecidedBy == answer {
			out = append(out, d)
		}
	}
	return out, nil
}

// sameStanding says whether a and b hold the same standing.
func sameStanding(a, b store.GateTrust) bool {
	return a.Rung == b.Rung && a.Run == b.Run && a.StraightRun == b.StraightRun && a.Share == b.Share &&
		a.Threshold == b.Threshold && a.FirstShare == b.FirstShare && a.AsksPromotion == b.AsksPromotion
}

// withStanding is g holding s.
func withStanding(g store.GateTrust, s earnedtrust.FamilyStanding) store.GateTrust {
	g.Rung, g.Run, g.StraightRun, g.Share = s.Rung, s.Run, s.StraightRun, s.Share
	g.Threshold, g.FirstShare, g.AsksPromotion = s.Threshold, Rule.ShareAt(1), s.Earned
	return g
}

// standingOf is the earnedtrust standing that g holds.
func standingOf(g store.GateTrust) earnedtrust.FamilyStanding {
	return earnedtrust.FamilyStanding{
		Family: g.Family, Threshold: g.Threshold, Promoted: g.Promoted(), Earned: g.AsksPromotion, MaxOpen: Rule.MaxOpen,
		Standing: earnedtrust.Standing{Rung: g.Rung, Run: g.Run, StraightRun: g.StraightRun, Share: g.Share},
	}
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

func orNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
