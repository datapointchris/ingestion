// Package route holds the side gate and the read gate. The side gate places
// an item by the first rung that decides it: every sighting's tags, then a
// routing rule, then the quality gate's side guess at or above its floor, else
// review. A rule placed before the model always wins over it, so the model
// judges only what the rules leave. The read gate then chooses whether an item
// placed on a side is read as is or by a model: by its connector's read rule,
// else its side's default.
//
// The service runs both gates in the write that ends an item's parse, so no
// item sits parsed with no side decision. A person's placement from review
// runs the read gate the same way. Each decision is appended, and the newest
// at a gate decides.
package route

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/store"
)

// The side gate's verdicts beyond the two sides. An item placed on a
// destination that takes no side is dropped, softly: it keeps its row.
const (
	VerdictReview = "review"
	VerdictDrop   = "drop"
)

// Causes that the gates write. A person's decision carries the aspect the
// person named instead, such as subreddit, channel, theme or title.
const (
	CauseTags         = "tags"
	CauseTagsDisagree = "tags-disagree"
	CauseRule         = "rule"
	CauseGuess        = "guess"
	CauseBelowFloor   = "below-floor"
	// CauseNoGuess sends to review an item that no tag or rule placed and
	// that the quality gate has given no guess for.
	CauseNoGuess = "no-guess"
	// CauseConnector and CauseSideDefault are the read gate's.
	CauseConnector   = "connector"
	CauseSideDefault = "side-default"
	// CauseUnstated is a person's placement that names no cause.
	CauseUnstated = "unstated"
)

// GuessFloor is the confidence at or above which the quality gate's side
// guess places an item. Below it, the item waits in review with its guess.
const GuessFloor = 0.7

// Who decided, as decided_by spells it, beside rule:<name>, model:<run> and
// person:<login>.
const (
	ByTag = "tag"
	// ByGate is the side gate itself, where no rung placed the item.
	ByGate = "gate"
)

// ByPerson is a person's decision, under their login.
func ByPerson(login string) string { return "person:" + login }

func byRule(name string) string { return "rule:" + name }

func byModel(run string) string { return "model:" + run }

// sideDefaults is how an item on each side is read where no connector's read
// rule decides.
var sideDefaults = map[string]string{
	connectors.SidePersonal: connectors.ReadAsIs,
	connectors.SideFleet:    connectors.ReadModel,
}

// Guess is the quality gate's side guess on an item.
type Guess struct {
	// Side is personal, fleet or unsure.
	Side       string
	Confidence float64
	// Run names the model run that answered it.
	Run string
}

// Side is the side gate's decision on an item that sightings sighted, under
// set and the quality gate's guess, which is nil where it has given none.
func Side(set connectors.Set, sightings []store.Sighting, guess *Guess) store.Decision {
	if d, ok := byTags(set, sightings); ok {
		return d
	}
	if d, ok := byRules(set, sightings); ok {
		return d
	}
	if guess == nil {
		return store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseNoGuess}
	}
	confidence := guess.Confidence
	d := store.Decision{Gate: store.GateSide, DecidedBy: byModel(guess.Run), Confidence: &confidence}
	if onASide(guess.Side) && confidence >= GuessFloor {
		d.Verdict, d.Cause = guess.Side, CauseGuess
	} else {
		d.Verdict, d.Cause = VerdictReview, CauseBelowFloor
	}
	return d
}

// byTags places the item by the destinations its sightings' tags name, and is
// false where no tag names one. Tags naming destinations on two sides, or a
// side and none, send it to review. The decision names the destination where
// the tags name only one.
func byTags(set connectors.Set, sightings []store.Sighting) (store.Decision, bool) {
	var named []connectors.Destination
	for _, s := range sightings {
		for _, tag := range s.Tags {
			d, ok := set.Tagged(tag)
			if ok && !slices.ContainsFunc(named, func(n connectors.Destination) bool { return n.Name == d.Name }) {
				named = append(named, d)
			}
		}
	}
	if len(named) == 0 {
		return store.Decision{}, false
	}
	decision := store.Decision{Gate: store.GateSide, DecidedBy: ByTag, Verdict: verdictOf(named[0]), Cause: CauseTags}
	for _, d := range named[1:] {
		if verdictOf(d) != decision.Verdict {
			decision.Verdict, decision.Cause = VerdictReview, CauseTagsDisagree
			return decision, true
		}
	}
	if len(named) == 1 {
		decision.Destination = &named[0].Name
	}
	return decision, true
}

// byRules places the item by the first routing rule, in the set's order, that
// one of its sightings holds for, and is false where none does.
func byRules(set connectors.Set, sightings []store.Sighting) (store.Decision, bool) {
	for _, rule := range set.RoutingRules {
		if !slices.ContainsFunc(sightings, func(s store.Sighting) bool { return holds(rule.Match, s) }) {
			continue
		}
		destination, ok := set.Destination(rule.Destination)
		if !ok {
			continue
		}
		return store.Decision{
			Gate: store.GateSide, Verdict: verdictOf(destination), DecidedBy: byRule(rule.Name), Cause: CauseRule,
			Destination: &destination.Name,
		}, true
	}
	return store.Decision{}, false
}

// holds says whether every condition m names holds for one sighting: its
// connector, one of its tags, and the value of one of its payload's fields.
func holds(m connectors.Match, s store.Sighting) bool {
	if m.Connector == "" && m.Tag == "" && m.Field == "" {
		return false
	}
	if m.Connector != "" && s.Connector != m.Connector {
		return false
	}
	if m.Tag != "" && !slices.Contains(s.Tags, m.Tag) {
		return false
	}
	if m.Field != "" {
		value, ok := s.Fields[m.Field]
		if !ok || !slices.Contains(m.Values, value) {
			return false
		}
	}
	return true
}

// Read is the read gate's decision on an item placed on side: the read rule of
// the first connector that sighted it and declares one, else side's default.
// A model read takes side's lens, and carries note, the questions and why the
// item was saved, where a person wrote one.
func Read(set connectors.Set, sightings []store.Sighting, side string, note *string) store.Decision {
	d := store.Decision{Gate: store.GateRead, Verdict: sideDefaults[side], DecidedBy: byRule(side + "-default"), Cause: CauseSideDefault}
	for _, s := range sightings {
		if c, ok := set.Connector(s.Connector); ok && c.ReadRule != "" {
			d.Verdict, d.DecidedBy, d.Cause = c.ReadRule, byRule(c.Name), CauseConnector
			break
		}
	}
	if d.Verdict == connectors.ReadModel {
		lens := side
		d.Lens, d.Note = &lens, note
	}
	return d
}

// AfterStage runs the gates in the write that ends task done, where task is
// an item's parse.
func AfterStage(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.Stage != store.StageParse || task.ItemKey == nil {
		return nil
	}
	_, err := Gate(ctx, tx, *task.ItemKey)
	return err
}

// Gate runs the side gate over the item under key and, where it places the
// item on a side, the read gate, appending each decision. It leaves an item
// that a side decision already holds as it is, so a sighting or a parse that
// arrives after placement moves nothing. It answers the decisions it appended.
//
// The guess rung reads the quality gate's newest done output. With none, the
// item goes to review with CauseNoGuess.
func Gate(ctx context.Context, tx *store.Tx, key string) ([]store.Decision, error) {
	if _, sided, err := tx.NewestDecision(ctx, key, store.GateSide); err != nil || sided {
		return nil, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return nil, err
	}
	guess, err := guessOf(ctx, tx, key)
	if err != nil {
		return nil, err
	}
	return decide(ctx, tx, key, Side(set, sightings, guess), set, sightings, nil)
}

// guessOf is the side guess of the quality gate's newest done output on the
// item under key, and nil where it holds none.
func guessOf(ctx context.Context, tx *store.Tx, key string) (*Guess, error) {
	o, ok, err := tx.NewestOutput(ctx, key, store.StageQuality, store.OutputDone)
	if err != nil || !ok || o.SideGuess == nil {
		return nil, err
	}
	g := &Guess{Side: *o.SideGuess, Run: o.Producer + "/" + o.ProducerVersion}
	if o.SideConfidence != nil {
		g.Confidence = *o.SideConfidence
	}
	if o.RunID != nil {
		g.Run = *o.RunID
	}
	return g, nil
}

// decide appends side, and the read gate's decision where side places the
// item on a side.
func decide(ctx context.Context, tx *store.Tx, key string, side store.Decision, set connectors.Set, sightings []store.Sighting,
	note *string,
) ([]store.Decision, error) {
	side.ItemKey = key
	written, err := tx.AddDecision(ctx, side)
	if err != nil {
		return nil, err
	}
	out := []store.Decision{written}
	if !onASide(side.Verdict) {
		return out, nil
	}
	read := Read(set, sightings, side.Verdict, note)
	read.ItemKey = key
	if written, err = tx.AddDecision(ctx, read); err != nil {
		return nil, err
	}
	return append(out, written), nil
}

// Placement is a person's decision on an item.
type Placement struct {
	// Destination is where the item goes, one the set in force declares. A
	// drop names none.
	Destination string
	// Cause is the aspect that decided it, such as subreddit, channel, theme
	// or title. Empty is CauseUnstated.
	Cause string
	// Note is the questions and why the item was saved, which a model read
	// answers.
	Note     *string
	Login    string
	ClientID string
}

// NotDeclaredError is a placement naming a destination that the set in force
// does not declare.
type NotDeclaredError struct {
	Destination string
	Declared    []string
}

func (e *NotDeclaredError) Error() string {
	if len(e.Declared) == 0 {
		return fmt.Sprintf("no destination is declared, so %s is not one: apply a set whose destinations.yml names it", e.Destination)
	}
	return fmt.Sprintf("%s is not a destination the set in force declares, which are %s", e.Destination, strings.Join(e.Declared, ", "))
}

// ErrNotDeclared is what a NotDeclaredError matches with errors.Is.
var ErrNotDeclared = errors.New("the destination is not declared")

func (e *NotDeclaredError) Is(target error) bool { return target == ErrNotDeclared }

// Place appends a person's decision on the item under key: on p's destination,
// or a drop where p names none. Where the destination takes a side, the read
// gate's decision follows it. A person can place any item, including one that
// a rule already placed. A placement that repeats the newest side decision
// appends nothing, and answers the decisions it made. Place answers
// store.ErrNoItem where the store holds no such item, and a NotDeclaredError
// for a destination that the set in force does not declare.
func Place(ctx context.Context, tx *store.Tx, key string, p Placement) ([]store.Decision, error) {
	if _, ok, err := tx.Item(ctx, key); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	if p.Cause == "" {
		p.Cause = CauseUnstated
	}
	side := store.Decision{Gate: store.GateSide, Verdict: VerdictDrop, DecidedBy: ByPerson(p.Login), Cause: p.Cause, Note: p.Note}
	if p.ClientID != "" {
		side.ClientID = &p.ClientID
	}
	if p.Destination != "" {
		destination, ok := set.Destination(p.Destination)
		if !ok {
			e := &NotDeclaredError{Destination: p.Destination}
			for _, d := range set.Destinations {
				e.Declared = append(e.Declared, d.Name)
			}
			return nil, e
		}
		side.Verdict, side.Destination = verdictOf(destination), &destination.Name
	}
	if made, ok, err := repeated(ctx, tx, key, side); err != nil || ok {
		return made, err
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return nil, err
	}
	return decide(ctx, tx, key, side, set, sightings, p.Note)
}

// repeated answers the decisions that an earlier copy of side made, and is
// false where the item's newest side decision is not one.
func repeated(ctx context.Context, tx *store.Tx, key string, side store.Decision) ([]store.Decision, bool, error) {
	newest, ok, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil || !ok {
		return nil, false, err
	}
	if newest.Verdict != side.Verdict || newest.DecidedBy != side.DecidedBy || newest.Cause != side.Cause ||
		text(newest.Destination) != text(side.Destination) || text(newest.Note) != text(side.Note) ||
		text(newest.ClientID) != text(side.ClientID) {
		return nil, false, nil
	}
	made := []store.Decision{newest}
	if read, ok, err := tx.NewestDecision(ctx, key, store.GateRead); err != nil {
		return nil, false, err
	} else if ok && onASide(newest.Verdict) {
		made = append(made, read)
	}
	return made, true, nil
}

func verdictOf(d connectors.Destination) string {
	if d.Side == "" {
		return VerdictDrop
	}
	return d.Side
}

func onASide(verdict string) bool {
	return verdict == connectors.SidePersonal || verdict == connectors.SideFleet
}

func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
