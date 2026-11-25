// Package route holds the side gate and the read gate. The side gate places
// an item by the first rung that decides it: every sighting's tags, then a
// routing rule, then the quality gate, else review. A rule placed before the
// model always wins over it, so the model judges only what the rules leave.
// The quality gate's rung drops the item where the gate dropped it, sends it
// to review where the gate was unsure, and otherwise places it by the gate's
// side guess at or above its floor. While the item's quality task is open,
// the side gate decides nothing, and the item waits at the quality gate. The
// read gate then chooses whether an item placed on a side is read as is or by
// a model: by its connector's read rule, else its side's default.
//
// The side gate places an item only once the scan of its newest parse is
// done. The service runs both gates in the write that ends each stage after
// the parse and before the side gate, so a scan and its side decision commit
// together, and a parse's end places nothing. It runs them again in the write
// that applies a changed set. Each run leaves an item that a tag, a rule, a
// model or a person placed as it is. It decides again an item that the gate
// itself sent to review, or a model's guess below the floor, so a guess or a
// rule that arrives later reaches it. A person's placement from
// review runs the read gate the same way. Each decision is appended, and the
// newest at a gate decides. Each decision brings the item's delivery and its
// model read in line in the same write: a read as is queues its delivery, a
// read by a model queues its model read, and a drop or a move withdraws
// either where it has not run.
package route

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// that the quality gate gave no answer for: its run failed, or no quality
	// task was queued.
	CauseNoGuess = "no-guess"
	// CauseGateError sends to review an item whose gate could not read the
	// set in force or the item's sightings. The item keeps its parse.
	CauseGateError = "gate-error"
	// CauseQualityDrop drops an item that the quality gate dropped, and
	// CauseQualityReview sends to review one that it was unsure of.
	CauseQualityDrop   = "quality-drop"
	CauseQualityReview = store.ReasonQualityReview
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

// ByModel is a model's decision, under the run that answered.
func ByModel(run string) string { return "model:" + run }

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
	if d, ok := BeforeModel(set, sightings); ok {
		return d
	}
	return byGuess(guess)
}

// BeforeModel is the side decision that a sighting's tags or a routing rule
// make under set, and false where neither places the item, so the quality
// gate judges it.
func BeforeModel(set connectors.Set, sightings []store.Sighting) (store.Decision, bool) {
	if d, ok := byTags(set, sightings); ok {
		return d, true
	}
	return byRules(set, sightings)
}

// byGuess is the side decision on the quality gate's guess, and review for
// want of one.
func byGuess(guess *Guess) store.Decision {
	if guess == nil {
		return store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseNoGuess}
	}
	confidence := guess.Confidence
	d := store.Decision{Gate: store.GateSide, DecidedBy: ByModel(guess.Run), Confidence: &confidence}
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

// AfterStage is the hook that serve runs in the write that ends a task done.
// Where the task's stage comes after the parse and before the side gate, such
// as the scan, it runs the gates over the task's item.
func AfterStage(log *slog.Logger) func(context.Context, *store.Tx, store.Task) error {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil || !store.Precedes(store.StageParse, task.Stage) || !store.Precedes(task.Stage, store.GateSide) {
			return nil
		}
		if _, err := Gate(ctx, tx, *task.ItemKey, log); err != nil {
			return fmt.Errorf("the side gate on %s: %w", *task.ItemKey, err)
		}
		return nil
	}
}

// AfterApply is the hook that serve runs in the write that applies a changed
// set. It runs the gates again over each scanned item that no side decision
// placed, so a rule the set adds places what it matches. It then reconciles
// the delivery of each item read as is that holds no settled delivery, so a
// side's default or a deliverer that the set adds reaches the items that
// waited for one.
func AfterApply(log *slog.Logger) connectors.AfterApply {
	return func(ctx context.Context, tx *store.Tx) error {
		keys, err := tx.UnplacedScannedKeys(ctx)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if _, err := Gate(ctx, tx, key, log); err != nil {
				return fmt.Errorf("the side gate on %s: %w", key, err)
			}
		}
		if keys, err = tx.UnsettledKeysReadAs(ctx, connectors.ReadAsIs); err != nil {
			return err
		}
		for _, key := range keys {
			if _, err := QueueDelivery(ctx, tx, key); err != nil {
				return fmt.Errorf("queue the delivery of %s: %w", key, err)
			}
		}
		return nil
	}
}

// Gate runs the side gate over the item under key and, where it places the
// item on a side, the read gate, appending each decision. It answers the
// decisions it appended.
//
// It runs only over an item whose newest done parse holds a done scan, so no
// item is placed before its scan. It leaves an item that a tag, a rule, a
// model or a person placed as it is, so a sighting that arrives after
// placement moves nothing. Where it decides again and answers what the item's
// newest side decision already says, it appends nothing.
//
// Where it cannot read the set in force or the item's sightings, it sends the
// item to review as CauseGateError and logs why, so the scan stands and the
// next stage or apply runs the gate again.
func Gate(ctx context.Context, tx *store.Tx, key string, log *slog.Logger) ([]store.Decision, error) {
	newest, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil || (sided && !Provisional(newest)) {
		return nil, err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || !store.NewestParseScanned(outputs) {
		return nil, err
	}
	set, sightings, err := inputs(ctx, tx, key)
	side, ruled := BeforeModel(set, sightings)
	switch {
	case err != nil:
		log.ErrorContext(ctx, "the side gate could not read what it decides from, so the item waits in review", "item", key, "error", err)
		side = store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseGateError}
	case !ruled:
		var waits bool
		if side, waits, err = byQuality(ctx, tx, key, byGuess(GuessOf(outputs))); err != nil || waits {
			return nil, err
		}
	}
	if sided && sameDecision(newest, side) {
		return nil, nil
	}
	return decide(ctx, tx, key, side, set, sightings, nil)
}

// byQuality is the side gate's decision on the item under key, which no tag
// or rule placed, by the quality gate's newest decision: a drop where it
// dropped the item, review where it was unsure, and side, the guess's
// decision, where it passed the item. It is true where the gate has not
// decided and the item's quality task is open, so the item waits at the
// quality gate.
func byQuality(ctx context.Context, tx *store.Tx, key string, side store.Decision) (store.Decision, bool, error) {
	quality, judged, err := tx.NewestDecision(ctx, key, store.GateQuality)
	if err != nil {
		return side, false, err
	}
	if !judged {
		_, open, err := tx.OpenTask(ctx, key, store.StageQuality)
		return side, open, err
	}
	switch quality.Verdict {
	case VerdictDrop:
		return store.Decision{
			Gate: store.GateSide, Verdict: VerdictDrop, DecidedBy: quality.DecidedBy, Confidence: quality.Confidence,
			Cause: CauseQualityDrop,
		}, false, nil
	case VerdictReview:
		return store.Decision{
			Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: quality.DecidedBy, Confidence: quality.Confidence,
			Cause: CauseQualityReview,
		}, false, nil
	}
	return side, false, nil
}

// inputs reads the set in force and the item's sightings, which the side gate
// decides from beside the quality gate's guess.
func inputs(ctx context.Context, tx *store.Tx, key string) (connectors.Set, []store.Sighting, error) {
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return connectors.Set{}, nil, err
	}
	sightings, err := tx.Sightings(ctx, key)
	return set, sightings, err
}

// Provisional says whether a side decision waits on a later input: a review
// that the gate wrote for want of a rule or a guess, or for an error, and a
// model's review or drop. A rule that a later set adds, or a later answer of
// the quality gate, then decides the item again. Every other side decision,
// by a tag, a rule, a person or a model's confident guess, is final, and the
// quality gate never runs on its item again.
func Provisional(d store.Decision) bool {
	return (d.Verdict == VerdictReview && d.DecidedBy == ByGate) ||
		((d.Verdict == VerdictReview || d.Verdict == VerdictDrop) && strings.HasPrefix(d.DecidedBy, ByModel("")))
}

// sameDecision says whether b decides what a decides, by the same hand.
func sameDecision(a, b store.Decision) bool {
	return a.Verdict == b.Verdict && a.DecidedBy == b.DecidedBy && a.Cause == b.Cause &&
		text(a.Destination) == text(b.Destination) && confidence(a) == confidence(b)
}

// GuessOf is the side guess of the quality gate's newest done output among
// outputs, and nil where that output holds none. The side gate places by it,
// and the review queue shows it.
func GuessOf(outputs []store.Output) *Guess {
	newest := store.NewestDone(outputs, store.StageQuality)
	if newest == nil || newest.SideGuess == nil {
		return nil
	}
	g := &Guess{Side: *newest.SideGuess, Run: newest.Run()}
	if newest.SideConfidence != nil {
		g.Confidence = *newest.SideConfidence
	}
	return g
}

// decide appends side, and the read gate's decision where side places the
// item on a side. Where a tag, a rule or a person decides the side for good,
// it withdraws the item's queued quality task, so no model judges an item
// already placed.
func decide(ctx context.Context, tx *store.Tx, key string, side store.Decision, set connectors.Set, sightings []store.Sighting,
	note *string,
) ([]store.Decision, error) {
	side.ItemKey = key
	written, err := tx.AddDecision(ctx, side)
	if err != nil {
		return nil, err
	}
	if !Provisional(written) {
		why := "withdrawn: " + written.DecidedBy + " decided the item's side before the quality gate ran"
		if err := tx.WithdrawQueued(ctx, key, store.StageQuality, why); err != nil {
			return nil, err
		}
	}
	out := []store.Decision{written}
	var read store.Decision
	if onASide(side.Verdict) {
		read = Read(set, sightings, side.Verdict, note)
		read.ItemKey = key
		if read, err = tx.AddDecision(ctx, read); err != nil {
			return nil, err
		}
		out = append(out, read)
	}
	item, _, err := tx.Item(ctx, key)
	if err != nil {
		return nil, err
	}
	if _, err := reconcileDelivery(ctx, tx, set, item, side, read); err != nil {
		return nil, err
	}
	if err := reconcileModelRead(ctx, tx, key, side, read); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadByModel says whether an item's newest side and read decisions send it
// to a model read: the side decision places it on a side, and the read
// decision reads it by a model.
func ReadByModel(side, read store.Decision) bool {
	return onASide(side.Verdict) && read.Verdict == connectors.ReadModel
}

// reconcileModelRead queues the model read of the item under key where its
// newest decisions read it by a model, and it holds neither an open model
// read's task nor a done read. It withdraws a queued one where they read it
// otherwise. A read that the item already holds runs again only through a
// replay.
func reconcileModelRead(ctx context.Context, tx *store.Tx, key string, side, read store.Decision) error {
	if !ReadByModel(side, read) {
		by := side.DecidedBy
		if onASide(side.Verdict) {
			by = read.DecidedBy
		}
		return tx.WithdrawQueued(ctx, key, store.StageModelRead, "withdrawn: "+by+" decided the item is not read by a model")
	}
	if _, open, err := tx.OpenTask(ctx, key, store.StageModelRead); err != nil || open {
		return err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || store.NewestDone(outputs, store.StageModelRead) != nil {
		return err
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageModelRead, Capability: store.StageModelRead})
	return err
}

// Due is where an item's newest side and read decisions send it for delivery
// under set, and false where they send it nowhere: the side decision places
// it on no side, or the read decision reads it other than as is. Where true,
// it is the destination whose deliverer writes the item, or why none does.
// The queue, a replay, an apply and items show each ask it.
func Due(set connectors.Set, item store.Item, side, read store.Decision) (connectors.Destination, *connectors.Undelivered, bool) {
	if !onASide(side.Verdict) || read.Verdict != connectors.ReadAsIs {
		return connectors.Destination{}, nil, false
	}
	d, undelivered := set.DeliveryTo(side, item)
	return d, undelivered, true
}

// QueueDelivery brings the item under key's deliveries in line with what Due
// answers for its newest decisions under the set in force. It answers whether
// it queued a delivery.
func QueueDelivery(ctx context.Context, tx *store.Tx, key string) (bool, error) {
	item, ok, err := tx.Item(ctx, key)
	if err != nil || !ok {
		return false, err
	}
	side, _, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil {
		return false, err
	}
	read, _, err := tx.NewestDecision(ctx, key, store.GateRead)
	if err != nil {
		return false, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return false, err
	}
	return reconcileDelivery(ctx, tx, set, item, side, read)
}

// reconcileDelivery withdraws each queued delivery of item to a destination
// that Due no longer answers, and queues the one it does answer, as a
// Kubernetes controller moves what runs toward what is declared. So a person's
// drop or move stops a delivery that has not run yet.
func reconcileDelivery(ctx context.Context, tx *store.Tx, set connectors.Set, item store.Item, side, read store.Decision) (bool, error) {
	d, undelivered, due := Due(set, item, side, read)
	keep := ""
	if due && undelivered == nil {
		keep = d.Name
	}
	if err := tx.WithdrawDeliveries(ctx, item.Key, keep); err != nil || keep == "" {
		return false, err
	}
	return tx.QueueDelivery(ctx, item, d.Name, d.Deliverer)
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

func confidence(d store.Decision) float64 {
	if d.Confidence == nil {
		return -1
	}
	return *d.Confidence
}
