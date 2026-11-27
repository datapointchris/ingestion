// Package route holds the side gate and the read gate. The side gate places
// an item by the first rung that decides it: every sighting's tags, then a
// routing rule, then the quality gate, else review. A routing rule is one
// that the set in force declares, or one that a person accepted. A seeded
// share of an accepted rule's items waits in review instead, marked
// rule-would-hide. A rule placed before the model always wins over it: the
// gate answers a ruled item too, and its answer moves nothing. The quality
// gate's rung sends to review an item that
// the gate dropped, marked gate-would-drop, unless every family of the item
// has earned hiding it, where it drops the item. It sends to review an item
// that the gate was unsure of, and otherwise places it by the gate's side
// guess at or above its floor. Where a connector that sighted the item
// declares person_sorts, a guess places nothing and the item waits in review
// beside it. While the item's quality task is open, an item that no tag or
// rule placed waits at the quality gate, and the side gate decides nothing. The
// read gate then chooses whether an item placed on a side is read as is or by
// a model: by its connector's read rule, else its side's default.
//
// The side gate places an item only once the scan of its newest parse is
// done. The service runs both gates in the write that ends each stage after
// the parse and before the side gate, so a scan and its side decision commit
// together, and a parse's end places nothing. It runs them again in the write
// that applies a changed set, and in the write that adds a sighting to an
// item the store held. Each run leaves an item that a tag, a rule, a
// model or a person placed as it is. It decides again an item that the gate
// itself sent to review, or a model's guess below the floor, so a guess or a
// rule that arrives later reaches it. A person's placement from
// review runs the read gate the same way. Each decision is appended, and the
// newest at a gate decides. Each decision brings the item's delivery and its
// model read in line in the same write: a read as is queues its delivery, a
// read by a model queues its model read, and a drop or a move withdraws
// either where it has not run. The same write passes the item's decisions to
// each of its chapters, which no gate decides. A chapter's delivery waits for
// its book's to settle, and the write that settles the book's queues it.
package route

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/store"
)

// The side gate's verdicts beyond the two sides. An item placed on a
// destination that takes no side is dropped, softly: it keeps its row.
const (
	VerdictReview = store.VerdictReview
	VerdictDrop   = store.VerdictDrop
	VerdictSplit  = store.VerdictSplit
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
	// CausePersonSorts sends to review an item that no tag or rule placed and
	// that a connector declaring person_sorts sighted, where the quality gate
	// passed it with a side guess. The review queue shows the guess beside it.
	CausePersonSorts = "person-sorts"
	// CauseOwnersWords sends to review an item that no tag or rule placed and
	// that holds the owner's own words, as store.Item.SkipsQualityGate names
	// one. The quality gate never reads it, so no guess can place it.
	CauseOwnersWords = "owners-words"
	// CauseTopics splits a memo once its topics are landed, each an item that
	// the gates place on its own. Its verdict is VerdictSplit.
	CauseTopics = store.CauseTopics
	// CauseGateWouldDrop sends to review an item that the quality gate
	// dropped, where a family of the item has not earned hiding it.
	// CauseQualityDrop drops one that every family's trust hides, and keeps its
	// row. CauseQualityReview sends to review one that the gate was unsure of.
	CauseGateWouldDrop = "gate-would-drop"
	CauseQualityDrop   = "quality-drop"
	CauseQualityReview = store.ReasonQualityReview
	// CauseRuleWouldHide sends to review an item that a routing rule would
	// have dropped or placed, where the rule's review share drew it.
	CauseRuleWouldHide = "rule-would-hide"
	// CauseConnector and CauseSideDefault are the read gate's.
	CauseConnector   = "connector"
	CauseSideDefault = "side-default"
	// CauseUnstated is a person's placement that names no cause.
	CauseUnstated = "unstated"
	// CauseTitle, CauseTheme, CauseChannel and CauseLowQuality are a person's
	// causes naming the item's title, its theme, the channel that published
	// it, or that it held nothing worth keeping. A person may also name a
	// field of a sighting, such as subreddit.
	CauseTitle      = "title"
	CauseTheme      = "theme"
	CauseChannel    = "channel"
	CauseLowQuality = "low-quality"
	// CausePush is a person's push that named its destination, at the side
	// gate and at the read gate.
	CausePush = "push"
	// CauseNoteDraft is a read by a model whose note is the note stage's
	// draft: the model's own where the draft reached the note floor, and a
	// person's where they took a draft that fell below it. CauseNoteUnclear
	// sends to review an item whose draft fell below the floor.
	CauseNoteDraft   = "note-draft"
	CauseNoteUnclear = "note-unclear"
	// CauseLearningRefused sends to review, at the read gate, an item that a
	// person sorted to learning where the quality gate's newest answer did
	// not pass it. CauseShelfOpen is a person's opening of a shelved item,
	// and CauseShelfRemove their taking it off the shelf.
	CauseLearningRefused = "learning-refused"
	CauseShelfOpen       = store.CauseShelfOpen
	CauseShelfRemove     = store.CauseShelfRemove
)

// ReviewReason is one reason an item waits in review: its name, the line
// that `review list --help` gives it, and its group's heading in the TUI.
type ReviewReason struct {
	Name    string
	Help    string
	Heading string
}

// ReviewReasons lists why an item may wait in review, in the order that the
// TUI groups them. The gate's own doubts lead, because the reading pane
// decides them best. review-list.schema.json's reason enum holds these names.
var ReviewReasons = []ReviewReason{
	{store.ReasonQualityReview, "the quality gate sent it to review", "the gate asks"},
	{
		CauseGateWouldDrop, "the quality gate would drop it, and its family has not earned hiding it, or the family's draw showed it",
		"the gate would drop it",
	},
	{
		CauseRuleWouldHide, "a routing rule would drop it or place it without you, and the rule's review share drew it",
		"a rule would decide it",
	},
	{
		CauseNoteUnclear, "a model read waits on its note, whose draft fell below the note floor: write a note, or place it with none to take the draft",
		"the note's draft is unclear",
	},
	{
		CauseLearningRefused, "you sorted it to learning, and the quality gate's newest answer did not pass it: its card holds the gate's reason",
		"learning refused it",
	},
	{CauseBelowFloor, "the quality gate guessed fleet, or its guess fell below its floor", "fleet guess, or below its floor"},
	{
		CausePersonSorts, "a connector that sighted it declares person_sorts, so its guess only shows",
		"its connector leaves the side to you, the guess beside it",
	},
	{CauseOwnersWords, "it holds your own words, such as a memo's topic, which the quality gate never reads", "your own words"},
	{CauseTagsDisagree, "its sightings' tags name destinations on two sides", "tags name both sides"},
	{CauseNoGuess, "no tag or rule placed it, and the quality gate gave no answer", "no rule, no answer"},
	{CauseGateError, "the side gate could not read the set in force or its sightings", "the gate could not read the set or the sightings"},
	{store.ReasonNoParse, "no parse of it is usable", "no usable parse"},
}

// DefaultNoteFloor is the confidence at or above which a note's draft becomes
// the read's note, where serve's [note] floor names none. Below it, the item
// waits in review as note-unclear.
const DefaultNoteFloor = 0.7

// GuessFloor is the confidence at or above which the quality gate's personal
// guess places an item. A fleet guess places nothing at any confidence: a tag,
// a rule or a person puts an item on the fleet side, never a guess. Below the
// floor, and on a fleet guess, the item waits in review with its guess.
const GuessFloor = 0.7

// Who decided, as decided_by spells it, beside rule:<name>, model:<run> and
// person:<login>.
const (
	ByTag = store.DecidedByTag
	// ByGate is the side gate itself, where no rung placed the item.
	ByGate = store.DecidedByGate
)

// ByPerson is a person's decision, under their login.
func ByPerson(login string) string { return store.DecidedByPerson(login) }

// ByParent is a decision that an item took from its parent, under the
// parent's key.
func ByParent(key string) string { return store.DecidedByParent(key) }

func byRule(name string) string { return store.DecidedByRule(name) }

// ByModel is a model's decision, under the run that answered.
func ByModel(run string) string { return store.DecidedByModel(run) }

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

// Side is the side gate's decision on the item under key that sightings
// sighted, under set, each accepted rule in force, and the quality gate's
// guess, which is nil where it has given none.
func Side(set connectors.Set, accepted []store.RoutingRule, key string, sightings []store.Sighting, guess *Guess) store.Decision {
	if d, ok := BeforeModel(set, accepted, key, sightings); ok {
		return d
	}
	return byGuess(guess, personSorts(set, sightings))
}

// personSorts says whether a connector that sighted the item declares
// person_sorts under set, so no guess places it.
func personSorts(set connectors.Set, sightings []store.Sighting) bool {
	return slices.ContainsFunc(sightings, func(s store.Sighting) bool {
		c, ok := set.Connector(s.Connector)
		return ok && c.PersonSorts
	})
}

// BeforeModel is the side decision on the item under key that a sighting's
// tags or a routing rule make, under set and each accepted rule in force, and
// false where neither places the item, so the quality gate judges it.
func BeforeModel(set connectors.Set, accepted []store.RoutingRule, key string, sightings []store.Sighting) (store.Decision, bool) {
	if d, ok := byTags(set, sightings); ok {
		return d, true
	}
	return byRules(set, accepted, key, sightings)
}

// byGuess is the side decision on the quality gate's guess, and review for
// want of one. Where a person sorts the item, a guess sends it to review.
func byGuess(guess *Guess, personSorts bool) store.Decision {
	if guess == nil {
		return store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseNoGuess}
	}
	if personSorts {
		return store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CausePersonSorts}
	}
	confidence := guess.Confidence
	d := store.Decision{Gate: store.GateSide, DecidedBy: ByModel(guess.Run), Confidence: &confidence}
	if guess.Side == connectors.SidePersonal && confidence >= GuessFloor {
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

// byRules places the item under key by the first routing rule, in the order
// that rulesInOrder reads them, that one of its sightings holds for, and is
// false where none does. A block drops the item and a keep places it on its
// destination, each keeping the item's row. Where the rule's review share
// draws the item, it waits in review instead, marked rule-would-hide.
func byRules(set connectors.Set, accepted []store.RoutingRule, key string, sightings []store.Sighting) (store.Decision, bool) {
	for _, r := range rulesInOrder(set, accepted) {
		if !slices.ContainsFunc(sightings, func(s store.Sighting) bool { return holds(r.match, s) }) {
			continue
		}
		d := store.Decision{Gate: store.GateSide, Verdict: VerdictDrop, DecidedBy: byRule(r.name), Cause: CauseRule}
		if r.destination != "" {
			destination, _ := set.Destination(r.destination)
			d.Verdict, d.Destination = verdictOf(destination), &destination.Name
		}
		if r.drawsReview(key) {
			d.Verdict, d.Cause = VerdictReview, CauseRuleWouldHide
		}
		return d, true
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

// AfterSighting is the hook that serve's lander runs in the write that adds a
// sighting to an item the store held. It runs the gates over the item again,
// so a later sighting's tags, connector or fields decide an item that waits
// in review, and an item already placed stays where it is.
func AfterSighting(log *slog.Logger) func(context.Context, *store.Tx, string) error {
	return func(ctx context.Context, tx *store.Tx, key string) error {
		if _, err := Gate(ctx, tx, key, log); err != nil {
			return fmt.Errorf("the side gate on %s: %w", key, err)
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
	return func(ctx context.Context, tx *store.Tx) error { return regate(ctx, tx, log, "") }
}

// regate is AfterApply's work, which also decides again each item whose
// newest side decision reopened made and did not place: a rule's drop, or a
// review drawn by its share. A write of a rule's version passes byRule(name), so
// its items follow that version, or the rungs below it once it is retired. An
// empty reopened reopens nothing.
func regate(ctx context.Context, tx *store.Tx, log *slog.Logger, reopened string) error {
	keys, err := tx.UnplacedScannedKeys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := gate(ctx, tx, key, log, reopened); err != nil {
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
	return gate(ctx, tx, key, log, "")
}

// gate is Gate, which also decides again over an item whose newest side
// decision reopened made, where that decision drops the item or waits in
// review. Where the quality gate has yet to answer such an item, the rungs
// below the rules send it to review rather than leave the reopened decision
// standing.
func gate(ctx context.Context, tx *store.Tx, key string, log *slog.Logger, reopened string) ([]store.Decision, error) {
	newest, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil {
		return nil, err
	}
	reopens := sided && reopened != "" && newest.DecidedBy == reopened && (newest.Verdict == VerdictDrop || newest.Verdict == VerdictReview)
	if sided && !newest.Provisional() && !reopens {
		return nil, nil
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || !store.ReadingParseScanned(outputs) {
		return nil, err
	}
	item, _, err := tx.Item(ctx, key)
	if err != nil {
		return nil, err
	}
	if split, waits, err := byTopics(ctx, tx, item); err != nil || waits {
		return nil, err
	} else if split != nil {
		if sided && sameDecision(newest, *split) {
			return nil, nil
		}
		set, _, sightings, err := inputs(ctx, tx, key)
		if err != nil {
			return nil, err
		}
		return decide(ctx, tx, key, *split, set, sightings, nil, nil)
	}
	set, accepted, sightings, err := inputs(ctx, tx, key)
	side, ruled := BeforeModel(set, accepted, key, sightings)
	switch {
	case err != nil:
		log.ErrorContext(ctx, "the side gate could not read what it decides from, so the item waits in review", "item", key, "error", err)
		side = store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseGateError}
	case !ruled && item.SkipsQualityGate():
		side = store.Decision{Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: ByGate, Cause: CauseOwnersWords}
	case !ruled:
		var held *store.Decision
		if sided {
			held = &newest
		}
		var waits bool
		guessed := byGuess(GuessOf(outputs), personSorts(set, sightings))
		if side, waits, err = byQuality(ctx, tx, key, guessed, outputs, sightings, held); err != nil || (waits && !reopens) {
			return nil, err
		}
	}
	if sided && sameDecision(newest, side) {
		return nil, nil
	}
	return decide(ctx, tx, key, side, set, sightings, nil, nil)
}

// byTopics is the side decision on item where it is a memo, as
// store.Item.SplitsIntoTopics names one: split once its topics are landed, and
// true while its topics task is open, so the memo waits at the topics stage
// and no tag or rule places it first. A memo with neither, such as one whose
// topics task failed, and any other item, fall to the rungs below.
func byTopics(ctx context.Context, tx *store.Tx, item store.Item) (*store.Decision, bool, error) {
	if !item.SplitsIntoTopics() {
		return nil, false, nil
	}
	topics, err := tx.Topics(ctx, item.Key)
	if err != nil {
		return nil, false, err
	}
	if len(topics) > 0 {
		return &store.Decision{Gate: store.GateSide, Verdict: VerdictSplit, DecidedBy: ByGate, Cause: CauseTopics}, false, nil
	}
	_, open, err := tx.OpenTask(ctx, item.Key, store.StageTopics)
	return nil, open, err
}

// byQuality is the side gate's decision on the item under key, which no tag
// or rule placed, by the quality gate's newest decision: byTrust's where it
// dropped the item, review where it was unsure, and side, the guess's
// decision, where it passed the item. held is the item's newest side
// decision, and nil where it has none. It is true where the gate has not
// decided and the item's quality task is open, so the item waits at the
// quality gate.
func byQuality(ctx context.Context, tx *store.Tx, key string, side store.Decision, outputs []store.Output, sightings []store.Sighting,
	held *store.Decision,
) (store.Decision, bool, error) {
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
		d, err := byTrust(ctx, tx, key, quality, outputs, sightings, held)
		return d, false, err
	case VerdictReview:
		return store.Decision{
			Gate: store.GateSide, Verdict: VerdictReview, DecidedBy: quality.DecidedBy, Confidence: quality.Confidence,
			Cause: CauseQualityReview,
		}, false, nil
	}
	return side, false, nil
}

// byTrust is the side decision on an item that the quality gate dropped: a
// review marked gate-would-drop where a family of the item shows the drop,
// and a drop marked quality-drop where every family hides it. gatetrust.Shows
// decides from the answer, under the families of the prompt version, model
// and effort that gave it. It decides once for each answer of the gate, so a
// family that climbs or falls later moves no item already shown or hidden. A sighting
// that arrives after a hidden drop shows the item where its own family shows
// it, so a connector whose family stands lower still reaches the person.
func byTrust(ctx context.Context, tx *store.Tx, key string, quality store.Decision, outputs []store.Output, sightings []store.Sighting,
	held *store.Decision,
) (store.Decision, error) {
	read := sightings
	if held != nil && held.DecidedBy == quality.DecidedBy {
		switch held.Cause {
		case CauseGateWouldDrop:
			return *held, nil
		case CauseQualityDrop:
			read = slices.DeleteFunc(slices.Clone(sightings), func(s store.Sighting) bool { return s.FirstSeenTS <= held.DecidedTS })
			if len(read) == 0 {
				return *held, nil
			}
		}
	}
	d := store.Decision{Gate: store.GateSide, DecidedBy: quality.DecidedBy, Confidence: quality.Confidence}
	shows, err := gatetrust.Shows(ctx, tx, quality.DecidedBy, gatetrust.Answered(outputs, quality), gatetrust.Connectors(read))
	if err != nil {
		return d, err
	}
	d.Verdict, d.Cause = VerdictDrop, CauseQualityDrop
	if shows {
		d.Verdict, d.Cause = VerdictReview, CauseGateWouldDrop
	}
	return d, nil
}

// inputs reads the set in force, each accepted rule in force and the item's
// sightings, which the side gate decides from beside the quality gate's
// guess.
func inputs(ctx context.Context, tx *store.Tx, key string) (connectors.Set, []store.RoutingRule, []store.Sighting, error) {
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return connectors.Set{}, nil, nil, err
	}
	accepted, err := tx.RoutingRulesInForce(ctx)
	if err != nil {
		return set, nil, nil, err
	}
	sightings, err := tx.Sightings(ctx, key)
	return set, accepted, sightings, err
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
// item on a side: readAs where it is set, and otherwise what Read answers.
// Where a person decides the side, it withdraws the item's queued quality
// task, so no model judges an item that a person placed before the gate
// answered. A tag's or a rule's placement leaves the task queued, because the
// gate's answer on every item it reads is what its trust is earned from. So
// does a learning sort that the gate's want of an answer refused, which the
// person decides again once the gate answers.
func decide(ctx context.Context, tx *store.Tx, key string, side store.Decision, set connectors.Set, sightings []store.Sighting,
	note *string, readAs *store.Decision,
) ([]store.Decision, error) {
	side.ItemKey = key
	written, err := tx.AddDecision(ctx, side)
	if err != nil {
		return nil, err
	}
	if written.ByPerson() && (readAs == nil || readAs.Cause != CauseLearningRefused) {
		why := "withdrawn: " + written.DecidedBy + " decided the item's side before the quality gate ran"
		if err := tx.WithdrawQueued(ctx, key, store.StageQuality, why); err != nil {
			return nil, err
		}
	}
	out := []store.Decision{written}
	var read store.Decision
	if onASide(side.Verdict) {
		read = Read(set, sightings, side.Verdict, note)
		if readAs != nil {
			read = *readAs
		}
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
	if err := PassToChapters(ctx, tx, key); err != nil {
		return nil, err
	}
	return out, nil
}

// PassToChapters gives each chapter of the item under key the item's newest
// side decision and, where it places the item on a side, its newest read
// decision, each decided by ByParent. It appends nothing to a chapter whose
// newest decisions already say the same. It then brings each chapter's
// delivery and model read in line, as decide does for the item. A chapter
// takes its decisions only from its parent, so the gates never run on it.
func PassToChapters(ctx context.Context, tx *store.Tx, key string) error {
	chapters, err := tx.Chapters(ctx, key)
	if err != nil {
		return err
	}
	if len(chapters) == 0 {
		return nil
	}
	side, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil || !sided {
		return err
	}
	read, hasRead, err := tx.NewestDecision(ctx, key, store.GateRead)
	if err != nil {
		return err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return err
	}
	for _, chapter := range chapters {
		taken, err := take(ctx, tx, chapter.Key, ByParent(key), side)
		if err != nil {
			return err
		}
		var takenRead store.Decision
		if onASide(side.Verdict) && hasRead {
			if takenRead, err = take(ctx, tx, chapter.Key, ByParent(key), read); err != nil {
				return err
			}
		}
		if _, err := reconcileDelivery(ctx, tx, set, chapter, taken, takenRead); err != nil {
			return err
		}
		if err := reconcileModelRead(ctx, tx, chapter.Key, taken, takenRead); err != nil {
			return err
		}
	}
	return nil
}

// take appends to the item under key a copy of d decided by by, and answers
// the item's newest decision at d's gate, which is the copy, or an earlier
// decision that already says the same.
func take(ctx context.Context, tx *store.Tx, key, by string, d store.Decision) (store.Decision, error) {
	copied := store.Decision{
		ItemKey: key, Gate: d.Gate, Verdict: d.Verdict, DecidedBy: by, Confidence: d.Confidence, Cause: d.Cause,
		Destination: d.Destination, Note: d.Note, Lens: d.Lens,
	}
	newest, ok, err := tx.NewestDecision(ctx, key, d.Gate)
	if err != nil {
		return store.Decision{}, err
	}
	if ok && sameDecision(newest, copied) && text(newest.Note) == text(copied.Note) && text(newest.Lens) == text(copied.Lens) {
		return newest, nil
	}
	return tx.AddDecision(ctx, copied)
}

// ReadByModel says whether an item's newest side and read decisions send it
// to a model read: the side decision places it on a side, and the read
// decision reads it by a model.
func ReadByModel(side, read store.Decision) bool {
	return onASide(side.Verdict) && read.Verdict == connectors.ReadModel
}

// ModelReadDue says whether a replay runs the model read on an item: its
// newest decisions read it by a model, under a lens whose prompt this build
// holds, the read waits on no note's draft, no model read's task of it is
// open, and no done read of that lens's prompt in force over the item's
// reading parse answers the read's note. A read decision that the item took
// from its parent is never due, because a model read reads the item's own
// parse, and a book's chapter holds none.
func ModelReadDue(side, read store.Decision, outputs []store.Output, open bool) bool {
	return modelReadDue(side, read, outputs, open, true)
}

// modelReadDue is ModelReadDue, where current false counts a done read of any
// prompt version over any parse under the read's lens, as the read gate does.
// A new prompt or parse then reads an item again only through a replay, which
// estimates its tokens first. An output that names no lens counts under every
// lens.
func modelReadDue(side, read store.Decision, outputs []store.Output, open, current bool) bool {
	if open || !ReadByModel(side, read) || read.ByParent() || awaitsNote(read, outputs) {
		return false
	}
	p, ok := prompts.ModelRead[text(read.Lens)]
	note, _ := NoteOf(read, store.NewestDone(outputs, store.StageNote))
	return ok && !slices.ContainsFunc(outputs, func(o store.Output) bool {
		return readDoneUnder(read, o) && (!current || (o.ProducerVersion == p.Version && store.ReadsQuotedParse(o, outputs))) &&
			store.NoteAnswered(o.NoteDigest, note)
	})
}

// readDoneUnder says whether o is a done model read under read's lens, as
// underLens decides.
func readDoneUnder(read store.Decision, o store.Output) bool {
	return o.Stage == store.StageModelRead && o.State == store.OutputDone && underLens(read, o)
}

// underLens says whether o was read under read's lens. An output that names
// no lens counts under every lens, since it was read before any lens but the
// fleet's.
func underLens(read store.Decision, o store.Output) bool {
	return o.Lens == nil || *o.Lens == text(read.Lens)
}

// reconcileModelRead queues the note's draft of the item under key where the
// read waits on one, and otherwise its model read where the read gate finds it
// due, so a new note reads an item again. It withdraws a queued draft where
// the read carries its note, and a queued draft or read where the item's
// newest decisions read it otherwise.
func reconcileModelRead(ctx context.Context, tx *store.Tx, key string, side, read store.Decision) error {
	if !ReadByModel(side, read) {
		by := side.DecidedBy
		if onASide(side.Verdict) {
			by = read.DecidedBy
		}
		why := "withdrawn: " + by + " decided the item is not read by a model"
		if err := tx.WithdrawQueued(ctx, key, store.StageNote, why); err != nil {
			return err
		}
		return tx.WithdrawQueued(ctx, key, store.StageModelRead, why)
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil {
		return err
	}
	if !awaitsNote(read, outputs) {
		if err := tx.WithdrawQueued(ctx, key, store.StageNote, "withdrawn: the item's read carries its note"); err != nil {
			return err
		}
	}
	_, noteOpen, err := tx.OpenTask(ctx, key, store.StageNote)
	if err != nil {
		return err
	}
	if noteDue(read, outputs, noteOpen) {
		_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageNote, Capability: store.StageNote})
		return err
	}
	_, open, err := tx.OpenTask(ctx, key, store.StageModelRead)
	if err != nil || !modelReadDue(side, read, outputs, open, false) {
		return err
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageModelRead, Capability: store.StageModelRead})
	return err
}

// DueReader reads what Due decides from beside an item's decisions: its
// bytes' live homes, and its book's deliveries where it is a book's chapter.
// A write's Tx and the store's reader each are one.
type DueReader interface {
	LiveHomes(ctx context.Context, sum string) ([]store.Home, error)
	Deliveries(ctx context.Context, key string) ([]store.Delivery, error)
}

// DueDelivery is where Due sends an item: the destination whose deliverer
// writes it, or why none does. Path is the item's file below the deliverer's
// root, for a deliverer that writes by file. BookDestinationID is the
// destination's id for a chapter's book's record. Each is empty where the
// item reaches no deliverer, or where it does not apply.
type DueDelivery struct {
	Destination       connectors.Destination
	Undelivered       *connectors.Undelivered
	Path              *string
	BookDestinationID string
}

// Due is where an item's newest side and read decisions send it for delivery
// under set, and false where they send it nowhere: the side decision places
// it on no side, or the read decision reads it other than as is. It reads the
// item's live homes and its book's deliveries through r. A chapter names its
// book's record, so it is due only once its book's delivery to the same
// destination settled on one. The queue, a replay, an apply and items show
// each ask it.
func Due(ctx context.Context, r DueReader, set connectors.Set, item store.Item, side, read store.Decision) (DueDelivery, bool, error) {
	if !onASide(side.Verdict) || read.Verdict != connectors.ReadAsIs {
		return DueDelivery{}, false, nil
	}
	var homes []store.Home
	var err error
	if item.BlobSHA256 != nil {
		if homes, err = r.LiveHomes(ctx, *item.BlobSHA256); err != nil {
			return DueDelivery{}, false, err
		}
	}
	var due DueDelivery
	due.Destination, due.Undelivered = set.DeliveryTo(side, item, homes)
	switch {
	case due.Undelivered != nil:
	case item.PlacedThroughParent():
		book, err := r.Deliveries(ctx, *item.ParentKey)
		if err != nil {
			return DueDelivery{}, false, err
		}
		due.BookDestinationID, due.Undelivered = bookRecord(due.Destination, side, book)
	case item.BlobSHA256 != nil:
		if due.Path, err = connectors.FilePath(due.Destination.Deliverer, homes); err != nil {
			return DueDelivery{}, false, err
		}
	}
	return due, true, nil
}

// bookRecord is the id of the record at d that holds a chapter's book, from
// the book's settled delivery there. Where the book's delivery to d has not
// settled on an id, or d refused the book, it is why the chapter waits.
func bookRecord(d connectors.Destination, side store.Decision, book []store.Delivery) (string, *connectors.Undelivered) {
	i := slices.IndexFunc(book, func(b store.Delivery) bool { return b.Destination == d.Name })
	switch {
	case i >= 0 && book[i].Outcome == store.DeliveryRefused:
		return "", &connectors.Undelivered{Reason: connectors.UndeliveredBookRefused, Side: side.Verdict, Destination: d.Name}
	case i < 0 || !book[i].Settled() || text(book[i].DestinationID) == "":
		return "", &connectors.Undelivered{Reason: connectors.UndeliveredBookUnsettled, Side: side.Verdict, Destination: d.Name}
	}
	return *book[i].DestinationID, nil
}

// QueueDelivery brings the deliveries of the item under key, and of each of
// its chapters, in line with what Due answers for their newest decisions
// under the set in force. So a replay of one book reaches its chapters. It
// answers whether it queued a delivery.
func QueueDelivery(ctx context.Context, tx *store.Tx, key string) (bool, error) {
	item, ok, err := tx.Item(ctx, key)
	if err != nil || !ok {
		return false, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return false, err
	}
	queued, err := queueDeliveryUnder(ctx, tx, set, item)
	if err != nil {
		return false, err
	}
	chapters, err := queueChapterDeliveries(ctx, tx, set, key)
	return queued || chapters > 0, err
}

// AfterDelivery is the hook that serve runs in the write that ends a deliver
// task done. It brings the delivery of each chapter of the task's item in
// line, so a book whose delivery settled queues its chapters' in that write,
// and a refused book queues none.
func AfterDelivery(ctx context.Context, tx *store.Tx, task store.Task) error {
	if task.ItemKey == nil {
		return nil
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return err
	}
	_, err = queueChapterDeliveries(ctx, tx, set, *task.ItemKey)
	return err
}

// QueueUndeliveredChapters queues the delivery of each chapter that holds
// none while its book's delivery settled on a record, and answers how many
// it queued. serve runs it once it listens. So the chapters of a book
// delivered before its deliverer wrote chapters reach the book's record,
// with no replay asked for.
func QueueUndeliveredChapters(ctx context.Context, tx *store.Tx) (int, error) {
	keys, err := tx.UndeliveredChapterKeys(ctx)
	if err != nil || len(keys) == 0 {
		return 0, err
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return 0, err
	}
	queued := 0
	for _, key := range keys {
		item, ok, err := tx.Item(ctx, key)
		if err != nil {
			return queued, err
		}
		if !ok {
			continue
		}
		did, err := queueDeliveryUnder(ctx, tx, set, item)
		if err != nil {
			return queued, fmt.Errorf("queue the delivery of %s: %w", key, err)
		}
		if did {
			queued++
		}
	}
	return queued, nil
}

// queueChapterDeliveries brings the delivery of each chapter of the book
// under key in line, in the chapters' order, and answers how many it queued.
func queueChapterDeliveries(ctx context.Context, tx *store.Tx, set connectors.Set, key string) (int, error) {
	chapters, err := tx.Chapters(ctx, key)
	if err != nil {
		return 0, err
	}
	queued := 0
	for _, chapter := range chapters {
		did, err := queueDeliveryUnder(ctx, tx, set, chapter)
		if err != nil {
			return queued, fmt.Errorf("queue the delivery of %s: %w", chapter.Key, err)
		}
		if did {
			queued++
		}
	}
	return queued, nil
}

// queueDeliveryUnder brings item's deliveries in line under set, as its
// newest decisions send it.
func queueDeliveryUnder(ctx context.Context, tx *store.Tx, set connectors.Set, item store.Item) (bool, error) {
	side, _, err := tx.NewestDecision(ctx, item.Key, store.GateSide)
	if err != nil {
		return false, err
	}
	read, _, err := tx.NewestDecision(ctx, item.Key, store.GateRead)
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
	due, ok, err := Due(ctx, tx, set, item, side, read)
	if err != nil {
		return false, err
	}
	keep := ""
	if ok && due.Undelivered == nil {
		keep = due.Destination.Name
	}
	if err := tx.WithdrawDeliveries(ctx, item.Key, keep); err != nil || keep == "" {
		return false, err
	}
	var chapter *store.ChapterInBook
	if item.PlacedThroughParent() {
		if chapter, err = chapterInBook(ctx, tx, item.Key, due.BookDestinationID); err != nil {
			return false, err
		}
	}
	return tx.QueueDelivery(ctx, item, due.Path, chapter, side.Playlist, due.Destination.Name, due.Destination.Deliverer)
}

// chapterInBook is where the chapter under key sits in the record book: at
// the pages of the newest sections that name the chapter.
func chapterInBook(ctx context.Context, tx *store.Tx, key, book string) (*store.ChapterInBook, error) {
	s, ok, err := tx.ChapterSection(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no section names the chapter %s, so it has no pages to deliver", key)
	}
	return &store.ChapterInBook{
		BookDestinationID: book, PDFPageStart: s.PDFPageStart, PDFPageEnd: s.PDFPageEnd,
		PrintedPageStart: s.PrintedPageStart, PrintedPageEnd: s.PrintedPageEnd,
	}, nil
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
	Note *string
	// Pushed is a push that named its destination, whose cause is CausePush.
	// The person decides the read too, as is, so no model stage reads the
	// item whatever its connector's read rule or its side's default.
	Pushed   bool
	Login    string
	ClientID string
	// Presented is the review card as drawn when the person decided, and
	// DecidedAfterMS how long after the drawing they decided. The side
	// decision keeps both, and the read decision after it neither.
	Presented      *store.Presented
	DecidedAfterMS *int64
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

// ChapterError is a placement of a book's chapter. A chapter takes every
// decision from its book, so a placement of its own would be undone by the
// book's next one.
type ChapterError struct {
	Chapter string
	Book    string
}

func (e *ChapterError) Error() string {
	return fmt.Sprintf("%s is a chapter of %s and takes its book's placement, so place %s", e.Chapter, e.Book, e.Book)
}

// PlaylistDestinationError is a placement on a destination whose deliverer
// adds the item to the playlist that a sort picks, which no placement carries.
type PlaylistDestinationError struct {
	Destination string
	// Verb is the verb whose sort places an item there.
	Verb string
}

func (e *PlaylistDestinationError) Error() string {
	return fmt.Sprintf("only a sort by %s places an item at %s, since its deliverer adds the item to the playlist that the sort picks, so sort the item by %s instead",
		e.Verb, e.Destination, e.Verb)
}

// PushOnlyError is a placement other than a push on a push-only destination,
// whose program only a push hands the item to.
type PushOnlyError struct {
	Destination string
}

func (e *PushOnlyError) Error() string {
	return fmt.Sprintf("only a push places an item at %s, since a push hands the item to its program, so push the item there instead", e.Destination)
}

// Place appends a person's decision on the item under key: on p's destination,
// or a drop where p names none. Where the destination takes a side, the read
// gate's decision follows it. A placement with no note of an item that holds a
// note's draft takes the draft, so its read by a model answers the draft, as
// the person's decision. A person can place any item, including one that
// a rule already placed. A placement that repeats the newest side decision
// appends nothing, and answers the decisions it made. A placement on a
// destination of an item whose drop the gate showed or hid disagrees with the
// gate, which returns each family of its answer to rung 0. Place answers
// store.ErrNoItem where the store holds no such item, a NotDeclaredError
// for a destination that the set in force does not declare, a PushOnlyError
// for a push-only destination that p does not push to, a
// PlaylistDestinationError for a destination that only a sort places on, and
// a ChapterError for a book's chapter.
func Place(ctx context.Context, tx *store.Tx, key string, p Placement) ([]store.Decision, error) {
	if item, ok, err := tx.Item(ctx, key); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("%s: %w", key, store.ErrNoItem)
	} else if item.PlacedThroughParent() {
		return nil, &ChapterError{Chapter: key, Book: *item.ParentKey}
	}
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return nil, err
	}
	switch {
	case p.Pushed:
		p.Cause = CausePush
	case p.Cause == "":
		p.Cause = CauseUnstated
	}
	side := store.Decision{
		Gate: store.GateSide, Verdict: VerdictDrop, DecidedBy: ByPerson(p.Login), Cause: p.Cause, Note: p.Note,
		Presented: p.Presented, DecidedAfterMS: p.DecidedAfterMS,
	}
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
		if destination.PushOnly && !p.Pushed {
			return nil, &PushOnlyError{Destination: destination.Name}
		}
		if connectors.TakesPlaylist(destination.Deliverer) {
			e := &PlaylistDestinationError{Destination: destination.Name}
			if i := slices.IndexFunc(Verbs, func(v VerbRoute) bool { return v.Deliverer == destination.Deliverer }); i >= 0 {
				e.Verb = Verbs[i].Verb
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
	held, sided, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil {
		return nil, err
	}
	var readAs *store.Decision
	switch {
	case p.Pushed:
		readAs = &store.Decision{Gate: store.GateRead, Verdict: connectors.ReadAsIs, DecidedBy: side.DecidedBy, Cause: CausePush, ClientID: side.ClientID}
	case store.NoteDigest(p.Note) == nil && onASide(side.Verdict):
		if readAs, err = takeDraft(ctx, tx, key, Read(set, sightings, side.Verdict, nil), side); err != nil {
			return nil, err
		}
	}
	made, err := decide(ctx, tx, key, side, set, sightings, p.Note, readAs)
	if err != nil || !sided || p.Destination == "" || !gateDropped(held) {
		return made, err
	}
	_, err = gatetrust.Disagree(ctx, tx, key, side.DecidedBy, p.ClientID)
	return made, err
}

// gateDropped says whether side is the side gate's decision on a drop of the
// quality gate's, shown in review or hidden.
func gateDropped(side store.Decision) bool {
	_, byModel := side.ModelRun()
	return byModel && (side.Cause == CauseGateWouldDrop || side.Cause == CauseQualityDrop)
}

// repeated answers the decisions that an earlier copy of side made, and is
// false where the item's newest side decision is not one, or where its newest
// read decision waits in review, which only a new placement answers.
func repeated(ctx context.Context, tx *store.Tx, key string, side store.Decision) ([]store.Decision, bool, error) {
	newest, ok, err := tx.NewestDecision(ctx, key, store.GateSide)
	if err != nil || !ok {
		return nil, false, err
	}
	if newest.Verdict != side.Verdict || newest.DecidedBy != side.DecidedBy || newest.Cause != side.Cause ||
		text(newest.Destination) != text(side.Destination) || text(newest.Note) != text(side.Note) ||
		text(newest.ClientID) != text(side.ClientID) || text(newest.Playlist) != text(side.Playlist) {
		return nil, false, nil
	}
	made := []store.Decision{newest}
	if read, ok, err := tx.NewestDecision(ctx, key, store.GateRead); err != nil {
		return nil, false, err
	} else if ok && read.Verdict == VerdictReview {
		return nil, false, nil
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
