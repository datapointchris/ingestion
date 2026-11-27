package route

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/datapointchris/earnedtrust/seededhash"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/store"
)

// rule is one routing rule as the rule rung reads it: one that the set in
// force declares, or one that a person accepted. A block names no
// destination.
type rule struct {
	name        string
	match       connectors.Match
	destination string
	reviewShare float64
}

// drawsReview says whether the rule's review share draws the item under key.
// The draw is seeded by the rule's name, so a replay over the same rule and
// keys draws the same items, and a higher share keeps every item that a lower
// one drew.
func (r rule) drawsReview(key string) bool {
	return r.reviewShare > 0 && seededhash.Fraction(byRule(r.name), key) < r.reviewShare
}

// rulesInOrder is every routing rule that the rule rung reads, in the order
// that it reads them. The set's declared rules come first, in the set's
// order, because a person wrote each by hand. Each accepted rule in force
// follows, every keep before every block, by name within each. A keep goes
// first because a like on a feature wins over a dislike, as NewsBlur's
// trainer weighs them.
func rulesInOrder(set connectors.Set, accepted []store.RoutingRule) []rule {
	out := make([]rule, 0, len(set.RoutingRules)+len(accepted))
	for _, r := range set.RoutingRules {
		out = append(out, rule{name: r.Name, match: r.Match, destination: r.Destination})
	}
	blocksLast := func(r store.RoutingRule) int {
		if r.Action == store.RuleBlock {
			return 1
		}
		return 0
	}
	accepted = slices.SortedFunc(slices.Values(accepted), func(a, b store.RoutingRule) int {
		return cmp.Or(cmp.Compare(blocksLast(a), blocksLast(b)), strings.Compare(a.Name, b.Name))
	})
	for _, r := range accepted {
		out = append(out, rule{name: r.Name, match: MatchOf(r), destination: text(r.Destination), reviewShare: r.ReviewShare})
	}
	return out
}

// MatchOf is what an accepted rule holds for, as a declared rule's match
// names it.
func MatchOf(r store.RoutingRule) connectors.Match {
	return connectors.Match{Connector: text(r.MatchConnector), Tag: text(r.MatchTag), Field: text(r.MatchField), Values: r.MatchValues}
}

// RuleError is an accepted rule that the set in force refuses, with each
// reason.
type RuleError struct {
	Name     string
	Problems []string
}

func (e *RuleError) Error() string {
	return fmt.Sprintf("the set in force refuses routing rule %s: %s", e.Name, strings.Join(e.Problems, "; "))
}

// ErrNoRule is a retirement of a rule that no person accepted.
var ErrNoRule = errors.New("no routing rule of that name was accepted")

// ruleProblems is each reason that set refuses r: a name that a declared rule
// holds, which would make its decisions' rule:<name> name two rules, a match
// naming a connector that set does not declare, a keep's destination that set
// does not declare, that only a push places an item at, or that takes no
// side, and a block's destination.
func ruleProblems(set connectors.Set, r store.RoutingRule) []string {
	var out []string
	if slices.ContainsFunc(set.RoutingRules, func(d connectors.RoutingRule) bool { return d.Name == r.Name }) {
		out = append(out, "routing-rules.yml declares a rule of this name")
	}
	if c := text(r.MatchConnector); c != "" {
		if _, ok := set.Connector(c); !ok {
			out = append(out, fmt.Sprintf("match names connector %s, which the set in force does not declare", c))
		}
	}
	switch r.Action {
	case store.RuleKeep:
		d, ok := set.Destination(text(r.Destination))
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("destination %s is not one the set in force declares", text(r.Destination)))
		case d.PushOnly:
			out = append(out, fmt.Sprintf("destination %s is push-only, so only a push places an item there", d.Name))
		case d.Side == "":
			out = append(out, fmt.Sprintf("destination %s takes no side, so a keep cannot place an item there: block the items instead", d.Name))
		}
	case store.RuleBlock:
		if r.Destination != nil {
			out = append(out, "a block drops what it matches, so it names no destination")
		}
	default:
		out = append(out, fmt.Sprintf("action is block or keep, not %q", r.Action))
	}
	return out
}

// AcceptRule appends r as its name's newest version, unless the newest
// already says the same by the same hand. Where it appends, it runs the gates
// again as an apply of a changed set does, so the rule decides each scanned
// item that no settled decision placed. An item that a tag, a rule, a model's
// confident guess or a person placed stays where it is. It answers the
// newest version and whether it appended one, and a *RuleError where the set
// in force refuses r.
func AcceptRule(ctx context.Context, tx *store.Tx, r store.RoutingRule, log *slog.Logger) (store.RoutingRule, bool, error) {
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return r, false, err
	}
	if problems := ruleProblems(set, r); len(problems) > 0 {
		return r, false, &RuleError{Name: r.Name, Problems: problems}
	}
	r.Retired = false
	newest, ok, err := tx.NewestRoutingRule(ctx, r.Name)
	if err != nil || (ok && sameVersion(newest, r)) {
		return newest, false, err
	}
	written, err := tx.AddRoutingRule(ctx, r)
	if err != nil {
		return written, false, err
	}
	return written, true, AfterApply(log)(ctx, tx)
}

// RetireRule appends a retired copy of the newest version of the rule called
// name, by decidedBy from clientID, which takes the rule out of force. It
// appends nothing where the rule is retired already. An item that the rule
// placed or dropped stays where it is, and one that its share sent to review
// still waits there. It answers ErrNoRule where no version of name was
// accepted.
func RetireRule(ctx context.Context, tx *store.Tx, name, decidedBy string, clientID *string) (store.RoutingRule, bool, error) {
	newest, ok, err := tx.NewestRoutingRule(ctx, name)
	switch {
	case err != nil:
		return newest, false, err
	case !ok:
		return newest, false, fmt.Errorf("%s: %w", name, ErrNoRule)
	case newest.Retired:
		return newest, false, nil
	}
	newest.Retired, newest.DecidedBy, newest.ClientID, newest.Proposal = true, decidedBy, clientID, nil
	written, err := tx.AddRoutingRule(ctx, newest)
	return written, err == nil, err
}

// sameVersion says whether b says what a says, by the same hand and from the
// same proposal.
func sameVersion(a, b store.RoutingRule) bool {
	return a.Action == b.Action && a.Retired == b.Retired && slices.Equal(a.MatchValues, b.MatchValues) &&
		text(a.MatchConnector) == text(b.MatchConnector) && text(a.MatchTag) == text(b.MatchTag) && text(a.MatchField) == text(b.MatchField) &&
		text(a.Destination) == text(b.Destination) && a.ReviewShare == b.ReviewShare && text(a.Proposal) == text(b.Proposal) &&
		a.DecidedBy == b.DecidedBy
}
