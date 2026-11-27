package route

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
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
		out = append(out, rule{name: r.Name, match: connectors.MatchOf(r), destination: text(r.Destination), reviewShare: r.ReviewShare})
	}
	return out
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

// AcceptRule appends r as its name's newest version, unless the newest
// already says the same by the same hand. Then, whether it appended or not,
// it runs the gates again as regate does for the rule, so a write sent again
// after a failed gate decides what the first could not. It answers the newest
// version and whether it appended one, and a *RuleError where the set in
// force refuses r.
func AcceptRule(ctx context.Context, tx *store.Tx, r store.RoutingRule, log *slog.Logger) (store.RoutingRule, bool, error) {
	set, err := connectors.AppliedSet(ctx, tx)
	if err != nil {
		return r, false, err
	}
	if problems := set.AcceptedRuleProblems(r); len(problems) > 0 {
		return r, false, &RuleError{Name: r.Name, Problems: problems}
	}
	r.Retired = false
	written, ok, err := tx.NewestRoutingRule(ctx, r.Name)
	if err != nil {
		return written, false, err
	}
	appended := !ok || !sameVersion(written, r)
	if appended {
		if written, err = tx.AddRoutingRule(ctx, r); err != nil {
			return written, false, err
		}
	}
	return written, appended, regate(ctx, tx, log, byRule(r.Name))
}

// RetireRule appends a retired copy of the newest version of the rule called
// name, by decidedBy from clientID, which takes the rule out of force. It
// appends nothing where the rule is retired already. Either way it then runs
// the gates again as regate does for the rule, so each item that the rule
// dropped, or that its share sent to review, is decided again without it. An
// item that it placed on a destination stays there. It answers ErrNoRule
// where no version of name was accepted.
func RetireRule(ctx context.Context, tx *store.Tx, name, decidedBy string, clientID *string, log *slog.Logger) (store.RoutingRule, bool, error) {
	newest, ok, err := tx.NewestRoutingRule(ctx, name)
	switch {
	case err != nil:
		return newest, false, err
	case !ok:
		return newest, false, fmt.Errorf("%s: %w", name, ErrNoRule)
	}
	written, appended := newest, !newest.Retired
	if appended {
		newest.Retired, newest.DecidedBy, newest.ClientID, newest.Proposal = true, decidedBy, clientID, nil
		if written, err = tx.AddRoutingRule(ctx, newest); err != nil {
			return written, false, err
		}
	}
	return written, appended, regate(ctx, tx, log, byRule(name))
}

// sameVersion says whether b says what a says, by the same hand and from the
// same proposal: every field but its number, its time and the client that
// sent it, so a field added to a version counts without naming it here.
func sameVersion(a, b store.RoutingRule) bool {
	said := func(r store.RoutingRule) ([]byte, error) {
		r.Sequence, r.DecidedTS, r.ClientID = 0, "", nil
		if r.MatchValues == nil {
			r.MatchValues = []string{}
		}
		return json.Marshal(r)
	}
	ja, errA := said(a)
	jb, errB := said(b)
	return errA == nil && errB == nil && bytes.Equal(ja, jb)
}
