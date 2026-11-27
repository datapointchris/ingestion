package connectors

import (
	"context"
	"fmt"
	"slices"

	"github.com/datapointchris/ingestion/store"
)

// MatchOf is what an accepted rule holds for, as a declared rule's match
// names it.
func MatchOf(r store.RoutingRule) Match {
	return Match{Connector: text(r.MatchConnector), Tag: text(r.MatchTag), Field: text(r.MatchField), Values: r.MatchValues}
}

// WithMatch is r holding for m, each condition that m leaves empty stored as
// none.
func WithMatch(r store.RoutingRule, m Match) store.RoutingRule {
	r.MatchConnector, r.MatchTag, r.MatchField, r.MatchValues = optional(m.Connector), optional(m.Tag), optional(m.Field), m.Values
	return r
}

// ruleTargetProblems is each reason that s refuses a routing rule's match
// and destination, declared or accepted: a connector that s does not declare,
// and a destination that s does not declare or that only a push places an
// item at. An empty destination names none.
func (s Set) ruleTargetProblems(m Match, destination string) []string {
	var out []string
	if _, ok := s.Connector(m.Connector); m.Connector != "" && !ok {
		out = append(out, fmt.Sprintf("match names connector %s, which the set does not declare", m.Connector))
	}
	if destination == "" {
		return out
	}
	switch d, ok := s.Destination(destination); {
	case !ok:
		out = append(out, fmt.Sprintf("destination %s is not one %s declares", destination, DestinationsFile))
	case d.PushOnly:
		out = append(out, fmt.Sprintf("destination %s is push-only, so only a push places an item there", destination))
	case TakesPlaylist(d.Deliverer):
		out = append(out, fmt.Sprintf("destination %s adds to the playlist that a sort picks, so only a sort places an item there", destination))
	}
	return out
}

// declaresRule says whether s declares a routing rule called name.
func (s Set) declaresRule(name string) bool {
	return slices.ContainsFunc(s.RoutingRules, func(r RoutingRule) bool { return r.Name == name })
}

// AcceptedRuleProblems is each reason that s refuses r, a routing rule that a
// person accepted: a name that s declares a rule under, which would make its
// decisions' rule:<name> name two rules, each of ruleTargetProblems, a keep
// with no destination or on one that takes no side, and a block's
// destination.
func (s Set) AcceptedRuleProblems(r store.RoutingRule) []string {
	var out []string
	if s.declaresRule(r.Name) {
		out = append(out, RoutingRulesFile+" declares a rule of this name")
	}
	out = append(out, s.ruleTargetProblems(MatchOf(r), text(r.Destination))...)
	switch r.Action {
	case store.RuleKeep:
		if d, ok := s.Destination(text(r.Destination)); ok && !d.PushOnly && d.Side == "" {
			out = append(out, fmt.Sprintf("destination %s takes no side, so a keep cannot place an item there: block the items instead", d.Name))
		} else if r.Destination == nil {
			out = append(out, "a keep places what it matches, so it names a destination")
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

// acceptedRulesRefused is each accepted rule that s would leave unable to
// hold, read inside the write that applies s: a rule of any version, a
// retired one included, whose name s declares, because its decisions already
// carry rule:<name>, and a rule in force that AcceptedRuleProblems refuses
// under s. Each problem names the command that takes the accepted rule out of
// the way.
func (s Set) acceptedRulesRefused(ctx context.Context, tx *store.Tx) ([]Invalid, error) {
	accepted, err := tx.RoutingRules(ctx)
	if err != nil {
		return nil, err
	}
	var out []Invalid
	for _, r := range accepted {
		if s.declaresRule(r.Name) {
			out = append(out, Invalid{Declaration: "routing rule " + r.Name, Problems: []string{
				"an accepted routing rule holds this name, so rule:" + r.Name + " would name two rules: name this one otherwise",
			}})
			continue
		}
		if r.Retired {
			continue
		}
		if problems := s.AcceptedRuleProblems(r); len(problems) > 0 {
			for i, p := range problems {
				problems[i] = p + ": retire it first with `ingestion gates routing-rules retire " + r.Name + "`"
			}
			out = append(out, Invalid{Declaration: "accepted routing rule " + r.Name, Problems: problems})
		}
	}
	return out, nil
}

func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
