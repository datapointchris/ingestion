package store

import (
	"context"
	"database/sql"
	"fmt"
)

// The actions of an accepted routing rule: a block drops each item that the
// rule matches, and a keep places it on the rule's destination.
const (
	RuleBlock = "block"
	RuleKeep  = "keep"
)

// RoutingRule is one version of a routing rule that a person accepted, as
// routing_rules holds it.
type RoutingRule struct {
	Name     string `json:"name"`
	Sequence int    `json:"sequence"`
	Action   string `json:"action"`
	// MatchConnector, MatchTag, MatchField and MatchValues are what the rule
	// holds for, as a declared rule's match names them. A nil one names
	// nothing.
	MatchConnector *string  `json:"match_connector"`
	MatchTag       *string  `json:"match_tag"`
	MatchField     *string  `json:"match_field"`
	MatchValues    []string `json:"match_values"`
	// Destination is a keep's, and nil on a block.
	Destination *string `json:"destination"`
	// ReviewShare is the share of the items that the rule matches that wait
	// in review instead, marked rule-would-hide.
	ReviewShare float64 `json:"review_share"`
	// Retired is a version that takes the rule out of force. It copies the
	// version before it.
	Retired bool `json:"retired"`
	// Proposal names the proposal that the person accepted, where one did.
	Proposal  *string `json:"proposal"`
	DecidedBy string  `json:"decided_by"`
	ClientID  *string `json:"client_id"`
	DecidedTS string  `json:"decided_ts"`
}

// AddRoutingRule appends r as its name's newest version, numbered after the
// name's last, and answers it as written.
func (t *Tx) AddRoutingRule(ctx context.Context, r RoutingRule) (RoutingRule, error) {
	var last sql.NullInt64
	if err := t.tx.QueryRowContext(ctx, `SELECT max(sequence) FROM routing_rules WHERE name = ?`, r.Name).Scan(&last); err != nil {
		return r, fmt.Errorf("number a version of routing rule %s: %w", r.Name, err)
	}
	if r.MatchValues == nil {
		r.MatchValues = []string{}
	}
	r.Sequence = int(last.Int64) + 1
	r.DecidedTS = t.stamp()
	return r, t.put(ctx, table("routing_rules"), r)
}

// NewestRoutingRule reads the newest version of the rule called name as this
// write sees it, and false where none was accepted.
func (t *Tx) NewestRoutingRule(ctx context.Context, name string) (RoutingRule, bool, error) {
	return first[RoutingRule](ctx, t.tx, table("routing_rules"), `name = ? ORDER BY sequence DESC`, name)
}

// newestRuleVersion selects the newest version of each name.
const newestRuleVersion = `sequence = (SELECT max(x.sequence) FROM routing_rules x WHERE x.name = routing_rules.name)`

// RoutingRulesInForce reads the newest version of each rule that is not
// retired, by name, as this write sees them.
func (t *Tx) RoutingRulesInForce(ctx context.Context) ([]RoutingRule, error) {
	return readInto[RoutingRule](ctx, t.tx, table("routing_rules"), newestRuleVersion+` AND retired = 0 ORDER BY name`)
}

// RoutingRules reads the newest version of every rule, each retired one
// included, by name.
func (s *Store) RoutingRules(ctx context.Context) ([]RoutingRule, error) {
	return readInto[RoutingRule](ctx, s.read, table("routing_rules"), newestRuleVersion+` ORDER BY name`)
}
