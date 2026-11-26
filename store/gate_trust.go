package store

import (
	"context"
	"database/sql"
	"fmt"
)

// Mark is a person's word on an item, as marks holds it.
type Mark struct {
	ItemKey   string  `json:"item_key"`
	Sequence  int     `json:"sequence"`
	Mark      string  `json:"mark"`
	DecidedBy string  `json:"decided_by"`
	ClientID  *string `json:"client_id"`
	// GateVerdict and GateDecidedBy are the quality gate's newest decision on
	// the item when the mark was made, and PromptVersion the version of the
	// prompt that answered it. Each is nil where the gate had decided nothing.
	GateVerdict   *string `json:"gate_verdict"`
	GateDecidedBy *string `json:"gate_decided_by"`
	PromptVersion *string `json:"prompt_version"`
	// Agreement is agreed, disagreed or neither, as the mark read against
	// GateVerdict.
	Agreement string `json:"agreement"`
	// Families names each family whose standing the mark moved.
	Families []string `json:"families"`
	MarkedTS string   `json:"marked_ts"`
}

// GateTrust is one family's standing on the trust ladder, as gate_trust holds
// it.
type GateTrust struct {
	Family        string  `json:"family"`
	Connector     string  `json:"connector"`
	PromptVersion string  `json:"prompt_version"`
	Rung          int     `json:"rung"`
	Run           int     `json:"run"`
	StraightRun   int     `json:"straight_run"`
	Share         float64 `json:"share"`
	Threshold     int     `json:"threshold"`
	FirstShare    float64 `json:"first_share"`
	AsksPromotion bool    `json:"asks_promotion"`
	// PromotedBy is person:<login> once a person said yes, and nil before.
	PromotedBy       *string `json:"promoted_by"`
	PromotedClientID *string `json:"promoted_client_id"`
	PromotedTS       *string `json:"promoted_ts"`
	// LastChange names what moved the row last, and LastItemKey the item it
	// moved on, where one did.
	LastChange  string  `json:"last_change"`
	LastItemKey *string `json:"last_item_key"`
	CreatedTS   string  `json:"created_ts"`
	UpdatedTS   string  `json:"updated_ts"`
}

// Promoted says whether a person promoted the family.
func (g GateTrust) Promoted() bool { return g.PromotedTS != nil }

// AddMark appends a mark on an item, numbered after the item's last, and
// answers it as written.
func (t *Tx) AddMark(ctx context.Context, m Mark) (Mark, error) {
	var last sql.NullInt64
	if err := t.tx.QueryRowContext(ctx, `SELECT max(sequence) FROM marks WHERE item_key = ?`, m.ItemKey).Scan(&last); err != nil {
		return m, fmt.Errorf("number a mark on %s: %w", m.ItemKey, err)
	}
	if m.Families == nil {
		m.Families = []string{}
	}
	m.Sequence = int(last.Int64) + 1
	m.MarkedTS = t.stamp()
	return m, t.put(ctx, table("marks"), m)
}

// NewestMark reads the newest mark on the item under key, and false where
// none is.
func (t *Tx) NewestMark(ctx context.Context, key string) (Mark, bool, error) {
	return first[Mark](ctx, t.tx, table("marks"), `item_key = ? ORDER BY sequence DESC`, key)
}

// GateTrust reads family's standing as this write sees it, and false where
// the store holds none.
func (t *Tx) GateTrust(ctx context.Context, family string) (GateTrust, bool, error) {
	return first[GateTrust](ctx, t.tx, table("gate_trust"), `family = ?`, family)
}

// PutGateTrust writes one family's standing, keeping when it was created.
func (t *Tx) PutGateTrust(ctx context.Context, g GateTrust) error {
	g.CreatedTS, g.UpdatedTS = t.stamp(), t.stamp()
	return t.put(ctx, table("gate_trust"), g)
}

// GateTrusts reads every family's standing, by connector and then prompt
// version.
func (s *Store) GateTrusts(ctx context.Context) ([]GateTrust, error) {
	return readInto[GateTrust](ctx, s.read, table("gate_trust"), `1 ORDER BY connector, prompt_version`)
}

// GateTrustDecision is one person's word that counts on a family's ladder, as
// gate_trust_decisions holds it.
type GateTrustDecision struct {
	Family   string `json:"family"`
	Sequence int    `json:"sequence"`
	ItemKey  string `json:"item_key"`
	// Kind is mark or placement.
	Kind string `json:"kind"`
	// Agreement is agreed or disagreed, as the word read against the gate's
	// answer that GateDecidedBy names.
	Agreement string `json:"agreement"`
	// Promoted says whether the family was promoted when the gate answered
	// the item.
	Promoted      bool    `json:"promoted"`
	DecidedBy     string  `json:"decided_by"`
	ClientID      *string `json:"client_id"`
	GateDecidedBy string  `json:"gate_decided_by"`
	DecidedTS     string  `json:"decided_ts"`
}

// AddGateTrustDecision appends d to its family's decisions, numbered after
// the family's last, and answers it as written.
func (t *Tx) AddGateTrustDecision(ctx context.Context, d GateTrustDecision) (GateTrustDecision, error) {
	var last sql.NullInt64
	if err := t.tx.QueryRowContext(ctx, `SELECT max(sequence) FROM gate_trust_decisions WHERE family = ?`, d.Family).Scan(&last); err != nil {
		return d, fmt.Errorf("number a decision of %s: %w", d.Family, err)
	}
	d.Sequence = int(last.Int64) + 1
	d.DecidedTS = t.stamp()
	return d, t.put(ctx, table("gate_trust_decisions"), d)
}

// GateTrustDecisions reads family's decisions as this write sees them, the
// oldest first.
func (t *Tx) GateTrustDecisions(ctx context.Context, family string) ([]GateTrustDecision, error) {
	return readInto[GateTrustDecision](ctx, t.tx, table("gate_trust_decisions"), `family = ? ORDER BY sequence`, family)
}

// GateTrusts reads every family's standing as this write sees it.
func (t *Tx) GateTrusts(ctx context.Context) ([]GateTrust, error) {
	return readInto[GateTrust](ctx, t.tx, table("gate_trust"), `1 ORDER BY connector, prompt_version`)
}
