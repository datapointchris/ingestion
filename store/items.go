package store

import (
	"context"
	"fmt"
)

// ItemRecords is one item with every stage record written about it.
type ItemRecords struct {
	Item       Item
	Sightings  []Sighting
	Outputs    []Output
	Decisions  []Decision
	Deliveries []Delivery
	Tasks      []Task
}

// Item reads the item under key with its records, or ErrNoItem.
func (s *Store) Item(ctx context.Context, key string) (ItemRecords, error) {
	items, err := readInto[Item](ctx, s.read, table("items"), `key = ?`, key)
	if err != nil {
		return ItemRecords{}, err
	}
	if len(items) == 0 {
		return ItemRecords{}, fmt.Errorf("%s: %w", key, ErrNoItem)
	}
	r := ItemRecords{Item: items[0]}
	if r.Sightings, err = readInto[Sighting](ctx, s.read, table("sightings"), `item_key = ? ORDER BY first_seen_ts, rowid`, key); err != nil {
		return r, err
	}
	if r.Outputs, err = readInto[Output](ctx, s.read, table("outputs"), `item_key = ? ORDER BY created_ts, rowid`, key); err != nil {
		return r, err
	}
	if r.Decisions, err = readInto[Decision](ctx, s.read, table("decisions"), `item_key = ? ORDER BY gate, sequence`, key); err != nil {
		return r, err
	}
	if r.Deliveries, err = readInto[Delivery](ctx, s.read, table("deliveries"), `item_key = ? ORDER BY destination`, key); err != nil {
		return r, err
	}
	if r.Tasks, err = readInto[Task](ctx, s.read, table("tasks"), `item_key = ? ORDER BY created_ts, rowid`, key); err != nil {
		return r, err
	}
	return r, nil
}

// Condition statuses, as Kubernetes writes them.
const (
	ConditionTrue    = "True"
	ConditionFalse   = "False"
	ConditionUnknown = "Unknown"
)

// Condition is how far one stage or gate has gone with an item.
type Condition struct {
	// Type is a stage or a gate.
	Type   string `json:"type"`
	Status string `json:"status"`
	// Reason is a decision's verdict, an output's or a delivery's state.
	Reason string `json:"reason"`
}

// line is every stage and gate, in the order that an item passes them.
var line = []string{StageLand, StageParse, StageScan, StageQuality, GateSide, GateRead, StageModelRead, StageDeliver}

// Conditions derives each stage's and gate's condition from which records
// exist, as Kubernetes derives a pod's status from its conditions. A stage
// with no record is left out. So when a push sends an item past the gates, it
// shows no gate at all rather than one that failed.
func (r ItemRecords) Conditions() []Condition {
	var out []Condition
	for _, step := range line {
		if c, ok := r.condition(step); ok {
			out = append(out, c)
		}
	}
	return out
}

// reached names the phase that each stage or gate's True condition reaches,
// in the words of schemas/items-list.schema.json.
var reached = map[string]string{
	StageLand: "landed", StageParse: "parsed", StageScan: "scanned", StageQuality: "gated",
	GateSide: "placed", GateRead: "read", StageModelRead: "read", StageDeliver: "delivered",
}

// Phase is the item's phase in the words of schemas/items-list.schema.json:
// failed where a task failed, dropped or review where the newest gate decision
// says so, waiting where a model stage holds it, and otherwise the furthest
// stage whose condition is True.
func (r ItemRecords) Phase() string {
	for _, t := range r.Tasks {
		if t.State == TaskFailed {
			return "failed"
		}
	}
	quality, decided := r.newestDecision(GateQuality)
	if decided && quality.Verdict == "drop" {
		return "dropped"
	}
	side, placed := r.newestDecision(GateSide)
	if (decided && quality.Verdict == "review" && !placed) || (placed && side.Verdict == "review") {
		return "review"
	}
	phase := ""
	for _, c := range r.Conditions() {
		switch c.Status {
		case ConditionUnknown:
			if c.Reason == OutputWaiting {
				return "waiting"
			}
		case ConditionFalse:
			return "failed"
		case ConditionTrue:
			phase = reached[c.Type]
		}
	}
	return phase
}

// WaitingAt is the model stage that holds the item, and "" where none does.
func (r ItemRecords) WaitingAt() string {
	for _, c := range r.Conditions() {
		if c.Status == ConditionUnknown && c.Reason == OutputWaiting {
			return c.Type
		}
	}
	return ""
}

func (r ItemRecords) condition(step string) (Condition, bool) {
	switch step {
	case StageLand:
		return Condition{Type: step, Status: ConditionTrue, Reason: "landed"}, true
	case StageQuality:
		if d, ok := r.newestDecision(GateQuality); ok {
			return Condition{Type: step, Status: ConditionTrue, Reason: d.Verdict}, true
		}
		return r.outputCondition(step)
	case GateSide, GateRead:
		if d, ok := r.newestDecision(step); ok {
			return Condition{Type: step, Status: ConditionTrue, Reason: d.Verdict}, true
		}
		return Condition{}, false
	case StageDeliver:
		return r.deliveryCondition()
	default:
		return r.outputCondition(step)
	}
}

func (r ItemRecords) newestDecision(gate string) (Decision, bool) {
	var newest Decision
	found := false
	for _, d := range r.Decisions {
		if d.Gate == gate && d.Sequence >= newest.Sequence {
			newest, found = d, true
		}
	}
	return newest, found
}

// outputCondition is True where any producer's output for stage is done,
// Unknown where one waits, and False where every one failed.
func (r ItemRecords) outputCondition(stage string) (Condition, bool) {
	states := map[string]bool{}
	for _, o := range r.Outputs {
		if o.Stage == stage {
			states[o.State] = true
		}
	}
	switch {
	case states[OutputDone]:
		return Condition{Type: stage, Status: ConditionTrue, Reason: OutputDone}, true
	case states[OutputWaiting]:
		return Condition{Type: stage, Status: ConditionUnknown, Reason: OutputWaiting}, true
	case states[OutputFailed]:
		return Condition{Type: stage, Status: ConditionFalse, Reason: OutputFailed}, true
	}
	return Condition{}, false
}

// deliveryCondition is True where any destination holds the item, Unknown
// where one is pending, and False where every one refused it.
func (r ItemRecords) deliveryCondition() (Condition, bool) {
	outcomes := map[string]bool{}
	for _, d := range r.Deliveries {
		outcomes[d.Outcome] = true
	}
	switch {
	case outcomes[DeliveryDelivered]:
		return Condition{Type: StageDeliver, Status: ConditionTrue, Reason: DeliveryDelivered}, true
	case outcomes[DeliveryDeliveredToExisting]:
		return Condition{Type: StageDeliver, Status: ConditionTrue, Reason: DeliveryDeliveredToExisting}, true
	case outcomes[DeliveryPending]:
		return Condition{Type: StageDeliver, Status: ConditionUnknown, Reason: DeliveryPending}, true
	case outcomes[DeliveryRefused]:
		return Condition{Type: StageDeliver, Status: ConditionFalse, Reason: DeliveryRefused}, true
	}
	return Condition{}, false
}
