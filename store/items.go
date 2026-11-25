package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// ItemRecords is one item with every stage record written about it.
type ItemRecords struct {
	Item Item
	// Blob is the row of the item's bytes, and nil where it holds none.
	Blob       *Blob
	Homes      []Home
	Sightings  []Sighting
	Outputs    []Output
	Decisions  []Decision
	Deliveries []Delivery
	Tasks      []Task

	unitCounts map[parseVersion]int
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
	records, err := s.recordsOf(ctx, items)
	if err != nil {
		return ItemRecords{}, err
	}
	return records[0], nil
}

// ItemRow reads the item row under key alone, and false where the store holds
// none.
func (s *Store) ItemRow(ctx context.Context, key string) (Item, bool, error) {
	return first[Item](ctx, s.read, table("items"), `key = ?`, key)
}

// ItemPage asks for up to Limit items, newest first, after the item that the
// cursor After names.
type ItemPage struct {
	Limit int
	After string
}

// ErrBadCursor refuses a page cursor that Items did not write.
var ErrBadCursor = errors.New("not a cursor this list wrote")

// Items reads a page of items, newest first, each with its records, and the
// cursor of the next page, which is "" where none is left.
func (s *Store) Items(ctx context.Context, page ItemPage) ([]ItemRecords, string, error) {
	where, args := `1`, []any{}
	if page.After != "" {
		raw, err := base64.RawURLEncoding.DecodeString(page.After)
		created, key, ok := strings.Cut(string(raw), "\x00")
		if err != nil || !ok {
			return nil, "", ErrBadCursor
		}
		where, args = `(created_ts < ? OR (created_ts = ? AND key < ?))`, []any{created, created, key}
	}
	items, err := readInto[Item](ctx, s.read, table("items"), where+` ORDER BY created_ts DESC, key DESC LIMIT ?`,
		append(args, page.Limit+1)...)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		last := items[len(items)-1]
		next = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedTS + "\x00" + last.Key))
	}
	records, err := s.recordsOf(ctx, items)
	return records, next, err
}

// recordsOf reads every record of items, one query per table.
func (s *Store) recordsOf(ctx context.Context, items []Item) ([]ItemRecords, error) {
	out := make([]ItemRecords, len(items))
	if len(items) == 0 {
		return out, nil
	}
	at := map[string]int{}
	keys := make([]any, len(items))
	var sums []any
	for i, item := range items {
		out[i].Item = item
		at[item.Key] = i
		keys[i] = item.Key
		if item.BlobSHA256 != nil {
			sums = append(sums, *item.BlobSHA256)
		}
	}
	in := func(column string, n int) string {
		return column + ` IN (?` + strings.Repeat(", ?", n-1) + `)`
	}
	sightings, err := readInto[Sighting](ctx, s.read, table("sightings"), in("item_key", len(keys))+` ORDER BY first_seen_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range sightings {
		out[at[r.ItemKey]].Sightings = append(out[at[r.ItemKey]].Sightings, r)
	}
	outputs, err := readInto[Output](ctx, s.read, table("outputs"), in("item_key", len(keys))+` ORDER BY created_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range outputs {
		out[at[r.ItemKey]].Outputs = append(out[at[r.ItemKey]].Outputs, r)
	}
	decisions, err := readInto[Decision](ctx, s.read, table("decisions"), in("item_key", len(keys))+` ORDER BY gate, sequence`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range decisions {
		out[at[r.ItemKey]].Decisions = append(out[at[r.ItemKey]].Decisions, r)
	}
	deliveries, err := readInto[Delivery](ctx, s.read, table("deliveries"), in("item_key", len(keys))+` ORDER BY destination`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range deliveries {
		out[at[r.ItemKey]].Deliveries = append(out[at[r.ItemKey]].Deliveries, r)
	}
	tasks, err := readInto[Task](ctx, s.read, table("tasks"), in("item_key", len(keys))+` ORDER BY created_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range tasks {
		out[at[r.ItemKey]].Tasks = append(out[at[r.ItemKey]].Tasks, r)
	}
	counts, err := s.unitCounts(ctx, keys)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].unitCounts = counts
	}
	if len(sums) == 0 {
		return out, nil
	}
	blobs, err := readInto[Blob](ctx, s.read, table("blobs"), in("sha256", len(sums)), sums...)
	if err != nil {
		return nil, err
	}
	homes, err := readInto[Home](ctx, s.read, table("homes"), in("blob_sha256", len(sums))+` ORDER BY found_ts, rowid`, sums...)
	if err != nil {
		return nil, err
	}
	for i := range out {
		sum := out[i].Item.BlobSHA256
		if sum == nil {
			continue
		}
		for _, b := range blobs {
			if b.SHA256 == *sum {
				out[i].Blob = &b
			}
		}
		for _, h := range homes {
			if h.BlobSHA256 == *sum {
				out[i].Homes = append(out[i].Homes, h)
			}
		}
	}
	return out, nil
}

// Blob reads the blob row for sum, or ErrNoBlob.
func (s *Store) Blob(ctx context.Context, sum string) (Blob, error) {
	b, ok, err := first[Blob](ctx, s.read, table("blobs"), `sha256 = ?`, sum)
	if err == nil && !ok {
		err = fmt.Errorf("%s: %w", sum, ErrNoBlob)
	}
	return b, err
}

// ErrNoBlob is a read naming bytes with no blob row.
var ErrNoBlob = errors.New("no such blob")

// Blobs reads every blob row.
func (s *Store) Blobs(ctx context.Context) ([]Blob, error) {
	return readInto[Blob](ctx, s.read, table("blobs"), `1 ORDER BY sha256`)
}

// LiveHomes reads every home of the blob sum that is not gone.
func (s *Store) LiveHomes(ctx context.Context, sum string) ([]Home, error) {
	return readInto[Home](ctx, s.read, table("homes"), `blob_sha256 = ? AND gone_ts IS NULL ORDER BY found_ts, rowid`, sum)
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
// review where no parse is usable, failed where another stage's task failed,
// dropped or review where the newest gate decision says so, waiting where a
// model stage holds it, and otherwise the furthest stage whose condition is
// True.
func (r ItemRecords) Phase() string {
	if r.NoUsableParse() {
		return "review"
	}
	for _, t := range r.Tasks {
		if t.State == TaskFailed && t.Stage != StageParse {
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

// Side is the newest side decision's verdict where it placed the item on a
// side, and false where none has.
func (r ItemRecords) Side() (string, bool) {
	d, ok := r.newestDecision(GateSide)
	if !ok || (d.Verdict != "personal" && d.Verdict != "fleet") {
		return "", false
	}
	return d.Verdict, true
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
