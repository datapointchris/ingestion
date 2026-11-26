package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
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
	// Sections are the item's sections, each producer version's in order.
	Sections []Section

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
	records, err := recordsOf(ctx, s.read, items)
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

// ItemsWithNoSourceTime reads up to limit items that hold bytes and no source
// time, of one of mediaTypes, whose key sorts after after, in key order.
func (s *Store) ItemsWithNoSourceTime(ctx context.Context, mediaTypes []string, after string, limit int) ([]Item, error) {
	if len(mediaTypes) == 0 {
		return nil, nil
	}
	args := []any{after}
	for _, m := range mediaTypes {
		args = append(args, m)
	}
	args = append(args, limit)
	where := `key > ? AND source_ts IS NULL AND blob_sha256 IS NOT NULL AND media_type IN (?` +
		strings.Repeat(`, ?`, len(mediaTypes)-1) + `) ORDER BY key LIMIT ?`
	return readInto[Item](ctx, s.read, table("items"), where, args...)
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
	records, err := recordsOf(ctx, s.read, items)
	return records, next, err
}

// recordsOf reads every record of items, one query per table. A table read
// here joins to an item by item_key or by the item's blob_sha256, which
// itemColumns also follows, so a write to it settles the item's phase.
func recordsOf(ctx context.Context, db querier, items []Item) ([]ItemRecords, error) {
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
	sightings, err := readInto[Sighting](ctx, db, table("sightings"), in("item_key", len(keys))+` ORDER BY first_seen_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range sightings {
		out[at[r.ItemKey]].Sightings = append(out[at[r.ItemKey]].Sightings, r)
	}
	outputs, err := readInto[Output](ctx, db, table("outputs"), in("item_key", len(keys))+` ORDER BY created_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range outputs {
		out[at[r.ItemKey]].Outputs = append(out[at[r.ItemKey]].Outputs, r)
	}
	decisions, err := readInto[Decision](ctx, db, table("decisions"), in("item_key", len(keys))+` ORDER BY gate, sequence`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range decisions {
		out[at[r.ItemKey]].Decisions = append(out[at[r.ItemKey]].Decisions, r)
	}
	deliveries, err := readInto[Delivery](ctx, db, table("deliveries"), in("item_key", len(keys))+` ORDER BY destination`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range deliveries {
		out[at[r.ItemKey]].Deliveries = append(out[at[r.ItemKey]].Deliveries, r)
	}
	tasks, err := readInto[Task](ctx, db, table("tasks"), in("item_key", len(keys))+` ORDER BY created_ts, rowid`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range tasks {
		out[at[*r.ItemKey]].Tasks = append(out[at[*r.ItemKey]].Tasks, r)
	}
	sections, err := readInto[Section](ctx, db, table("sections"), in("item_key", len(keys))+` ORDER BY created_ts, producer, producer_version, n`, keys...)
	if err != nil {
		return nil, err
	}
	for _, r := range sections {
		out[at[r.ItemKey]].Sections = append(out[at[r.ItemKey]].Sections, r)
	}
	counts, err := unitCounts(ctx, db, keys)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].unitCounts = counts
	}
	if len(sums) == 0 {
		return out, nil
	}
	blobs, err := readInto[Blob](ctx, db, table("blobs"), in("sha256", len(sums)), sums...)
	if err != nil {
		return nil, err
	}
	homes, err := readInto[Home](ctx, db, table("homes"), in("blob_sha256", len(sums))+` ORDER BY found_ts, rowid`, sums...)
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
var line = []string{
	StageLand, StageParse, StageStructure, StageContents, StageSections, StageScan, StageQuality, GateSide, GateRead, StageModelRead,
	StageDeliver,
}

// Precedes says whether step comes before later on the line.
func Precedes(step, later string) bool {
	i, j := slices.Index(line, step), slices.Index(line, later)
	return i >= 0 && j >= 0 && i < j
}

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
	StageLand:      "landed",
	StageParse:     "parsed",
	StageStructure: "structured",
	StageContents:  "structured",
	StageSections:  "sectioned",
	StageScan:      "scanned",
	StageQuality:   "gated",
	GateSide:       "placed",
	GateRead:       "read",
	StageModelRead: "read",
	StageDeliver:   "delivered",
}

// Phase is the item's phase in the words of schemas/items-list.schema.json:
// dropped where the newest side decision drops it, review where WaitsOnPerson
// says it waits, failed where another stage on the line holds a newest task
// that failed, dropped where the quality gate drops it before any side
// decision, waiting where a model stage holds it, and otherwise the furthest
// stage whose condition is True. A person can place an
// item whose parse or quality gate failed, which moves it on from that stage.
// A failed structure profile or contents reading fails no phase.
func (r ItemRecords) Phase() string {
	side, sided := r.NewestDecision(GateSide)
	if sided && side.Verdict == "drop" {
		return "dropped"
	}
	if _, _, waits := r.WaitsOnPerson(); waits {
		return "review"
	}
	newest := map[string]Task{}
	for _, t := range r.Tasks {
		newest[t.Stage] = t
	}
	for stage, t := range newest {
		if t.State == TaskFailed && slices.Contains(line, stage) && stage != StageParse && stage != StageStructure && stage != StageContents &&
			(stage != StageQuality || !r.PersonPlaced()) {
			return "failed"
		}
	}
	if quality, decided := r.NewestDecision(GateQuality); decided && !sided && quality.Verdict == "drop" {
		return "dropped"
	}
	phase := ""
	for _, c := range r.Conditions() {
		switch c.Status {
		case ConditionUnknown:
			if c.Reason == OutputWaiting {
				return "waiting"
			}
		case ConditionFalse:
			if c.Type == StageParse && sided {
				continue
			}
			return "failed"
		case ConditionTrue:
			phase = reached[c.Type]
		}
	}
	return phase
}

// PersonPlaced says whether a person made the item's newest side decision.
func (r ItemRecords) PersonPlaced() bool {
	side, ok := r.NewestDecision(GateSide)
	return ok && side.ByPerson()
}

// ReviewPlacement is the item's label, as ReviewPlacement decides it from the
// item's decisions.
func (r ItemRecords) ReviewPlacement() (Decision, bool) { return ReviewPlacement(r.Decisions) }

// Side is the newest side decision's verdict where it placed the item on a
// side, and false where none has.
func (r ItemRecords) Side() (string, bool) {
	d, ok := r.NewestDecision(GateSide)
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
		if d, ok := r.NewestDecision(GateQuality); ok {
			return Condition{Type: step, Status: ConditionTrue, Reason: d.Verdict}, true
		}
		if r.PersonPlaced() {
			// A run of the gate on an item that a person placed measures the
			// gate against the placement, so it holds and fails nothing.
			return Condition{}, false
		}
		return r.outputCondition(step)
	case GateSide, GateRead:
		if d, ok := r.NewestDecision(step); ok {
			return Condition{Type: step, Status: ConditionTrue, Reason: d.Verdict}, true
		}
		return Condition{}, false
	case StageStructure, StageContents:
		// A PDF's profile and its contents reading run beside its scan, so
		// one that failed costs the book its sections and holds back no
		// other stage.
		if c, ok := r.outputCondition(step); ok && c.Status != ConditionFalse {
			return c, true
		}
		return Condition{}, false
	case StageSections:
		if len(r.Sections) > 0 {
			return Condition{Type: step, Status: ConditionTrue, Reason: r.Sections[0].Producer}, true
		}
		return Condition{}, false
	case StageDeliver:
		return r.deliveryCondition()
	default:
		return r.outputCondition(step)
	}
}

// NewestDecision is the item's newest decision at gate, and false where none
// is.
func (r ItemRecords) NewestDecision(gate string) (Decision, bool) {
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
// Unknown where one waits or a model stage's task is queued or leased, and
// False where every one failed. A model stage's open task holds the item at
// that stage, whether a first fill holds it, no worker claims it, or its
// dispatch has not answered. A model stage's waiting output holds it only
// while that task is open, because a placement that withdraws the task leaves
// the output as it was.
func (r ItemRecords) outputCondition(stage string) (Condition, bool) {
	model := slices.Contains(modelStages, stage)
	open := model && slices.ContainsFunc(r.Tasks, func(t Task) bool {
		return t.Stage == stage && (t.State == TaskQueued || t.State == TaskLeased)
	})
	states := map[string]bool{OutputWaiting: open}
	for _, o := range r.Outputs {
		if o.Stage == stage && (o.State != OutputWaiting || !model || open) {
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
