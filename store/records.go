package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
)

// Each record's JSON names are its table's column names, which
// schemas/ingestion-record.schema.json and schemas/ingestion-text.schema.json
// publish together. A nil pointer is a NULL.

// Zone says who wrote an item's content.
type Zone string

const (
	// Outside is outsider-written content, even where the owner shared it.
	Outside Zone = "outside"
	// Owner is the owner's own words, such as a voice memo or typed text.
	Owner Zone = "owner"
)

// Stricter is the stricter of a and b. Outside is stricter than owner.
func Stricter(a, b Zone) Zone {
	if a == Outside || b == Outside {
		return Outside
	}
	return Owner
}

// Stages, in the order that an item passes them, after the poll that fetches
// it. A task or an output names one.
const (
	StagePoll      = "poll"
	StageLand      = "land"
	StageParse     = "parse"
	StageScan      = "scan"
	StageQuality   = "quality"
	StageModelRead = "model-read"
	StageDeliver   = "deliver"
)

// Gates, each deciding by an appended decision.
const (
	GateQuality = "quality"
	GateSide    = "side"
	GateRead    = "read"
)

// Item is one artifact.
type Item struct {
	Key          string  `json:"key"`
	MediaType    string  `json:"media_type"`
	Zone         Zone    `json:"zone"`
	BlobSHA256   *string `json:"blob_sha256"`
	ParentKey    *string `json:"parent_key"`
	FlowID       string  `json:"flow_id"`
	ParentFlowID *string `json:"parent_flow_id"`
	Title        *string `json:"title"`
	URL          *string `json:"url"`
	// Phase is what the write's settle decides before it commits, replacing
	// any value that a put carried. A put of an item with no phase leaves the
	// column as it was.
	Phase     *string `json:"phase,omitempty"`
	CreatedTS string  `json:"created_ts"`
	UpdatedTS string  `json:"updated_ts"`
}

// Sighting is one connector's arrival of an item.
type Sighting struct {
	ItemKey   string   `json:"item_key"`
	Connector string   `json:"connector"`
	Tags      []string `json:"tags"`
	// Fields hold the source payload's values by name, such as a Reddit
	// save's subreddit, for a routing rule to match.
	Fields         map[string]string `json:"fields"`
	Zone           Zone              `json:"zone"`
	CursorPosition *string           `json:"cursor_position"`
	ExternalKey    string            `json:"external_key"`
	FirstSeenTS    string            `json:"first_seen_ts"`
	LastSeenTS     string            `json:"last_seen_ts"`
}

// Blob is landed bytes, written once.
type Blob struct {
	SHA256    string `json:"sha256"`
	MD5       string `json:"md5"`
	SizeBytes int64  `json:"size_bytes"`
	MediaType string `json:"media_type"`
	CreatedTS string `json:"created_ts"`
}

// Home names where a file already lives, under a named root.
type Home struct {
	BlobSHA256 string `json:"blob_sha256"`
	Root       string `json:"root"`
	PathSHA256 string `json:"path_sha256"`
	Path       string `json:"path"`
	// ModifiedTS is the file's modification time, to the nanosecond, when its
	// bytes were last hashed at Path. A file whose size or modification time
	// differs is hashed again before it is read as the blob.
	ModifiedTS *string `json:"modified_ts"`
	FoundTS    string  `json:"found_ts"`
	GoneTS     *string `json:"gone_ts"`
}

// Tokens is a model run's tokens by kind.
type Tokens struct {
	UncachedInput int64 `json:"uncached_input"`
	CacheWrite    int64 `json:"cache_write"`
	CacheRead     int64 `json:"cache_read"`
	Output        int64 `json:"output"`
}

// Finding is one scanner rule's match, without its span: where it starts and
// how many bytes it covers. Unit names the parse's unit it starts in, and its
// offset counts from that unit's start. A finding from Scanner.Scan names no
// unit.
type Finding struct {
	Rule     string `json:"rule"`
	Severity string `json:"severity"`
	Unit     *int   `json:"unit"`
	Offset   int    `json:"offset"`
	Length   int    `json:"length"`
}

// Unit is one part of a parse, such as a post or a page, numbered from 1.
type Unit struct {
	N     int     `json:"n"`
	Label *string `json:"label"`
	Text  string  `json:"text"`
}

// ParseUnit is one unit as parse_units holds it.
type ParseUnit struct {
	ItemKey          string  `json:"item_key"`
	Extractor        string  `json:"extractor"`
	ExtractorVersion string  `json:"extractor_version"`
	N                int     `json:"n"`
	Label            *string `json:"label"`
	Text             string  `json:"text"`
	CreatedTS        string  `json:"created_ts"`
}

// Replay is one replay that queued a stage again over the items its connector
// and key select.
type Replay struct {
	// ID derives from the caller and its Idempotency-Key.
	ID        string  `json:"id"`
	Stage     string  `json:"stage"`
	Connector *string `json:"connector"`
	ItemKey   *string `json:"item_key"`
	// Items counts the tasks queued, one per item.
	Items       int    `json:"items"`
	RequestedBy string `json:"requested_by"`
	RequestedTS string `json:"requested_ts"`
}

// Span is the text one finding matched. An output's spans hold one per
// finding, in the findings' order.
type Span struct {
	Rule string `json:"rule"`
	Span string `json:"span"`
}

// Output states for a stage's output.
const (
	OutputWaiting = "waiting"
	OutputDone    = "done"
	OutputFailed  = "failed"
)

// Output is one stage's output for one item from one producer at one version.
type Output struct {
	ItemKey         string  `json:"item_key"`
	Stage           string  `json:"stage"`
	Producer        string  `json:"producer"`
	ProducerVersion string  `json:"producer_version"`
	Lens            *string `json:"lens"`
	LensVersion     *string `json:"lens_version"`
	// InputProducer and InputVersion name the parse a later stage read, such
	// as the parse a scan's findings index the units of.
	InputProducer *string `json:"input_producer"`
	InputVersion  *string `json:"input_version"`
	State         string  `json:"state"`
	BlobSHA256    *string `json:"blob_sha256"`
	DispatchID    *string `json:"dispatch_id"`
	RunID         *string `json:"run_id"`
	// Model and Effort are what a model stage asked its run for, and
	// AnsweredModel the model that its run reports answering on.
	Model          *string         `json:"model"`
	Effort         *string         `json:"effort"`
	AnsweredModel  *string         `json:"answered_model"`
	Tokens         *Tokens         `json:"tokens"`
	Findings       []Finding       `json:"findings"`
	Quality        *int            `json:"quality"`
	SideGuess      *string         `json:"side_guess"`
	SideConfidence *float64        `json:"side_confidence"`
	Spans          []Span          `json:"spans"`
	Answer         json.RawMessage `json:"answer"`
	Error          *string         `json:"error"`
	CreatedTS      string          `json:"created_ts"`
	UpdatedTS      string          `json:"updated_ts"`
}

// NewestDone is the newest done output of stage among outputs, and nil where
// none is. A model stage's newest done output is its answer.
func NewestDone(outputs []Output, stage string) *Output {
	var newest *Output
	for i := range outputs {
		o := &outputs[i]
		if o.Stage == stage && o.State == OutputDone && (newest == nil || o.UpdatedTS >= newest.UpdatedTS) {
			newest = o
		}
	}
	return newest
}

// Run names the run that wrote o: its run id, else its producer and version.
func (o Output) Run() string {
	if o.RunID != nil {
		return *o.RunID
	}
	return o.Producer + "/" + o.ProducerVersion
}

// Decision is one gate's decision on one item. The newest at a gate decides.
type Decision struct {
	ItemKey     string   `json:"item_key"`
	Gate        string   `json:"gate"`
	Sequence    int      `json:"sequence"`
	Verdict     string   `json:"verdict"`
	DecidedBy   string   `json:"decided_by"`
	Confidence  *float64 `json:"confidence"`
	Cause       string   `json:"cause"`
	Destination *string  `json:"destination"`
	Note        *string  `json:"note"`
	// Lens is set on a read decision whose verdict is model.
	Lens      *string `json:"lens"`
	ClientID  *string `json:"client_id"`
	DecidedTS string  `json:"decided_ts"`
}

// Delivery outcomes.
const (
	DeliveryPending             = "pending"
	DeliveryDelivered           = "delivered"
	DeliveryDeliveredToExisting = "delivered-to-existing"
	DeliveryRefused             = "refused"
)

// Delivery is one item's delivery to one destination.
type Delivery struct {
	ItemKey       string  `json:"item_key"`
	Destination   string  `json:"destination"`
	DestinationID *string `json:"destination_id"`
	Outcome       string  `json:"outcome"`
	Attempts      int     `json:"attempts"`
	LastError     *string `json:"last_error"`
	// Title and URL are the item's when the delivery was first queued, which
	// every attempt sends.
	Title     *string `json:"title"`
	URL       *string `json:"url"`
	UpdatedTS string  `json:"updated_ts"`
}

// A connector's first fill: none until an item reaches a model stage, held
// once one has, and released once a person lets the rest through.
const (
	FirstFillNone     = "none"
	FirstFillHeld     = "held"
	FirstFillReleased = "released"
)

// Cursor is one connector's position and when it is next due.
type Cursor struct {
	Connector string  `json:"connector"`
	Position  *string `json:"position"`
	DueAt     string  `json:"due_at"`
	// StaleAt is written by the connectors package, which holds the freshness.
	StaleAt          *string `json:"stale_at"`
	LastRunTS        *string `json:"last_run_ts"`
	LastDeliveryTS   *string `json:"last_delivery_ts"`
	LastError        *string `json:"last_error"`
	LastErrorTS      *string `json:"last_error_ts"`
	Failures         int     `json:"failures"`
	FirstFill        string  `json:"first_fill"`
	FirstFillItemKey *string `json:"first_fill_item_key"`
	CreatedTS        string  `json:"created_ts"`
	UpdatedTS        string  `json:"updated_ts"`
}

// Declarations is one applied set of connectors, routing rules and
// destinations, each declaration as its JSON.
type Declarations struct {
	Commit       string            `json:"commit"`
	AppliedTS    string            `json:"applied_ts"`
	AppliedBy    string            `json:"applied_by"`
	Connectors   []json.RawMessage `json:"connectors"`
	RoutingRules []json.RawMessage `json:"routing_rules"`
	Destinations []json.RawMessage `json:"destinations"`
}

// KeyedRequest is one request a caller sent under an Idempotency-Key, and
// what it answered.
type KeyedRequest struct {
	Caller         string          `json:"caller"`
	IdempotencyKey string          `json:"idempotency_key"`
	Route          string          `json:"route"`
	Connector      *string         `json:"connector"`
	Fingerprint    string          `json:"fingerprint"`
	Status         int             `json:"status"`
	Answer         json.RawMessage `json:"answer"`
	CreatedTS      string          `json:"created_ts"`
}

// NewFlowID is a fresh flow id, as 32 hex characters.
func NewFlowID() string { return randomHex(16) }

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// table is the declaration of the table called name, which Tables holds.
func table(name string) Table {
	t, ok := TableNamed(name)
	if !ok {
		panic("store: no table " + name)
	}
	return t
}
