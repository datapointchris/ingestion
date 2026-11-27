package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
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
	StagePoll  = "poll"
	StageLand  = "land"
	StageParse = "parse"
	// StageStructure profiles a PDF's bytes: its outline, page labels,
	// creation date and fonts.
	StageStructure = "structure"
	// StageContents is a model's reading of the printed contents pages of a
	// PDF whose outline gives it no sections.
	StageContents = "contents"
	// StageSections is the write of an item's sections that serve makes from
	// its structure or its contents, which queues no task.
	StageSections  = "sections"
	StageScan      = "scan"
	StageQuality   = "quality"
	StageModelRead = "model-read"
	StageDeliver   = "deliver"
)

// StageDelete removes a landed item from its connector's source, such as a
// save from the playlist it was captured in, where the connector declares
// after_landing delete. It sits on no item's line, so its tasks move no
// item's phase.
const StageDelete = "delete"

// StageRefreshMetrics reads again, at a source, the metrics of each item that
// the source's connectors sighted and that waits in review, and writes them
// onto those sightings. Its task names its source rather than an item, so it
// sits on no item's line and moves no item's phase.
const StageRefreshMetrics = "refresh-metrics"

// StageQualityArm is the quality gate's prompt answered by a model that its
// task names, as a measurement against each label beside the gate's own
// answer. It sits on no item's line, so its tasks and outputs move no item's
// phase, decision or delivery.
const StageQualityArm = "quality-arm"

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
	// SourceTS is when the artifact was made at its source, such as a Reddit
	// post's creation, as a store stamp, and nil where the source gives none.
	SourceTS *string `json:"source_ts"`
	// Phase is what the write's settle decides before it commits, replacing
	// any value that a put carried. A put of an item with no phase leaves the
	// column as it was.
	Phase     *string `json:"phase,omitempty"`
	CreatedTS string  `json:"created_ts"`
	UpdatedTS string  `json:"updated_ts"`
}

// MediaTypeChapter is a book's chapter: a child item that holds no bytes of
// its own and takes its parent's side and read decisions.
const MediaTypeChapter = "application/x-book-chapter"

// PlacedThroughParent says whether the item takes every side and read
// decision from its parent, as a book's chapter does. A person places its
// parent instead, so neither the review queue nor a placement takes it.
func (i Item) PlacedThroughParent() bool {
	return i.MediaType == MediaTypeChapter && i.ParentKey != nil
}

// Sighting is one connector's arrival of an item.
type Sighting struct {
	ItemKey   string   `json:"item_key"`
	Connector string   `json:"connector"`
	Tags      []string `json:"tags"`
	// Fields hold the source payload's values by name, such as a Reddit
	// save's subreddit, for a routing rule to match.
	Fields map[string]string `json:"fields"`
	// Metrics hold the source's counts by name, such as a video's views and
	// duration. No outsider wrote them, so they ship on the record channel.
	Metrics        map[string]int64 `json:"metrics"`
	Zone           Zone             `json:"zone"`
	CursorPosition *string          `json:"cursor_position"`
	ExternalKey    string           `json:"external_key"`
	FirstSeenTS    string           `json:"first_seen_ts"`
	// LastSeenTS is the last arrival that changed the sighting.
	LastSeenTS string `json:"last_seen_ts"`
	// WrittenTS stamps every write of the row, an arrival's or a refresh's,
	// and is nil on a row written before the store kept it.
	WrittenTS *string `json:"written_ts"`
	// MetricsReadTS is when the counts were last taken from the source: by
	// an arrival that changed them, or by a refresh that read them, changed or
	// not. It is nil where no read has taken any.
	MetricsReadTS *string `json:"metrics_read_ts"`
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

// PlacedByPerson is the placed_by of a replay that runs a stage only over the
// labeled items, each one that a person placed from review.
const PlacedByPerson = "person"

// Replay is one replay that queued a stage again over the items its connector
// and key select.
type Replay struct {
	// ID derives from the caller and its Idempotency-Key.
	ID        string  `json:"id"`
	Stage     string  `json:"stage"`
	Connector *string `json:"connector"`
	ItemKey   *string `json:"item_key"`
	// PlacedBy is PlacedByPerson where the replay ran only over the labeled
	// items, and nil where it ran over every item it selected.
	PlacedBy *string `json:"placed_by"`
	// Model is the model that a measurement arm's replay asked each task's
	// run for, and nil for every other replay.
	Model *string `json:"model"`
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
	// OutputDeferred is a parse whose extractor found no text of its own and
	// handed the item to another, such as a video with no caption track that
	// Whisper transcribes. The parse reads as Unknown until the other ends.
	OutputDeferred = "deferred"
)

// StageRun is how one producer's run of a stage over its subject went, as
// both a subject's tables hold it: an outputs row for an item, and a
// prompt_proposals row for a draft. The modelstage protocol reads and writes
// these columns alone, so a column it adds is added here once.
type StageRun struct {
	Producer        string `json:"producer"`
	ProducerVersion string `json:"producer_version"`
	// State is OutputWaiting, OutputDone or OutputFailed.
	State      string  `json:"state"`
	DispatchID *string `json:"dispatch_id"`
	RunID      *string `json:"run_id"`
	// Model and Effort are what a model stage asked its run for, and
	// AnsweredModel the model that its run reports answering on.
	Model         *string         `json:"model"`
	Effort        *string         `json:"effort"`
	AnsweredModel *string         `json:"answered_model"`
	Tokens        *Tokens         `json:"tokens"`
	Answer        json.RawMessage `json:"answer"`
	Error         *string         `json:"error"`
}

// Output is one stage's output for one item from one producer at one version.
type Output struct {
	ItemKey string `json:"item_key"`
	Stage   string `json:"stage"`
	StageRun
	Lens        *string `json:"lens"`
	LensVersion *string `json:"lens_version"`
	// NoteDigest is NoteDigest of the note that a model read answered.
	NoteDigest *string `json:"note_digest"`
	// InputProducer and InputVersion name the parse a later stage read, such
	// as the parse a scan's findings index the units of.
	InputProducer *string   `json:"input_producer"`
	InputVersion  *string   `json:"input_version"`
	BlobSHA256    *string   `json:"blob_sha256"`
	Findings      []Finding `json:"findings"`
	// RulesVersion is the scanner rules' version that made Findings.
	RulesVersion   *string  `json:"rules_version"`
	Quality        *int     `json:"quality"`
	SideGuess      *string  `json:"side_guess"`
	SideConfidence *float64 `json:"side_confidence"`
	Spans          []Span   `json:"spans"`
	CreatedTS      string   `json:"created_ts"`
	UpdatedTS      string   `json:"updated_ts"`
}

// NoteDigest is the hex SHA-256 of a read decision's note, trimmed, and nil
// where the note is empty.
func NoteDigest(note *string) *string {
	if note == nil || strings.TrimSpace(*note) == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(*note)))
	digest := hex.EncodeToString(sum[:])
	return &digest
}

// NoteAnswered says whether an output that answered the note under answered,
// a NoteDigest, answers note. An output answers a decision that carries no
// note whatever note it answered, so only a new note reads an item again.
func NoteAnswered(answered, note *string) bool {
	digest := NoteDigest(note)
	return digest == nil || (answered != nil && *answered == *digest)
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

// Each verdict beside a side: the quality gate's pass, and either gate's drop
// and review. An item dropped on a destination that takes no side keeps its
// row.
const (
	VerdictPass   = "pass"
	VerdictDrop   = "drop"
	VerdictReview = "review"
)

// Who decided, as decided_by spells it, beside the forms that the DecidedBy
// functions write.
const (
	DecidedByTag = "tag"
	// DecidedByGate is the side gate itself, where no rung placed the item.
	DecidedByGate = "gate"
)

const (
	personPrefix  = "person:"
	servicePrefix = "service:"
	rulePrefix    = "rule:"
	modelPrefix   = "model:"
	parentPrefix  = "parent:"
)

// DecidedByPerson is a person's decision, under their login.
func DecidedByPerson(login string) string { return personPrefix + login }

// DecidedByService is a service's word, under its client id, where the
// service names no person.
func DecidedByService(clientID string) string { return servicePrefix + clientID }

// DecidedByRule is a routing rule's decision, under its name.
func DecidedByRule(name string) string { return rulePrefix + name }

// DecidedByModel is a model's decision, under the run that answered.
func DecidedByModel(run string) string { return modelPrefix + run }

// DecidedByParent is a decision that an item took from its parent, under the
// parent's key.
func DecidedByParent(key string) string { return parentPrefix + key }

// ByPerson says whether a person made d, as its decided_by person:<login>
// records.
func (d Decision) ByPerson() bool { return strings.HasPrefix(d.DecidedBy, personPrefix) }

// ByParent says whether the item took d from its parent, as its decided_by
// parent:<key> records.
func (d Decision) ByParent() bool { return strings.HasPrefix(d.DecidedBy, parentPrefix) }

// ModelRun is the run that answered d, where a model made it, as its
// decided_by model:<run> records.
func (d Decision) ModelRun() (string, bool) { return strings.CutPrefix(d.DecidedBy, modelPrefix) }

// Provisional says whether a side decision waits on a later input: a review
// that the side gate wrote for want of a rule or a guess, or for an error,
// and a model's review or drop. A rule that a later set adds, or a later
// answer of the quality gate, then decides the item again. Every other side
// decision, by a tag, a rule, a person, a parent or a model's confident
// guess, is settled, and no later answer of the quality gate moves it.
func (d Decision) Provisional() bool {
	_, byModel := d.ModelRun()
	return (d.Verdict == VerdictReview && d.DecidedBy == DecidedByGate) ||
		((d.Verdict == VerdictReview || d.Verdict == VerdictDrop) && byModel)
}

// ReviewPlacement is the label among decisions, one item's at every gate: the
// newest side decision, where a person made it after the gates sent the item
// to review. The gates sent it there where an earlier side decision was a
// review, or where the quality gate's review came before the placement. A
// placement that no review came before labels nothing, as one made before the
// gates ran, or one carried over from a judgment made elsewhere.
func ReviewPlacement(decisions []Decision) (Decision, bool) {
	var placement Decision
	sided := false
	for _, d := range decisions {
		if d.Gate == GateSide && (!sided || d.Sequence > placement.Sequence) {
			placement, sided = d, true
		}
	}
	if !sided || !placement.ByPerson() {
		return Decision{}, false
	}
	if _, ok := ReviewBefore(decisions, placement); !ok {
		return Decision{}, false
	}
	return placement, true
}

// ReviewBefore is the review among decisions that sent the item to the person
// who made placement: the newest side review before it, else the newest
// quality review before it, and false where neither came before it.
func ReviewBefore(decisions []Decision, placement Decision) (Decision, bool) {
	var side, quality *Decision
	for i, d := range decisions {
		switch {
		case d.Verdict != VerdictReview:
		case d.Gate == GateSide && d.Sequence < placement.Sequence && (side == nil || d.Sequence > side.Sequence):
			side = &decisions[i]
		case d.Gate == GateQuality && d.DecidedTS < placement.DecidedTS && (quality == nil || d.Sequence > quality.Sequence):
			quality = &decisions[i]
		}
	}
	for _, d := range []*Decision{side, quality} {
		if d != nil {
			return *d, true
		}
	}
	return Decision{}, false
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
	// Title, URL and SourceTS are the item's when the delivery was first
	// queued, which every attempt sends. So is Path, for a deliverer that
	// writes an item by its file: its live home's path below the root that
	// the deliverer's config names. It is nil for every other deliverer.
	Title    *string `json:"title"`
	URL      *string `json:"url"`
	SourceTS *string `json:"source_ts"`
	Path     *string `json:"path"`
	// BookDestinationID and the pages are a book's chapter's, kept the same
	// way from its ChapterInBook, and nil for every other item.
	BookDestinationID *string `json:"book_destination_id"`
	PDFPageStart      *int    `json:"pdf_page_start"`
	PDFPageEnd        *int    `json:"pdf_page_end"`
	PrintedPageStart  *string `json:"printed_page_start"`
	PrintedPageEnd    *string `json:"printed_page_end"`
	UpdatedTS         string  `json:"updated_ts"`
}

// ChapterInBook is where a book's chapter sits in its destination: the
// destination's id for its book's record, from the book's settled delivery,
// and the chapter's pages in the book, from the newest sections that name it.
// A deliver task carries it whole, as its delivery first kept it.
type ChapterInBook struct {
	BookDestinationID string  `json:"book_destination_id"`
	PDFPageStart      int     `json:"pdf_page_start"`
	PDFPageEnd        int     `json:"pdf_page_end"`
	PrintedPageStart  *string `json:"printed_page_start"`
	PrintedPageEnd    *string `json:"printed_page_end"`
}

// Chapter is where the delivery's chapter sits in its book's record, as the
// delivery first kept it, and nil where the delivery is no chapter's.
func (d Delivery) Chapter() *ChapterInBook {
	if d.BookDestinationID == nil || d.PDFPageStart == nil || d.PDFPageEnd == nil {
		return nil
	}
	return &ChapterInBook{
		BookDestinationID: *d.BookDestinationID, PDFPageStart: *d.PDFPageStart, PDFPageEnd: *d.PDFPageEnd,
		PrintedPageStart: d.PrintedPageStart, PrintedPageEnd: d.PrintedPageEnd,
	}
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
	// FinishedTS is when a connector scheduled once ran a poll that ended
	// done, after which it never runs again, and nil until then.
	FinishedTS *string `json:"finished_ts"`
	CreatedTS  string  `json:"created_ts"`
	UpdatedTS  string  `json:"updated_ts"`
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

// The keyed routes, as a KeyedRequest's Route names each. KeyedRoutes lists
// them, and the record schema's route enum holds the same names.
const (
	KeyedRouteDrops  = "drops"
	KeyedRouteRuns   = "runs"
	KeyedRoutePushes = "pushes"
)

// KeyedRoutes is every keyed route's name.
var KeyedRoutes = []string{KeyedRouteDrops, KeyedRouteRuns, KeyedRoutePushes}

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
