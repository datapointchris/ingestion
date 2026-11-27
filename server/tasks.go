package server

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// ScopeTasksClaim admits a worker to claim tasks, extend their leases and
// report them.
const ScopeTasksClaim = "ingestion.tasks.claim"

const (
	defaultLease = 5 * time.Minute
	minLease     = 10 * time.Second
	maxLease     = time.Hour
	maxClaim     = 50
)

var (
	errDeliveryOutsideDeliver  = errors.New("only a deliver task reports a delivery")
	errBadDelivery             = errors.New("the delivery is not one the store records")
	errArrivalsOutsideDonePoll = errors.New("only a poll that ends done reports arrivals or a cursor")
	errMetricsOutsideRefresh   = errors.New("only a refresh of a source's metrics reports metrics")
)

// MaxListed is the most tasks one list answers.
const MaxListed = 1000

// ClaimRequest is a worker asking for up to Max tasks whose capability it
// holds, each leased for LeaseSeconds. MaxByCapability bounds how many of
// each capability it names the claim leases, such as a worker whose slot
// limit for one capability holds one of its slots free for the rest.
// Each of SlotGroups bounds the tasks of its capabilities together, such as
// two heavy parses where the worker's memory holds one. A capability sits in
// one group at most, and MaxByCapability names no grouped one.
type ClaimRequest struct {
	WorkerID        string               `json:"worker_id"`
	Capabilities    []string             `json:"capabilities"`
	Version         string               `json:"version"`
	StartedTS       string               `json:"started_ts"`
	Max             int                  `json:"max"`
	MaxByCapability map[string]int       `json:"max_by_capability,omitempty"`
	SlotGroups      map[string]SlotGroup `json:"slot_groups,omitempty"`
	LeaseSeconds    int                  `json:"lease_seconds"`
}

// SlotGroup is the capabilities of one slot group in a claim, and the most of
// them together that the claim leases.
type SlotGroup struct {
	Capabilities []string `json:"capabilities"`
	Max          int      `json:"max"`
}

// LeasedTask is one leased task, with the token for every heartbeat and
// report on it. A stage's task names its item. A poll names its connector,
// and its capability is poll:<source>, so each source's handler claims its
// own. A refresh of a source's metrics names its source, and its capability
// is refresh-metrics:<source>.
type LeasedTask struct {
	ID             string  `json:"id"`
	ItemKey        *string `json:"item_key"`
	Connector      *string `json:"connector"`
	Source         *string `json:"source,omitempty"`
	Stage          string  `json:"stage"`
	Capability     string  `json:"capability"`
	Attempts       int     `json:"attempts"`
	LeaseToken     int64   `json:"lease_token"`
	LeaseExpiresTS string  `json:"lease_expires_ts"`
	// WaitedSeconds is how long before this claim a report first said the
	// task waits, and 0 where none has.
	WaitedSeconds int64 `json:"waited_seconds,omitempty"`
	// MediaType and BlobSHA256 are the item's, so a worker reads its bytes
	// holding no scope to read items. A poll carries neither, and BlobSHA256
	// is null where the item holds no bytes.
	MediaType  string  `json:"media_type"`
	BlobSHA256 *string `json:"blob_sha256"`
	// Zone, Title, URL and LandedTS are the item's too, so a deliverer writes
	// it holding no scope to read items. A deliver task's Title, URL and
	// SourceTS are its delivery's, as they were when it was first queued, so
	// a retry sends what the first attempt sent. SourceTS is when the item
	// was made at its source, and null where its source gave no time. Path
	// is a deliver task's file below its deliverer's root, kept the same way,
	// and absent for a deliverer that writes no file. A poll carries none of
	// them.
	Zone     store.Zone `json:"zone,omitempty"`
	Title    *string    `json:"title"`
	URL      *string    `json:"url"`
	LandedTS string     `json:"landed_ts,omitempty"`
	SourceTS *string    `json:"source_ts,omitempty"`
	Path     *string    `json:"path,omitempty"`
	// Playlist is a watch's deliver task's: the playlist that the delivery
	// adds the video to, kept the same way. Every other task carries none.
	Playlist *string `json:"playlist,omitempty"`
	// Chapter is a deliver task's of a book's chapter, kept the same way:
	// the destination's id for its book's record, and where the chapter
	// sits in the book. Every other task carries none.
	Chapter *store.ChapterInBook `json:"chapter,omitempty"`
	// Destination names where a deliver task writes its item, and is null
	// for every other task.
	Destination *string `json:"destination"`
	// StageOutputs are the item's outputs at a model stage's task's stage,
	// so a task whose run is already dispatched follows that run rather than
	// starting another. Every other task carries none.
	StageOutputs []StageOutput `json:"stage_outputs,omitempty"`
	// Parse is the done parse that the prompt of a model stage's task quotes,
	// as the claim read it: the item's reading parse, or the one extractor's
	// that the stage reads. The task's call reads it, and its output names it.
	// It is absent where the stage quotes no parse or the item holds none.
	Parse *store.ParseRef `json:"parse,omitempty"`
	// Lens, Note and NoteDraft are a model read's task's: the lens of the
	// item's newest read decision, the note that a person wrote on it, and
	// the note stage's draft where the decision took one instead. A draft
	// never rides in Note, so a reader that knows only Note never reads a
	// model's draft as the owner's. A note task carries Lens alone, and every
	// other task none of them.
	Lens      *string `json:"lens,omitempty"`
	Note      *string `json:"note,omitempty"`
	NoteDraft *string `json:"note_draft,omitempty"`
	// Model is a measurement arm's task's. The task asks its run for this
	// model in place of the worker's configured one. Every other task
	// carries none.
	Model *string `json:"model,omitempty"`
	// Cursor and Declaration are a poll's: its connector's position, where an
	// earlier poll moved it, and the connector's declaration as the set in
	// force holds it, so the source's handler reads what its connector names.
	// Declaration is absent where the connector is missing from the set in
	// force. Every other task carries neither.
	Cursor      *string         `json:"cursor,omitempty"`
	Declaration json.RawMessage `json:"declaration,omitempty"`
	// Sighting is a delete task's: the sighting whose source the task removes
	// the item from. Every other task carries none.
	Sighting *LeasedSighting `json:"sighting,omitempty"`
	// ProposalID and Proposal are a prompt-proposal task's: the prompt proposal that
	// it drafts, and the misses that the draft reads. Its StageOutputs hold
	// the proposal's run, once one was sent. Every other task carries
	// neither.
	ProposalID *string         `json:"proposal_id,omitempty"`
	Proposal   *LeasedProposal `json:"proposal,omitempty"`
	// Keys are a refresh-metrics task's: the key of each item in review that
	// a connector of its source sighted, as the claim read them, whose
	// metrics the task reads again. Every other task carries none.
	Keys []string `json:"keys,omitempty"`
}

// Answers is the note that a model read's task answers: the person's note
// where the task carries one, the draft otherwise, and nil with neither.
func (t LeasedTask) Answers() *string {
	if store.NoteDigest(t.Note) != nil {
		return t.Note
	}
	return t.NoteDraft
}

// LeasedProposal is the prompt proposal that a prompt-proposal task drafts: the
// prompt it changes, at the version it changes, and the misses labeled after
// LabeledAfter, through LabeledThrough, as one JSON array.
type LeasedProposal struct {
	Prompt        string `json:"prompt"`
	PromptVersion string `json:"prompt_version"`
	// PromptText is the template of the prompt at PromptVersion, which the
	// draft's edits apply to.
	PromptText     string          `json:"prompt_text"`
	LabeledAfter   *string         `json:"labeled_after"`
	LabeledThrough string          `json:"labeled_through"`
	MissCount      int             `json:"miss_count"`
	Misses         json.RawMessage `json:"misses"`
}

// LeasedSighting is one connector's sighting of a delete task's item, which
// the task removes from that connector's source: the source's own key for the
// item, and the fields read from its payload, such as a playlist item's id.
type LeasedSighting struct {
	Connector   string            `json:"connector"`
	ExternalKey string            `json:"external_key"`
	Fields      map[string]string `json:"fields"`
}

// StageOutput is one output that a model stage's task carries: which prompt
// wrote it over which parse, how far it got, and the dispatch it waits on.
// Error is why a waiting one has not started, or why a failed one failed.
type StageOutput struct {
	Producer        string  `json:"producer"`
	ProducerVersion string  `json:"producer_version"`
	NoteDigest      *string `json:"note_digest"`
	InputProducer   *string `json:"input_producer"`
	InputVersion    *string `json:"input_version"`
	State           string  `json:"state"`
	DispatchID      *string `json:"dispatch_id"`
	Model           *string `json:"model"`
	Effort          *string `json:"effort"`
	Error           *string `json:"error"`
}

// ClaimAnswer is every task that the claim leased, and none where nothing waits.
type ClaimAnswer struct {
	Tasks []LeasedTask `json:"tasks"`
}

// HeartbeatRequest extends a lease by LeaseSeconds from now.
type HeartbeatRequest struct {
	Token        int64 `json:"token"`
	LeaseSeconds int   `json:"lease_seconds"`
}

// Report outcomes.
const (
	OutcomeDone   = "done"
	OutcomeFailed = "failed"
	OutcomeRetry  = "retry"
)

// ReportRequest ends a lease. Outputs are the stage's records for the task's
// item, and Delivery is how a deliver task's write ended. Arrivals are what a
// poll that ends done found at its source, in source order, and Cursor the
// connector's next position, where the poll moves it. Metrics are what a
// refresh of a source's metrics read again, however it ended, so a refresh
// stopped partway keeps what it read. Each is written in
// the same transaction that ends the lease. A poll that ends done has
// reported every item its source holds past its cursor, so a connector
// scheduled once never polls after it.
type ReportRequest struct {
	Token             int64   `json:"token"`
	Outcome           string  `json:"outcome"`
	Error             *string `json:"error,omitempty"`
	RetryAfterSeconds int     `json:"retry_after_seconds,omitempty"`
	// Wait marks a retry that its far end asked for, or that a stopping
	// worker hands back, so the claim spends none of the task's attempts.
	Wait     bool            `json:"wait,omitempty"`
	Outputs  []ReportOutput  `json:"outputs,omitempty"`
	Delivery *ReportDelivery `json:"delivery,omitempty"`
	Arrivals []ReportArrival `json:"arrivals,omitempty"`
	Cursor   *string         `json:"cursor,omitempty"`
	Metrics  []ReportMetrics `json:"metrics,omitempty"`
}

// ReportMetrics is one item's metrics as a refresh read them again at its
// source, by name, such as a video's views. serve writes them onto each
// sighting of the item by a connector of the task's source, and stamps when
// they were read. It changes nothing else of the sighting.
type ReportMetrics struct {
	Key     string           `json:"key"`
	Metrics map[string]int64 `json:"metrics"`
}

// ReportArrival is one item that a poll found at its source. serve lands it
// under the poll's connector, with the zone and tags that the set in force
// declares for it. Key or URL names the item, and landing refuses a key that
// the URL does not derive. Title, ExternalKey and Fields are outsider text
// where the connector's zone is outside. Metrics are the source's counts by
// name, which ship on the record channel. SourceTS is when the item was made
// at its source, in RFC 3339.
type ReportArrival struct {
	Key       string `json:"key,omitempty"`
	URL       string `json:"url,omitempty"`
	MediaType string `json:"media_type"`
	Title     string `json:"title,omitempty"`
	// ExternalKey is the source's own key for the item, such as a playlist
	// item's id, which a deleter removes.
	ExternalKey string            `json:"external_key,omitempty"`
	Fields      map[string]string `json:"fields,omitempty"`
	Metrics     map[string]int64  `json:"metrics,omitempty"`
	SourceTS    *string           `json:"source_ts,omitempty"`
	// BlobSHA256 names the item's bytes, which the worker kept through the
	// blob route before it reported the arrival, such as a page's HTML.
	BlobSHA256 *string `json:"blob_sha256,omitempty"`
}

// arrival is a as landing takes it. The connector, zone and tags are left for
// the poll's declaration to fill.
func (a ReportArrival) arrival() (landing.Arrival, error) {
	out := landing.Arrival{
		Key: a.Key, URL: a.URL, MediaType: a.MediaType, Title: a.Title, ExternalKey: a.ExternalKey, Fields: a.Fields, Metrics: a.Metrics,
	}
	if a.BlobSHA256 != nil {
		out.BlobSHA256 = *a.BlobSHA256
	}
	if a.SourceTS != nil {
		at, err := time.Parse(time.RFC3339Nano, *a.SourceTS)
		if err != nil {
			return out, fmt.Errorf("%w: source_ts is an RFC 3339 time, not %q", landing.ErrInvalidArrival, *a.SourceTS)
		}
		out.SourceTime = at
	}
	return out, nil
}

// ReportDelivery is how one write of a deliver task's item ended: its outcome,
// the destination's id for the record that holds the item, and the
// destination's answer where the delivery is pending or refused.
type ReportDelivery struct {
	Outcome       string  `json:"outcome"`
	DestinationID *string `json:"destination_id,omitempty"`
	Error         *string `json:"error,omitempty"`
}

// ReportOutput is one output a worker reports: each field of an outputs row
// that the stage's work decides, and a parse's units. The service takes the
// item and stage from the task and stamps the row, so a column the store adds
// for itself never changes what a worker sends.
type ReportOutput struct {
	Producer        string  `json:"producer"`
	ProducerVersion string  `json:"producer_version"`
	Lens            *string `json:"lens,omitempty"`
	LensVersion     *string `json:"lens_version,omitempty"`
	// NoteDigest is store.NoteDigest of the note that a model read answered.
	NoteDigest    *string       `json:"note_digest,omitempty"`
	State         string        `json:"state"`
	BlobSHA256    *string       `json:"blob_sha256,omitempty"`
	DispatchID    *string       `json:"dispatch_id,omitempty"`
	RunID         *string       `json:"run_id,omitempty"`
	Model         *string       `json:"model,omitempty"`
	Effort        *string       `json:"effort,omitempty"`
	AnsweredModel *string       `json:"answered_model,omitempty"`
	Tokens        *store.Tokens `json:"tokens,omitempty"`
	// InputProducer and InputVersion name the parse that a later stage read.
	InputProducer *string `json:"input_producer,omitempty"`
	InputVersion  *string `json:"input_version,omitempty"`
	// Findings and Spans are a scan's, and a model read's over its
	// suggestion sentence. A scan that found nothing sends an empty list,
	// which reads apart from a stage that sends none. RulesVersion is the
	// version of the rules that made them.
	Findings       []store.Finding `json:"findings"`
	RulesVersion   *string         `json:"rules_version,omitempty"`
	Quality        *int            `json:"quality,omitempty"`
	Fit            *int            `json:"fit,omitempty"`
	SideGuess      *string         `json:"side_guess,omitempty"`
	SideConfidence *float64        `json:"side_confidence,omitempty"`
	// Units are a parse's, numbered from 1 in order, which the service writes
	// as rows of parse_units under the producer and its version.
	Units  []store.Unit    `json:"units,omitempty"`
	Spans  []store.Span    `json:"spans"`
	Answer json.RawMessage `json:"answer,omitempty"`
	Error  *string         `json:"error,omitempty"`
}

// run is how o's run went, which every subject's table keeps the same way.
func (o ReportOutput) run() store.StageRun {
	return store.StageRun{
		Producer: o.Producer, ProducerVersion: o.ProducerVersion, State: o.State, DispatchID: o.DispatchID, RunID: o.RunID,
		Model: o.Model, Effort: o.Effort, AnsweredModel: o.AnsweredModel, Tokens: o.Tokens, Answer: o.Answer, Error: o.Error,
	}
}

// output is o as an output row, whose item and stage KeepRun takes from the
// task.
func (o ReportOutput) output() store.Output {
	return store.Output{
		StageRun: o.run(), Lens: o.Lens, LensVersion: o.LensVersion, NoteDigest: o.NoteDigest,
		InputProducer: o.InputProducer, InputVersion: o.InputVersion,
		BlobSHA256: o.BlobSHA256, Findings: o.Findings, RulesVersion: o.RulesVersion, Quality: o.Quality,
		Fit: o.Fit, SideGuess: o.SideGuess, SideConfidence: o.SideConfidence, Spans: o.Spans,
	}
}

// stageOutput is run as a model stage's task carries it.
func stageOutput(run store.StageRun) StageOutput {
	return StageOutput{
		Producer: run.Producer, ProducerVersion: run.ProducerVersion, State: run.State, DispatchID: run.DispatchID,
		Model: run.Model, Effort: run.Effort, Error: run.Error,
	}
}

// itemStageOutput is an item's output o as its model stage's task carries it,
// with the note digest of a model read and the parse that its call quoted.
func itemStageOutput(o store.Output) StageOutput {
	out := stageOutput(o.StageRun)
	out.NoteDigest, out.InputProducer, out.InputVersion = o.NoteDigest, o.InputProducer, o.InputVersion
	return out
}

// TaskView is one task as the list shows it. It leaves out the lease token
// and the task's error, which can quote outsider text, and says in has_error
// whether one is kept.
type TaskView struct {
	ID      string  `json:"id"`
	ItemKey *string `json:"item_key"`
	// Connector names a poll's connector, and the connector whose sighting a
	// delete removes from its source.
	Connector   *string `json:"connector"`
	Destination *string `json:"destination"`
	Model       *string `json:"model"`
	// ProposalID names the prompt proposal that a prompt-proposal task drafts.
	ProposalID *string `json:"proposal_id"`
	// Source names the source whose metrics a refresh-metrics task reads
	// again.
	Source         *string `json:"source"`
	Stage          string  `json:"stage"`
	Capability     string  `json:"capability"`
	State          string  `json:"state"`
	HeldBy         *string `json:"held_by"`
	Lapsed         bool    `json:"lapsed"`
	LeaseHolder    *string `json:"lease_holder"`
	LeaseExpiresTS *string `json:"lease_expires_ts"`
	Attempts       int     `json:"attempts"`
	HasError       bool    `json:"has_error"`
	NotBeforeTS    *string `json:"not_before_ts"`
	WaitingSinceTS *string `json:"waiting_since_ts"`
	CreatedTS      string  `json:"created_ts"`
	UpdatedTS      string  `json:"updated_ts"`
}

// TasksAnswer is what GET /api/v1/tasks answers:
// schemas/tasks-list.schema.json.
type TasksAnswer struct {
	Tasks   []TaskView     `json:"tasks"`
	Workers []store.Worker `json:"workers"`
	// WorkersHidden counts the workers not heard from within
	// WorkersHeardWithin, which the list leaves out unless asked for all.
	WorkersHidden int `json:"workers_hidden"`
}

// WorkersHeardWithin is how recently a worker was heard from for the list to
// show it by default. Each process start is a worker of its own, so the
// workers that stopped outnumber the live ones within a few releases.
const WorkersHeardWithin = 10 * time.Minute

// AfterStage runs in the write that ends an item's task done, after the
// stage's outputs, such as the scan and the gates that follow an item's parse.
// An error rolls the whole report back, so the task stays leased.
type AfterStage func(ctx context.Context, tx *store.Tx, task store.Task) error

// TaskRoutes are the routes for a worker to lease tasks through and read a
// task's input, and the list for a person to read. lander lands what each poll
// found, in the write that ends the poll's lease. after runs in the write that
// ends each item's task done, and nil runs nothing.
func TaskRoutes(st *store.Store, lander *landing.Lander, log *slog.Logger, after AfterStage) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := taskHandlers{store: st, lander: lander, log: log, after: after}
	return func(r *Router) {
		r.Handle(RouteTaskClaims, h.claim)
		r.Handle(RouteTaskHeartbeats, h.heartbeat)
		r.Handle(RouteTaskReports, h.report)
		r.Handle(RouteTaskParse, h.parse)
		r.Handle(RouteTaskRetries, h.retry)
		r.Handle(RouteTasks, h.list)
	}
}

type taskHandlers struct {
	store  *store.Store
	lander *landing.Lander
	log    *slog.Logger
	after  AfterStage
}

func (h taskHandlers) claim(w http.ResponseWriter, r *http.Request) {
	var body ClaimRequest
	if !decode(w, r, &body) {
		return
	}
	if body.WorkerID == "" || body.Version == "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a claim names its worker_id and version")
		return
	}
	if body.Max < 0 || body.Max > maxClaim {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "max is from 0 to %d, not %d", maxClaim, body.Max)
		return
	}
	for capability, most := range body.MaxByCapability {
		if most < 0 || most > maxClaim {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "max_by_capability's %s is from 0 to %d, not %d", capability, maxClaim, most)
			return
		}
	}
	groups, ok := slotGroupsOf(w, body)
	if !ok {
		return
	}
	lease, ok := leaseOf(w, body.LeaseSeconds)
	if !ok {
		return
	}
	var admission connectors.Admission
	keysOf := map[string][]string{}
	tasks, err := h.store.Claim(r.Context(), store.Claim{
		Worker: store.Worker{ID: body.WorkerID, Capabilities: body.Capabilities, Version: body.Version, StartedTS: body.StartedTS},
		Max:    body.Max, MaxByCapability: body.MaxByCapability, SlotGroups: groups, Lease: lease,
		Admit: func(ctx context.Context, tx *store.Tx, task store.Task) (string, error) {
			why, keys, err := admission.Admit(ctx, tx, task)
			if len(keys) > 0 {
				keysOf[task.ID] = keys
			}
			return why, err
		},
		Lapsed: func(ctx context.Context, tx *store.Tx, task store.Task, reason string) error {
			if task.Stage != store.StagePoll || task.Connector == nil {
				return nil
			}
			return connectors.PollFailed(ctx, tx, *task.Connector, "the poll failed: "+reason)
		},
	})
	if err != nil {
		h.internal(w, r, "claim tasks", err)
		return
	}
	answer := ClaimAnswer{Tasks: []LeasedTask{}}
	for _, t := range tasks {
		leased := LeasedTask{
			ID: t.ID, ItemKey: t.ItemKey, Connector: t.Connector, Source: t.Source, Stage: t.Stage, Capability: t.Capability, Attempts: t.Attempts,
			LeaseToken: *t.LeaseToken, LeaseExpiresTS: *t.LeaseExpiresTS, MediaType: t.Item.MediaType, BlobSHA256: t.Item.BlobSHA256,
			Zone: t.Item.Zone, Title: t.Item.Title, URL: t.Item.URL, LandedTS: t.Item.CreatedTS, Destination: t.Destination,
			Model: t.Model, ProposalID: t.ProposalID, WaitedSeconds: waited(t.Task), Keys: keysOf[t.ID],
		}
		if d := t.Delivery; d != nil {
			leased.Title, leased.URL, leased.SourceTS, leased.Path, leased.Chapter = d.Title, d.URL, d.SourceTS, d.Path, d.Chapter()
			leased.Playlist = d.Playlist
		}
		if t.Read != nil {
			leased.Lens = t.Read.Lens
			if t.Stage == store.StageModelRead {
				if note, drafted := route.NoteOf(*t.Read, t.Draft); drafted {
					leased.NoteDraft = note
				} else {
					leased.Note = note
				}
			}
		}
		for _, o := range t.Outputs {
			leased.StageOutputs = append(leased.StageOutputs, itemStageOutput(o))
		}
		leased.Parse = t.Parse.Ref()
		if s := t.Sighting; s != nil {
			leased.Sighting = &LeasedSighting{Connector: s.Connector, ExternalKey: s.ExternalKey, Fields: s.Fields}
		}
		if p := t.Proposal; p != nil {
			leased.Proposal = &LeasedProposal{
				Prompt: p.Prompt, PromptVersion: p.PromptVersion, PromptText: p.PromptText, LabeledAfter: p.LabeledAfter,
				LabeledThrough: p.LabeledThrough, MissCount: p.MissCount, Misses: p.Misses,
			}
			leased.StageOutputs = append(leased.StageOutputs, stageOutput(p.StageRun))
		}
		if t.Stage == store.StagePoll && t.Connector != nil {
			if t.Cursor != nil {
				leased.Cursor = t.Cursor.Position
			}
			if c, ok := admission.Connector(*t.Connector); ok {
				if leased.Declaration, err = json.Marshal(c); err != nil {
					h.internal(w, r, "write a connector's declaration", err)
					return
				}
			}
		}
		answer.Tasks = append(answer.Tasks, leased)
	}
	for _, t := range answer.Tasks {
		h.log.Info("leased a task", "task", t.ID, "item", text(t.ItemKey), "connector", text(t.Connector), "source", text(t.Source),
			"stage", t.Stage, "keys", len(t.Keys), "worker", body.WorkerID, "token", t.LeaseToken)
	}
	JSON(w, http.StatusOK, answer)
}

// slotGroupsOf is body's slot groups as the store bounds a claim by them. It
// refuses a group's bound outside 0 to maxClaim, a capability in two groups,
// and a grouped capability that max_by_capability bounds too. ok is false once
// it has answered.
func slotGroupsOf(w http.ResponseWriter, body ClaimRequest) (map[string]store.SlotGroup, bool) {
	if len(body.SlotGroups) == 0 {
		return nil, true
	}
	groups := map[string]store.SlotGroup{}
	grouped := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(body.SlotGroups)) {
		g := body.SlotGroups[name]
		if name == "" || g.Max < 0 || g.Max > maxClaim {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a slot group has a name and a max from 0 to %d, not %q and %d", maxClaim, name, g.Max)
			return nil, false
		}
		for _, capability := range g.Capabilities {
			if other, taken := grouped[capability]; taken {
				Refuse(w, http.StatusBadRequest, CodeInvalidBody, "capability %s sits in slot groups %s and %s, and one holds it at most", capability, other, name)
				return nil, false
			}
			if _, bound := body.MaxByCapability[capability]; bound {
				Refuse(w, http.StatusBadRequest, CodeInvalidBody, "capability %s sits in slot group %s, so max_by_capability may not name it", capability, name)
				return nil, false
			}
			grouped[capability] = name
		}
		groups[name] = store.SlotGroup{Capabilities: g.Capabilities, Max: g.Max}
	}
	return groups, true
}

func (h taskHandlers) heartbeat(w http.ResponseWriter, r *http.Request) {
	var body HeartbeatRequest
	if !decode(w, r, &body) {
		return
	}
	lease, ok := leaseOf(w, body.LeaseSeconds)
	if !ok {
		return
	}
	task, err := h.store.Heartbeat(r.Context(), r.PathValue("id"), body.Token, lease)
	if h.leaseRefused(w, r, err) {
		return
	}
	JSON(w, http.StatusOK, viewOf(task, h.store.Now()))
}

// waited is how long before t's claim, which stamped it, a report first said
// t waits, in whole seconds, and 0 where none has.
func waited(t store.Task) int64 {
	if t.WaitingSinceTS == nil {
		return 0
	}
	since, err := time.Parse(store.StampLayout, *t.WaitingSinceTS)
	if err != nil {
		return 0
	}
	claimed, err := time.Parse(store.StampLayout, t.UpdatedTS)
	if err != nil {
		return 0
	}
	return int64(max(claimed.Sub(since), 0) / time.Second)
}

func (h taskHandlers) report(w http.ResponseWriter, r *http.Request) {
	var body ReportRequest
	if !decode(w, r, &body) {
		return
	}
	outcome := store.Outcome{Error: body.Error}
	switch body.Outcome {
	case OutcomeDone:
		outcome.State = store.TaskDone
	case OutcomeFailed:
		outcome.State = store.TaskFailed
	case OutcomeRetry:
		outcome.State = store.TaskQueued
		outcome.RetryAfter = time.Duration(body.RetryAfterSeconds) * time.Second
		outcome.Wait = body.Wait
	default:
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "outcome is done, failed or retry, not %q", body.Outcome)
		return
	}
	var polled connectors.PollLanded
	var refreshed connectors.MetricsRefreshed
	task, err := h.store.Report(r.Context(), r.PathValue("id"), body.Token, outcome, func(tx *store.Tx, task store.Task, ended *store.Outcome) error {
		if (len(body.Arrivals) > 0 || body.Cursor != nil) && (task.Stage != store.StagePoll || outcome.State != store.TaskDone) {
			return errArrivalsOutsideDonePoll
		}
		if len(body.Metrics) > 0 {
			if task.Stage != store.StageRefreshMetrics || task.Source == nil {
				return errMetricsOutsideRefresh
			}
			read := make(map[string]map[string]int64, len(body.Metrics))
			for _, m := range body.Metrics {
				read[m.Key] = m.Metrics
			}
			var err error
			if refreshed, err = connectors.RefreshMetrics(r.Context(), tx, *task.Source, read); err != nil {
				return err
			}
		}
		if task.Stage == store.StagePoll && task.Connector != nil {
			var err error
			if polled, err = h.endPoll(r.Context(), tx, *task.Connector, outcome, body); err != nil {
				return err
			}
			if polled.Failure != "" {
				ended.State, ended.Error = store.TaskFailed, &polled.Failure
			}
		}
		for _, o := range body.Outputs {
			if err := tx.KeepRun(r.Context(), task, o.output(), o.Units); err != nil {
				return err
			}
		}
		if d := body.Delivery; d != nil {
			if task.Stage != store.StageDeliver || task.Destination == nil {
				return errDeliveryOutsideDeliver
			}
			if err := tx.RecordDelivery(r.Context(), task, store.DeliveryAttempt{
				Outcome: d.Outcome, DestinationID: d.DestinationID, Error: d.Error,
			}); err != nil {
				return fmt.Errorf("%w: %w", errBadDelivery, err)
			}
		}
		if h.after != nil && outcome.State == store.TaskDone && task.ItemKey != nil {
			return h.after(r.Context(), tx, task)
		}
		return nil
	})
	if h.leaseRefused(w, r, err) {
		return
	}
	h.log.Info("a task ended", "task", task.ID, "item", text(task.ItemKey), "connector", text(task.Connector), "source", text(task.Source),
		"stage", task.Stage, "state", task.State, "outputs", len(body.Outputs), "arrivals", polled.Arrivals, "items_added", polled.Items,
		"sightings_added", polled.Sightings, "deletes_queued", polled.Deletes, "arrivals_refused", polled.Refused,
		"metrics_read", refreshed.Items, "sightings_refreshed", refreshed.Sightings, "metrics_unsighted", refreshed.Unsighted)
	JSON(w, http.StatusOK, viewOf(task, h.store.Now()))
}

// endPoll writes what a poll's report carries to its connector in tx: each
// arrival that can land landed, and the cursor moved where every one did,
// where the poll ended done, and the failure kept where it ended failed. A
// poll sent back to try again changes nothing. Where any arrival could not
// land, or the set in force dropped the connector, it answers why the poll
// fails in Failure.
func (h taskHandlers) endPoll(ctx context.Context, tx *store.Tx, connector string, outcome store.Outcome, body ReportRequest) (connectors.PollLanded, error) {
	switch outcome.State {
	case store.TaskFailed:
		reason := "the poll failed and its worker gave no reason"
		if body.Error != nil {
			reason = *body.Error
		}
		return connectors.PollLanded{}, connectors.PollFailed(ctx, tx, connector, reason)
	case store.TaskDone:
		arrivals := make([]connectors.PollArrival, len(body.Arrivals))
		for i, a := range body.Arrivals {
			arrivals[i].Arrival, arrivals[i].Refused = a.arrival()
		}
		if h.lander == nil && len(arrivals) > 0 {
			return connectors.PollLanded{}, errors.New("these routes hold no lander, so a poll's arrivals cannot land")
		}
		landed, err := connectors.LandPoll(ctx, tx, h.lander, connector, arrivals, body.Cursor)
		if errors.Is(err, connectors.ErrNotDeclared) {
			return connectors.PollLanded{
				Arrivals: len(arrivals), Refused: len(arrivals),
				Failure: fmt.Sprintf("%v, so nothing its poll found landed", err),
			}, nil
		}
		return landed, err
	}
	return connectors.PollLanded{}, nil
}

// retry queues a failed task again for a person. A model stage's task whose
// run failed then dispatches again, and one whose run waits follows it.
func (h taskHandlers) retry(w http.ResponseWriter, r *http.Request) {
	task, err := h.store.RetryTask(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, store.ErrNoTask):
		Refuse(w, http.StatusNotFound, CodeTaskNotFound, "no task %s", r.PathValue("id"))
		return
	case errors.Is(err, store.ErrTaskDone):
		Refuse(w, http.StatusConflict, CodeTaskDone, "task %s ended done, so nothing is retried: items replay runs its stage again", r.PathValue("id"))
		return
	case errors.Is(err, store.ErrTaskSuperseded):
		Refuse(w, http.StatusConflict, CodeTaskSuperseded, "%v, so nothing is retried", err)
		return
	case err != nil:
		h.internal(w, r, "retry a task", err)
		return
	}
	h.log.Info("a task was queued again", "task", task.ID, "item", text(task.ItemKey), "stage", task.Stage)
	JSON(w, http.StatusOK, TaskRetryAnswer{SchemaVersion: 1, Task: viewOf(task, h.store.Now())})
}

// TaskRetryAnswer is what POST /api/v1/tasks/{id}/retries answers:
// schemas/task-retry.schema.json.
type TaskRetryAnswer struct {
	SchemaVersion int      `json:"schema_version"`
	Task          TaskView `json:"task"`
}

// parse answers the newest done parse of the item whose task the caller
// leases, under the lease's token in the token query, so a worker reads the
// input of a task it holds and of no other. The extractor and version queries
// narrow it, as on the item's parse route.
func (h taskHandlers) parse(w http.ResponseWriter, r *http.Request) {
	token, err := strconv.ParseInt(r.URL.Query().Get("token"), 10, 64)
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "token is the lease token of the task, not %q", r.URL.Query().Get("token"))
		return
	}
	task, err := h.store.Leased(r.Context(), r.PathValue("id"), token)
	if h.leaseRefused(w, r, err) {
		return
	}
	if task.ItemKey == nil {
		Refuse(w, http.StatusNotFound, CodeParseNotFound, "task %s is for no item, so it has no parse", task.ID)
		return
	}
	pick := store.ParsePick{Extractor: r.URL.Query().Get("extractor"), Version: r.URL.Query().Get("version")}
	p, err := h.store.Parse(r.Context(), *task.ItemKey, pick)
	switch {
	case errors.Is(err, store.ErrNoParse):
		Refuse(w, http.StatusNotFound, CodeParseNotFound, "%s holds no done parse%s", *task.ItemKey, pickWords(pick))
		return
	case err != nil:
		h.internal(w, r, "read a task's parse", err)
		return
	}
	units := p.Units
	if units == nil {
		units = []store.Unit{}
	}
	JSON(w, http.StatusOK, TaskParseAnswer{
		SchemaVersion: 1, Key: p.Item.Key, Extractor: p.Output.Producer, ExtractorVersion: p.Output.ProducerVersion, Units: units,
	})
}

// TaskParseAnswer is what GET /api/v1/tasks/{id}/parse answers:
// schemas/task-parse.schema.json. It carries the units alone, because the
// parse's whole text is the units joined.
type TaskParseAnswer struct {
	SchemaVersion    int          `json:"schema_version"`
	Key              string       `json:"key"`
	Extractor        string       `json:"extractor"`
	ExtractorVersion string       `json:"extractor_version"`
	Units            []store.Unit `json:"units"`
}

func (h taskHandlers) list(w http.ResponseWriter, r *http.Request) {
	q := store.TaskQuery{States: r.URL.Query()["state"]}
	for _, s := range q.States {
		switch s {
		case store.TaskQueued, store.TaskLeased, store.TaskDone, store.TaskFailed:
		default:
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "state is queued, leased, done or failed, not %q", s)
			return
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > MaxListed {
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "limit is from 1 to %d, not %q", MaxListed, raw)
			return
		}
		q.Limit = n
	}
	tasks, err := h.store.Tasks(r.Context(), q)
	if err != nil {
		h.internal(w, r, "list tasks", err)
		return
	}
	now := h.store.Now()
	since := store.Stamp(now.Add(-WorkersHeardWithin))
	switch r.URL.Query().Get("workers") {
	case "":
	case "all":
		since = ""
	default:
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "workers is all or absent, not %q", r.URL.Query().Get("workers"))
		return
	}
	workers, hidden, err := h.store.Workers(r.Context(), since)
	if err != nil {
		h.internal(w, r, "list workers", err)
		return
	}
	answer := TasksAnswer{Tasks: make([]TaskView, 0, len(tasks)), Workers: workers, WorkersHidden: hidden}
	if answer.Workers == nil {
		answer.Workers = []store.Worker{}
	}
	for _, t := range tasks {
		answer.Tasks = append(answer.Tasks, viewOf(t, now))
	}
	JSON(w, http.StatusOK, answer)
}

func viewOf(t store.Task, now time.Time) TaskView {
	return TaskView{
		ID: t.ID, ItemKey: t.ItemKey, Connector: cmp.Or(t.Connector, t.SightingConnector), ProposalID: t.ProposalID, Source: t.Source,
		Destination: t.Destination, Model: t.Model, Stage: t.Stage, Capability: t.Capability,
		State: t.State, HeldBy: t.HeldBy, Lapsed: t.Lapsed(now),
		LeaseHolder: t.LeaseHolder, LeaseExpiresTS: t.LeaseExpiresTS, Attempts: t.Attempts, HasError: t.Error != nil,
		NotBeforeTS: t.NotBeforeTS, WaitingSinceTS: t.WaitingSinceTS, CreatedTS: t.CreatedTS, UpdatedTS: t.UpdatedTS,
	}
}

// leaseRefused answers err's refusal and says whether it answered one: 404
// where the store holds no such task, 409 for a token that does not hold its
// lease, and 500 for any other failure.
func (h taskHandlers) leaseRefused(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNoTask):
		Refuse(w, http.StatusNotFound, CodeTaskNotFound, "no task %s", r.PathValue("id"))
	case errors.Is(err, store.ErrStaleLease):
		Refuse(w, http.StatusConflict, CodeStaleLease, "%v", err)
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusUnprocessableEntity, CodeItemNotFound, "%v", err)
	case errors.Is(err, store.ErrRunWithoutSubject):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "task %s polls a connector, which carries no item, so it reports no outputs", r.PathValue("id"))
	case errors.Is(err, errDeliveryOutsideDeliver):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "task %s delivers nothing, so it reports no delivery", r.PathValue("id"))
	case errors.Is(err, errBadDelivery):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "%v", err)
	case errors.Is(err, errArrivalsOutsideDonePoll), errors.Is(err, errMetricsOutsideRefresh):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "task %s: %v", r.PathValue("id"), err)
	case errors.Is(err, store.ErrInvalidMetric):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "%v", err)
	case errors.Is(err, store.ErrUnitsOutsideParse), errors.Is(err, store.ErrUnitsOutOfOrder):
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidBody, "%v", err)
	default:
		h.internal(w, r, "update a task", err)
	}
	return true
}

func (h taskHandlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.log.ErrorContext(r.Context(), what, "error", err, "path", r.URL.Path)
	Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not %s", what)
}

func leaseOf(w http.ResponseWriter, seconds int) (time.Duration, bool) {
	if seconds == 0 {
		return defaultLease, true
	}
	lease := time.Duration(seconds) * time.Second
	if lease < minLease || lease > maxLease {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "lease_seconds is from %d to %d, not %d",
			int(minLease.Seconds()), int(maxLease.Seconds()), seconds)
		return 0, false
	}
	return lease, true
}

// decode reads r's JSON body into into, and says whether it could. An unknown
// field is refused with 422 naming it, apart from a body that does not parse,
// so a worker built ahead of the service can tell the two apart.
func decode(w http.ResponseWriter, r *http.Request, into any) bool {
	return decodeFrom(w, http.MaxBytesReader(w, r.Body, 64<<20), into)
}

// decodeFrom is decode over body, a request's body that the caller has
// already bounded.
func decodeFrom(w http.ResponseWriter, body io.Reader, into any) bool {
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(into)
	if err == nil {
		return true
	}
	if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		field = strings.Trim(field, `"`)
		status := http.StatusUnprocessableEntity
		JSON(w, status, unknownField{
			refusal: refusal{
				Error: http.StatusText(status), Code: CodeUnknownField,
				Message: fmt.Sprintf("this route reads no field %s, so the caller may be built ahead of this service", field),
			},
			Field: field,
		})
		return false
	}
	Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the body is not one this route reads: %v", err)
	return false
}

// unknownField is the refusal of a body naming a field the route does not read.
type unknownField struct {
	refusal
	Field string `json:"field"`
}
