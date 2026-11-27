package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// ScopeItemsRead admits a caller to read items and their records.
const ScopeItemsRead = "ingestion.items.read"

// DefaultItemsListed bounds a list whose caller names no limit, and
// MaxItemsListed bounds every page. A larger limit answers a page of
// MaxItemsListed and its next cursor, so a caller pages on without knowing it.
const (
	DefaultItemsListed = 100
	MaxItemsListed     = 1000
)

// ItemsAnswer is what GET /api/v1/items answers:
// schemas/items-list.schema.json. Next is the cursor of the next page, and
// null where none is left.
type ItemsAnswer struct {
	SchemaVersion int          `json:"schema_version"`
	Items         []ItemListed `json:"items"`
	Next          *string      `json:"next"`
}

// ItemListed is one item as the list shows it.
type ItemListed struct {
	Key        string     `json:"key"`
	Zone       store.Zone `json:"zone"`
	MediaType  string     `json:"media_type"`
	Title      *string    `json:"title"`
	URL        *string    `json:"url"`
	Connectors []string   `json:"connectors"`
	Phase      string     `json:"phase"`
	WaitingAt  *string    `json:"waiting_at"`
	Side       *string    `json:"side"`
	CreatedTS  string     `json:"created_ts"`
}

// ItemAnswer is what GET /api/v1/items/{key} answers:
// schemas/item.schema.json.
type ItemAnswer struct {
	SchemaVersion int            `json:"schema_version"`
	Item          ItemView       `json:"item"`
	Homes         []HomeView     `json:"homes"`
	Sightings     []SightingView `json:"sightings"`
	Outputs       []OutputView   `json:"outputs"`
	Decisions     []DecisionView `json:"decisions"`
	Deliveries    []DeliveryView `json:"deliveries"`
	// Sections are the item's sections, each producer version's in order.
	Sections []SectionView `json:"sections"`
	// Undelivered says why an item placed on a side and read as is reaches
	// no deliverer, and is null where it does.
	Undelivered *UndeliveredView `json:"undelivered"`
	// Waiting says why the item waits at a model stage, and is null where it
	// waits at none.
	Waiting *WaitingView `json:"waiting"`
	// Progress is the parse's and the scan's progress, as
	// store.ItemRecords.Progress reads it from their tasks and outputs.
	Progress []store.StageProgress `json:"progress"`
	// Suggestion is the item's latest suggestion, and null where no model
	// read holds a sentence.
	Suggestion *SuggestionView `json:"suggestion"`
	// QualityAnswer is the quality gate's newest done answer, as
	// store.NewestDone picks it and the review route serves it, and null where
	// the gate gave none.
	QualityAnswer json.RawMessage `json:"quality_answer"`
	// ModelRead is the newest done model read, as store.NewestDone picks it,
	// with the scanner's findings over its sentence, and null where none is
	// done.
	ModelRead *OutputView `json:"model_read"`
}

// SuggestionView is an item's latest suggestion, as
// schemas/suggestion.schema.json publishes it: its newest done model read
// whose answer holds a sentence, with the scanner's findings first.
type SuggestionView struct {
	SchemaVersion int                     `json:"schema_version"`
	ItemKey       string                  `json:"item_key"`
	Sentence      string                  `json:"sentence"`
	Title         *string                 `json:"title"`
	URL           *string                 `json:"url"`
	Zone          store.Zone              `json:"zone"`
	Findings      []SuggestionFindingView `json:"findings"`
	PromptVersion string                  `json:"prompt_version"`
	// Lens names the read's lens and its version, as "fleet 1-9dcd3940".
	Lens        *string `json:"lens"`
	SuggestedTS string  `json:"suggested_ts"`
}

// SuggestionFindingView is one scanner finding beside a suggestion, in its
// source or in its sentence.
type SuggestionFindingView struct {
	Rule     string  `json:"rule"`
	Severity string  `json:"severity"`
	In       string  `json:"in"`
	Span     *string `json:"span"`
}

// Where a suggestion's finding was found.
const (
	FoundInSource   = "source"
	FoundInSentence = "sentence"
)

// suggestion is rec's latest suggestion. Its findings are the newest done
// scan's, in the source, then the read's own, in the sentence. It is nil where
// no done model read holds a sentence.
func suggestion(rec store.ItemRecords) *SuggestionView {
	var read *store.Output
	sentence := ""
	for i := range rec.Outputs {
		o := &rec.Outputs[i]
		if o.Stage != store.StageModelRead || o.State != store.OutputDone || (read != nil && o.UpdatedTS < read.UpdatedTS) {
			continue
		}
		var answer struct {
			Suggestion *string `json:"suggestion"`
		}
		if json.Unmarshal(o.Answer, &answer) == nil && answer.Suggestion != nil && *answer.Suggestion != "" {
			read, sentence = o, *answer.Suggestion
		}
	}
	if read == nil {
		return nil
	}
	s := &SuggestionView{
		SchemaVersion: 1, ItemKey: rec.Item.Key, Sentence: sentence, Title: rec.Item.Title, URL: rec.Item.URL, Zone: rec.Item.Zone,
		Findings: []SuggestionFindingView{}, PromptVersion: read.ProducerVersion, SuggestedTS: read.UpdatedTS,
	}
	if read.Lens != nil && *read.Lens != "" {
		lens := strings.TrimSpace(*read.Lens + " " + text(read.LensVersion))
		s.Lens = &lens
	}
	if scan := store.NewestDone(rec.Outputs, store.StageScan); scan != nil {
		s.Findings = append(s.Findings, suggestionFindings(*scan, FoundInSource)...)
	}
	s.Findings = append(s.Findings, suggestionFindings(*read, FoundInSentence)...)
	return s
}

// suggestionFindings are o's findings found in in, each with the span at its
// own position where that span names its rule.
func suggestionFindings(o store.Output, in string) []SuggestionFindingView {
	views := make([]SuggestionFindingView, 0, len(o.Findings))
	for i, f := range o.Findings {
		view := SuggestionFindingView{Rule: f.Rule, Severity: f.Severity, In: in}
		if i < len(o.Spans) && o.Spans[i].Rule == f.Rule {
			view.Span = &o.Spans[i].Span
		}
		views = append(views, view)
	}
	return views
}

// Why an item waits at a model stage.
const (
	// WaitingFirstFill is a task that a connector's first fill holds.
	WaitingFirstFill = "first-fill"
	// WaitingUnclaimed is a task that no worker heard from lately claims.
	WaitingUnclaimed = "unclaimed"
	// WaitingQueued is a task that a worker claims next.
	WaitingQueued = "queued"
	// WaitingDispatched is a run that the model port started and that has
	// not answered.
	WaitingDispatched = "dispatched"
)

// WaitingView is why an item waits at a model stage.
type WaitingView struct {
	Stage      string `json:"stage"`
	Reason     string `json:"reason"`
	Capability string `json:"capability"`
	// HeldBy is the connector whose first fill holds the task.
	HeldBy *string `json:"held_by"`
	// DispatchID is the run's dispatch, where the port started one.
	DispatchID *string `json:"dispatch_id"`
	// LastError is why the task's last try did not go through, such as a
	// dispatch service that did not answer, where it ended so.
	LastError *string `json:"last_error"`
	Message   string  `json:"message"`
}

// UndeliveredView is why an item reaches no deliverer, in the words of
// connectors.Undelivered.
type UndeliveredView struct {
	Reason      string  `json:"reason"`
	Side        string  `json:"side"`
	Destination *string `json:"destination"`
	Message     string  `json:"message"`
}

// ItemView is the item, with its bytes' size and MD5 and its derived phase.
type ItemView struct {
	Key        string     `json:"key"`
	Zone       store.Zone `json:"zone"`
	MediaType  string     `json:"media_type"`
	Title      *string    `json:"title"`
	URL        *string    `json:"url"`
	SourceTS   *string    `json:"source_ts"`
	BlobSHA256 *string    `json:"blob_sha256"`
	SizeBytes  *int64     `json:"size_bytes"`
	MD5        *string    `json:"md5"`
	ParentKey  *string    `json:"parent_key"`
	FlowID     string     `json:"flow_id"`
	Phase      string     `json:"phase"`
	WaitingAt  *string    `json:"waiting_at"`
	CreatedTS  string     `json:"created_ts"`
}

// HomeView names where the item's file lives under a named root.
type HomeView struct {
	Root       string  `json:"root"`
	Path       string  `json:"path"`
	ModifiedTS *string `json:"modified_ts"`
	FoundTS    string  `json:"found_ts"`
	GoneTS     *string `json:"gone_ts"`
}

// SightingView is one connector's arrival of the item.
type SightingView struct {
	Connector      string            `json:"connector"`
	ExternalKey    string            `json:"external_key"`
	Tags           []string          `json:"tags"`
	Fields         map[string]string `json:"fields"`
	Metrics        map[string]int64  `json:"metrics"`
	Zone           store.Zone        `json:"zone"`
	CursorPosition *string           `json:"cursor_position"`
	FirstSeenTS    string            `json:"first_seen_ts"`
	LastSeenTS     string            `json:"last_seen_ts"`
	// MetricsReadTS is when the metrics were last taken from the source, by
	// an arrival or by a refresh of an item in review, and null where no read
	// stamped them.
	MetricsReadTS *string `json:"metrics_read_ts"`
}

// OutputView is one stage's output, with a parse's units counted and each
// finding beside its span.
type OutputView struct {
	Stage           string          `json:"stage"`
	Producer        string          `json:"producer"`
	ProducerVersion string          `json:"producer_version"`
	Lens            *string         `json:"lens"`
	LensVersion     *string         `json:"lens_version"`
	NoteDigest      *string         `json:"note_digest"`
	State           string          `json:"state"`
	Units           *int            `json:"units"`
	Findings        []FindingView   `json:"findings"`
	RulesVersion    *string         `json:"rules_version"`
	Answer          json.RawMessage `json:"answer"`
	RunID           *string         `json:"run_id"`
	DispatchID      *string         `json:"dispatch_id"`
	Model           *string         `json:"model"`
	Effort          *string         `json:"effort"`
	AnsweredModel   *string         `json:"answered_model"`
	Tokens          *store.Tokens   `json:"tokens"`
	Error           *string         `json:"error"`
}

// FindingView is one scanner finding and the text it matched.
type FindingView struct {
	store.Finding
	Span *string `json:"span"`
}

// DecisionView is one gate's decision.
type DecisionView struct {
	Gate        string   `json:"gate"`
	Verdict     string   `json:"verdict"`
	DecidedBy   string   `json:"decided_by"`
	Confidence  *float64 `json:"confidence"`
	Cause       string   `json:"cause"`
	Destination *string  `json:"destination"`
	Note        *string  `json:"note"`
	Lens        *string  `json:"lens"`
	ClientID    *string  `json:"client_id"`
	DecidedTS   string   `json:"decided_ts"`
}

// DeliveryView is one delivery to one destination.
type DeliveryView struct {
	Destination   string  `json:"destination"`
	DestinationID *string `json:"destination_id"`
	Outcome       string  `json:"outcome"`
	Attempts      int     `json:"attempts"`
	LastError     *string `json:"last_error"`
	// UnclaimedCapability is the capability of the delivery's queued task
	// where no worker heard from lately holds it, and null otherwise.
	UnclaimedCapability *string `json:"unclaimed_capability"`
}

// ParseAnswer is what GET /api/v1/items/{key}/parse answers:
// schemas/item-parse.schema.json. Text is the units' text in order, joined by
// a blank line.
type ParseAnswer struct {
	SchemaVersion    int            `json:"schema_version"`
	Key              string         `json:"key"`
	Zone             store.Zone     `json:"zone"`
	Title            *string        `json:"title"`
	URL              *string        `json:"url"`
	MediaType        string         `json:"media_type"`
	Extractor        string         `json:"extractor"`
	ExtractorVersion string         `json:"extractor_version"`
	Text             string         `json:"text"`
	Units            []store.Unit   `json:"units"`
	RulesVersion     *string        `json:"rules_version"`
	Findings         []ParseFinding `json:"findings"`
}

// ParseFinding is one finding of the newest scan of the parse, with the text
// it matched. Its offset counts bytes from its unit's start.
type ParseFinding struct {
	store.Finding
	Span string `json:"span"`
}

// ItemRoutes are the routes for a person or a service holding
// ingestion.items.read to read items. A key in the path is one escaped
// segment, such as reddit%2Ft3_aq7ggd, and a URL there resolves to its key.
// The parse route answers the item's newest done parse, or the newest of the
// extractor and at the version that its query names.
func ItemRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := itemHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle(RouteItems, h.list)
		r.Handle(RouteItem, h.show)
		r.Handle(RouteItemParse, h.parse)
	}
}

type itemHandlers struct {
	store *store.Store
	log   *slog.Logger
}

func (h itemHandlers) list(w http.ResponseWriter, r *http.Request) {
	page := store.ItemPage{Limit: DefaultItemsListed, After: r.URL.Query().Get("after")}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "limit is a whole number from 1, not %q", raw)
			return
		}
		page.Limit = min(n, MaxItemsListed)
	}
	records, next, err := h.store.Items(r.Context(), page)
	if errors.Is(err, store.ErrBadCursor) {
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "after is the next cursor a list answered, not %q", page.After)
		return
	}
	if err != nil {
		h.internal(w, r, "list items", err)
		return
	}
	answer := ItemsAnswer{SchemaVersion: 1, Items: make([]ItemListed, 0, len(records))}
	if next != "" {
		answer.Next = &next
	}
	for _, rec := range records {
		listed := ItemListed{
			Key: rec.Item.Key, Zone: rec.Item.Zone, MediaType: rec.Item.MediaType, Title: rec.Item.Title, URL: rec.Item.URL,
			Connectors: []string{}, Phase: rec.Phase(), WaitingAt: optional(rec.WaitingAt()), CreatedTS: rec.Item.CreatedTS,
		}
		for _, s := range rec.Sightings {
			listed.Connectors = append(listed.Connectors, s.Connector)
		}
		if side, ok := rec.Side(); ok {
			listed.Side = &side
		}
		answer.Items = append(answer.Items, listed)
	}
	JSON(w, http.StatusOK, answer)
}

func (h itemHandlers) show(w http.ResponseWriter, r *http.Request) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
		return
	}
	rec, err := h.store.Item(r.Context(), key)
	if errors.Is(err, store.ErrNoItem) {
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
		return
	}
	if err != nil {
		h.internal(w, r, "read an item", err)
		return
	}
	answer := itemAnswer(rec)
	if answer.Undelivered, err = h.undelivered(r.Context(), rec); err != nil {
		h.internal(w, r, "read the set in force", err)
		return
	}
	claimed, err := h.claimed(r.Context())
	if err != nil {
		h.internal(w, r, "read the workers", err)
		return
	}
	markUnclaimed(rec, answer.Deliveries, claimed)
	answer.Waiting = waitingView(rec, claimed)
	JSON(w, http.StatusOK, answer)
}

// claimed is each capability that a worker heard from within
// WorkersHeardWithin holds.
func (h itemHandlers) claimed(ctx context.Context) (map[string]bool, error) {
	workers, _, err := h.store.Workers(ctx, store.Stamp(h.store.Now().Add(-WorkersHeardWithin)))
	if err != nil {
		return nil, err
	}
	held := map[string]bool{}
	for _, w := range workers {
		for _, c := range w.Capabilities {
			held[c] = true
		}
	}
	return held, nil
}

// waitingView is why rec's item waits at the model stage that holds it, and
// nil where none does.
func waitingView(rec store.ItemRecords, claimed map[string]bool) *WaitingView {
	stage := rec.WaitingAt()
	if stage == "" {
		return nil
	}
	v := &WaitingView{Stage: stage}
	for _, t := range rec.Tasks {
		if t.Stage == stage && (t.State == store.TaskQueued || t.State == store.TaskLeased) {
			v.Capability, v.HeldBy, v.LastError = t.Capability, t.HeldBy, t.Error
		}
	}
	var dispatched *store.Output
	for i, o := range rec.Outputs {
		if o.Stage == stage && o.State == store.OutputWaiting && o.DispatchID != nil {
			dispatched = &rec.Outputs[i]
		}
	}
	switch {
	case v.HeldBy != nil:
		v.Reason = WaitingFirstFill
		v.Message = fmt.Sprintf("the first fill of connector %s holds it until `ingestion connectors release %s`", *v.HeldBy, *v.HeldBy)
	case dispatched != nil:
		v.Reason, v.DispatchID = WaitingDispatched, dispatched.DispatchID
		v.Message = fmt.Sprintf("dispatch %s has not answered", *dispatched.DispatchID)
		if dispatched.Error != nil {
			v.Message += ": " + *dispatched.Error
		}
	case v.Capability != "" && !claimed[v.Capability]:
		v.Reason = WaitingUnclaimed
		v.Message = fmt.Sprintf("no worker heard from lately claims %s tasks. A worker claims them once its config names a [model] adapter", v.Capability)
	default:
		v.Reason = WaitingQueued
		v.Message = "a worker claims its task next"
	}
	if v.LastError != nil {
		v.Message += ". Its last try did not go through: " + *v.LastError
	}
	return v
}

// undelivered is why rec's item reaches no deliverer where route.Due sends it
// somewhere under the set in force, and nil where it does.
func (h itemHandlers) undelivered(ctx context.Context, rec store.ItemRecords) (*UndeliveredView, error) {
	set, err := connectors.AppliedSet(ctx, h.store)
	if err != nil {
		return nil, err
	}
	side, _ := rec.NewestDecision(store.GateSide)
	read, _ := rec.NewestDecision(store.GateRead)
	due, _, err := route.Due(ctx, h.store, set, rec.Item, side, read)
	if err != nil {
		return nil, err
	}
	u := due.Undelivered
	if u == nil {
		return nil, nil
	}
	return &UndeliveredView{Reason: u.Reason, Side: u.Side, Destination: optional(u.Destination), Message: u.Error()}, nil
}

// markUnclaimed names, on each of views whose delivery waits on a queued task,
// the task's capability where it is not claimed. So a delivery that nothing
// here can write reads apart from one that its destination failed.
func markUnclaimed(rec store.ItemRecords, views []DeliveryView, claimed map[string]bool) {
	waiting := map[string]string{}
	for _, t := range rec.Tasks {
		if t.Stage == store.StageDeliver && t.State == store.TaskQueued && t.Destination != nil {
			waiting[*t.Destination] = t.Capability
		}
	}
	for i, v := range views {
		if capability, ok := waiting[v.Destination]; ok && !claimed[capability] {
			views[i].UnclaimedCapability = &capability
		}
	}
}

func (h itemHandlers) parse(w http.ResponseWriter, r *http.Request) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
		return
	}
	pick := store.ParsePick{Extractor: r.URL.Query().Get("extractor"), Version: r.URL.Query().Get("version")}
	p, err := h.store.Parse(r.Context(), key, pick)
	switch {
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
		return
	case errors.Is(err, store.ErrNoParse):
		Refuse(w, http.StatusNotFound, CodeParseNotFound, "%s holds no done parse%s", key, pickWords(pick))
		return
	case err != nil:
		h.internal(w, r, "read a parse", err)
		return
	}
	JSON(w, http.StatusOK, parseAnswer(p))
}

// parseAnswer is p as the parse routes answer it, with the findings of the
// newest scan that read it.
func parseAnswer(p store.Parse) ParseAnswer {
	answer := ParseAnswer{
		SchemaVersion: 1, Key: p.Item.Key, Zone: p.Item.Zone, Title: p.Item.Title, URL: p.Item.URL, MediaType: p.Item.MediaType,
		Extractor: p.Output.Producer, ExtractorVersion: p.Output.ProducerVersion, Text: p.Text(), Units: p.Units,
		Findings: []ParseFinding{},
	}
	if answer.Units == nil {
		answer.Units = []store.Unit{}
	}
	if p.Scan != nil {
		rules := store.ScanRules(p.Scan.ProducerVersion)
		answer.RulesVersion = &rules
		for i, f := range p.Scan.Findings {
			finding := ParseFinding{Finding: f}
			if i < len(p.Scan.Spans) {
				finding.Span = p.Scan.Spans[i].Span
			}
			answer.Findings = append(answer.Findings, finding)
		}
	}
	return answer
}

// pickWords is the extractor and version that pick narrows to, as a phrase.
func pickWords(pick store.ParsePick) string {
	out := ""
	if pick.Extractor != "" {
		out += " from " + pick.Extractor
	}
	if pick.Version != "" {
		out += " at version " + pick.Version
	}
	return out
}

func (h itemHandlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.log.ErrorContext(r.Context(), what, "error", err, "path", r.URL.Path)
	Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not %s", what)
}

func itemAnswer(rec store.ItemRecords) ItemAnswer {
	it := rec.Item
	a := ItemAnswer{
		SchemaVersion: 1,
		Item: ItemView{
			Key: it.Key, Zone: it.Zone, MediaType: it.MediaType, Title: it.Title, URL: it.URL, SourceTS: it.SourceTS, BlobSHA256: it.BlobSHA256,
			ParentKey: it.ParentKey, FlowID: it.FlowID, Phase: rec.Phase(), WaitingAt: optional(rec.WaitingAt()), CreatedTS: it.CreatedTS,
		},
		Homes: []HomeView{}, Sightings: []SightingView{}, Outputs: []OutputView{}, Decisions: []DecisionView{}, Deliveries: []DeliveryView{},
		Sections: []SectionView{}, Progress: rec.Progress(),
	}
	if rec.Blob != nil {
		a.Item.SizeBytes, a.Item.MD5 = &rec.Blob.SizeBytes, &rec.Blob.MD5
	}
	for _, h := range rec.Homes {
		a.Homes = append(a.Homes, HomeView{Root: h.Root, Path: h.Path, ModifiedTS: h.ModifiedTS, FoundTS: h.FoundTS, GoneTS: h.GoneTS})
	}
	for _, s := range rec.Sightings {
		a.Sightings = append(a.Sightings, SightingView{
			Connector: s.Connector, ExternalKey: s.ExternalKey, Tags: s.Tags, Fields: s.Fields, Metrics: s.Metrics, Zone: s.Zone,
			CursorPosition: s.CursorPosition, FirstSeenTS: s.FirstSeenTS, LastSeenTS: s.LastSeenTS, MetricsReadTS: s.MetricsReadTS,
		})
	}
	for _, o := range rec.Outputs {
		a.Outputs = append(a.Outputs, outputView(rec, o))
	}
	if quality := store.NewestDone(rec.Outputs, store.StageQuality); quality != nil && len(quality.Answer) > 0 {
		a.QualityAnswer = quality.Answer
	}
	if read := store.NewestDone(rec.Outputs, store.StageModelRead); read != nil {
		view := outputView(rec, *read)
		a.ModelRead = &view
	}
	for _, d := range rec.Decisions {
		a.Decisions = append(a.Decisions, decisionView(d))
	}
	for _, d := range rec.Deliveries {
		a.Deliveries = append(a.Deliveries, DeliveryView{
			Destination: d.Destination, DestinationID: d.DestinationID, Outcome: d.Outcome, Attempts: d.Attempts, LastError: d.LastError,
		})
	}
	for _, s := range rec.Sections {
		a.Sections = append(a.Sections, SectionView{
			Producer: s.Producer, ProducerVersion: s.ProducerVersion, N: s.N, ParentN: s.ParentN, Level: s.Level, Kind: s.Kind, Title: s.Title,
			PDFPageStart: s.PDFPageStart, PDFPageEnd: s.PDFPageEnd, PrintedPageStart: s.PrintedPageStart, PrintedPageEnd: s.PrintedPageEnd,
			StartPlacedBy: s.StartPlacedBy, ChildKey: s.ChildKey,
		})
	}
	a.Suggestion = suggestion(rec)
	return a
}

// outputView is o as the item route lists it, with a parse's units counted
// and each finding beside its span.
func outputView(rec store.ItemRecords, o store.Output) OutputView {
	view := OutputView{
		Stage: o.Stage, Producer: o.Producer, ProducerVersion: o.ProducerVersion, Lens: o.Lens, LensVersion: o.LensVersion,
		NoteDigest: o.NoteDigest, State: o.State, Answer: o.Answer, RunID: o.RunID, DispatchID: o.DispatchID, Model: o.Model, Effort: o.Effort,
		AnsweredModel: o.AnsweredModel, Tokens: o.Tokens, Error: o.Error, RulesVersion: o.RulesVersion,
	}
	if n, ok := rec.UnitCount(o); ok {
		view.Units = &n
	}
	if o.Findings != nil {
		view.Findings = []FindingView{}
	}
	for i, f := range o.Findings {
		finding := FindingView{Finding: f}
		if i < len(o.Spans) {
			finding.Span = &o.Spans[i].Span
		}
		view.Findings = append(view.Findings, finding)
	}
	return view
}

// SectionView is one entry of an item's sections, as the sections table holds
// it.
type SectionView struct {
	Producer         string  `json:"producer"`
	ProducerVersion  string  `json:"producer_version"`
	N                int     `json:"n"`
	ParentN          *int    `json:"parent_n"`
	Level            int     `json:"level"`
	Kind             string  `json:"kind"`
	Title            *string `json:"title"`
	PDFPageStart     int     `json:"pdf_page_start"`
	PDFPageEnd       int     `json:"pdf_page_end"`
	PrintedPageStart *string `json:"printed_page_start"`
	PrintedPageEnd   *string `json:"printed_page_end"`
	StartPlacedBy    string  `json:"start_placed_by"`
	ChildKey         *string `json:"child_key"`
}

func decisionView(d store.Decision) DecisionView {
	return DecisionView{
		Gate: d.Gate, Verdict: d.Verdict, DecidedBy: d.DecidedBy, Confidence: d.Confidence, Cause: d.Cause,
		Destination: d.Destination, Note: d.Note, Lens: d.Lens, ClientID: d.ClientID, DecidedTS: d.DecidedTS,
	}
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
