package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/datapointchris/ingestion/itemkey"
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
}

// ItemView is the item, with its bytes' size and MD5 and its derived phase.
type ItemView struct {
	Key        string     `json:"key"`
	Zone       store.Zone `json:"zone"`
	MediaType  string     `json:"media_type"`
	Title      *string    `json:"title"`
	URL        *string    `json:"url"`
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
	Connector      string     `json:"connector"`
	ExternalKey    string     `json:"external_key"`
	Tags           []string   `json:"tags"`
	Zone           store.Zone `json:"zone"`
	CursorPosition *string    `json:"cursor_position"`
	FirstSeenTS    string     `json:"first_seen_ts"`
	LastSeenTS     string     `json:"last_seen_ts"`
}

// OutputView is one stage's output, with a parse's units counted and each
// finding beside its span.
type OutputView struct {
	Stage           string          `json:"stage"`
	Producer        string          `json:"producer"`
	ProducerVersion string          `json:"producer_version"`
	Lens            *string         `json:"lens"`
	LensVersion     *string         `json:"lens_version"`
	State           string          `json:"state"`
	Units           *int            `json:"units"`
	Findings        []FindingView   `json:"findings"`
	Answer          json.RawMessage `json:"answer"`
	RunID           *string         `json:"run_id"`
	Tokens          *store.Tokens   `json:"tokens"`
	Error           *string         `json:"error"`
}

// FindingView is one scanner finding and the text it matched.
type FindingView struct {
	Rule     string  `json:"rule"`
	Severity string  `json:"severity"`
	Span     *string `json:"span"`
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
	DecidedTS   string   `json:"decided_ts"`
}

// DeliveryView is one delivery to one destination.
type DeliveryView struct {
	Destination   string  `json:"destination"`
	DestinationID *string `json:"destination_id"`
	Outcome       string  `json:"outcome"`
	Attempts      int     `json:"attempts"`
	LastError     *string `json:"last_error"`
}

// ItemRoutes are the routes for a person or a service holding
// ingestion.items.read to read items. A key in the path is one escaped
// segment, such as reddit%2Ft3_aq7ggd, and a URL there resolves to its key.
func ItemRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := itemHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle("GET /api/v1/items", ScopeItemsRead, h.list)
		r.Handle("GET /api/v1/items/{key}", ScopeItemsRead, h.show)
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
	JSON(w, http.StatusOK, itemAnswer(rec))
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
			Key: it.Key, Zone: it.Zone, MediaType: it.MediaType, Title: it.Title, URL: it.URL, BlobSHA256: it.BlobSHA256,
			ParentKey: it.ParentKey, FlowID: it.FlowID, Phase: rec.Phase(), WaitingAt: optional(rec.WaitingAt()), CreatedTS: it.CreatedTS,
		},
		Homes: []HomeView{}, Sightings: []SightingView{}, Outputs: []OutputView{}, Decisions: []DecisionView{}, Deliveries: []DeliveryView{},
	}
	if rec.Blob != nil {
		a.Item.SizeBytes, a.Item.MD5 = &rec.Blob.SizeBytes, &rec.Blob.MD5
	}
	for _, h := range rec.Homes {
		a.Homes = append(a.Homes, HomeView{Root: h.Root, Path: h.Path, ModifiedTS: h.ModifiedTS, FoundTS: h.FoundTS, GoneTS: h.GoneTS})
	}
	for _, s := range rec.Sightings {
		a.Sightings = append(a.Sightings, SightingView{
			Connector: s.Connector, ExternalKey: s.ExternalKey, Tags: s.Tags, Zone: s.Zone, CursorPosition: s.CursorPosition,
			FirstSeenTS: s.FirstSeenTS, LastSeenTS: s.LastSeenTS,
		})
	}
	for _, o := range rec.Outputs {
		view := OutputView{
			Stage: o.Stage, Producer: o.Producer, ProducerVersion: o.ProducerVersion, Lens: o.Lens, LensVersion: o.LensVersion,
			State: o.State, Answer: o.Answer, RunID: o.RunID, Tokens: o.Tokens, Error: o.Error,
		}
		if o.Units != nil {
			n := len(o.Units)
			view.Units = &n
		}
		for i, f := range o.Findings {
			finding := FindingView{Rule: f.Rule, Severity: f.Severity}
			if i < len(o.Spans) {
				finding.Span = &o.Spans[i].Span
			}
			view.Findings = append(view.Findings, finding)
		}
		a.Outputs = append(a.Outputs, view)
	}
	for _, d := range rec.Decisions {
		a.Decisions = append(a.Decisions, DecisionView{
			Gate: d.Gate, Verdict: d.Verdict, DecidedBy: d.DecidedBy, Confidence: d.Confidence, Cause: d.Cause,
			Destination: d.Destination, Note: d.Note, DecidedTS: d.DecidedTS,
		})
	}
	for _, d := range rec.Deliveries {
		a.Deliveries = append(a.Deliveries, DeliveryView{
			Destination: d.Destination, DestinationID: d.DestinationID, Outcome: d.Outcome, Attempts: d.Attempts, LastError: d.LastError,
		})
	}
	return a
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
