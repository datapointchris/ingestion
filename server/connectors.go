package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"slices"
	"strings"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// Scopes of the connector routes.
const (
	// ScopeDeclarationsApply admits a caller to apply a set of declarations.
	ScopeDeclarationsApply = "ingestion.declarations.apply"
	// ScopeConnectorsRead admits a caller to read each connector's state.
	ScopeConnectorsRead = "ingestion.connectors.read"
	// ScopeConnectorsRun admits a caller to run a connector now.
	ScopeConnectorsRun = "ingestion.connectors.run"
	// ScopeFirstFillsRelease admits a caller to release a connector's first
	// fill, which sends every item it held on to the model stages.
	ScopeFirstFillsRelease = "ingestion.first-fills.release"
	// ScopeItemsDrop admits a caller to drop a file into a drop connector.
	ScopeItemsDrop = "ingestion.items.drop"
)

// IdempotencyKey is the header that names a keyed request across its
// retries. A retry under one key answers what the first attempt answered, and
// a different request under it is refused.
const IdempotencyKey = "Idempotency-Key"

// MaxDropBytes bounds one dropped file. A folder connector takes larger
// files, such as books, by reference.
const MaxDropBytes = 256 << 20

// Refusal codes of the connector routes.
const (
	CodeConnectorNotFound     = "connector_not_found"
	CodeConnectorMode         = "connector_mode"
	CodeInvalidDeclarations   = "invalid_declarations"
	CodeDeclarationsMoved     = "declarations_moved"
	CodeIdempotencyKeyMissing = "idempotency_key_missing"
	CodeIdempotencyKeyReused  = "idempotency_key_reused"
	CodeDropTooLarge          = "drop_too_large"
	CodeInvalidDrop           = "invalid_drop"
)

// DeclarationsRequest is what PUT /api/v1/declarations reads:
// schemas/declarations.schema.json.
type DeclarationsRequest struct {
	Commit string `json:"commit"`
	// Replaces is the commit the caller saw in force, and null where it saw
	// none. An apply that replaces another commit is refused.
	Replaces *string `json:"replaces"`
	connectors.Set
}

// DeclarationsAnswer is what an apply answers:
// schemas/declarations-applied.schema.json.
type DeclarationsAnswer struct {
	SchemaVersion int    `json:"schema_version"`
	Commit        string `json:"commit"`
	AppliedTS     string `json:"applied_ts"`
	AppliedBy     string `json:"applied_by"`
	Unchanged     bool   `json:"unchanged"`
	Connectors    int    `json:"connectors"`
	RoutingRules  int    `json:"routing_rules"`
	Destinations  int    `json:"destinations"`
}

// invalidDeclarations is the refusal of a set holding invalid declarations,
// naming each.
type invalidDeclarations struct {
	refusal
	Invalid []connectors.Invalid `json:"invalid"`
}

// declarationsMoved is the refusal of an apply built against a set that is no
// longer in force, naming the commit in force and the one it replaces.
type declarationsMoved struct {
	refusal
	Current  *string `json:"applied_commit"`
	Replaces *string `json:"replaces"`
}

// ConnectorsAnswer is what GET /api/v1/connectors answers:
// schemas/connectors-list.schema.json.
type ConnectorsAnswer struct {
	SchemaVersion int `json:"schema_version"`
	// AppliedCommit is the commit of the set in force, and null where no set
	// has been applied.
	AppliedCommit *string         `json:"applied_commit"`
	Connectors    []ConnectorView `json:"connectors"`
}

// ConnectorView is one applied connector, its counts and its due time.
type ConnectorView struct {
	Name           string     `json:"name"`
	Mode           string     `json:"mode"`
	Zone           store.Zone `json:"zone"`
	Source         string     `json:"source"`
	Tags           []string   `json:"tags"`
	AppliedCommit  string     `json:"applied_commit"`
	LastRunTS      *string    `json:"last_run_ts"`
	LastDeliveryTS *string    `json:"last_delivery_ts"`
	// LastError is why the last run failed, null since one succeeded, and
	// Failures counts the failed runs since then.
	LastError   *string `json:"last_error"`
	LastErrorTS *string `json:"last_error_ts"`
	Failures    int     `json:"failures"`
	Items       int     `json:"items"`
	Waiting     int     `json:"waiting"`
	Review      int     `json:"review"`
	Failed      int     `json:"failed"`
	Schedule    *string `json:"schedule"`
	DueInterval *string `json:"due_interval"`
	Freshness   *string `json:"freshness"`
	DueAt       string  `json:"due_at"`
	Overdue     bool    `json:"overdue"`
	// FirstFill is answered on one connector alone.
	FirstFill *FirstFillView `json:"first_fill,omitempty"`
}

// FirstFillView is a connector's first fill: its state, the one item it ran
// end to end, the items held behind it, and that item's tokens by model stage.
type FirstFillView struct {
	State   string                  `json:"state"`
	ItemKey *string                 `json:"item_key"`
	Held    int                     `json:"held"`
	Tokens  map[string]store.Tokens `json:"tokens"`
}

// RunAnswer is what POST /api/v1/connectors/{name}/runs answers:
// schemas/connector-run.schema.json. A poll answers its task, and a folder
// the due time that the scheduler scans it at.
type RunAnswer struct {
	SchemaVersion int     `json:"schema_version"`
	Connector     string  `json:"connector"`
	Mode          string  `json:"mode"`
	TaskID        *string `json:"task_id"`
	DueAt         string  `json:"due_at"`
}

// FirstFillRequest is what PATCH /api/v1/connectors/{name}/first-fill reads.
// Released is the one state a person sets.
type FirstFillRequest struct {
	State string `json:"state"`
}

// FirstFillAnswer is what the PATCH answers: schemas/first-fill.schema.json.
type FirstFillAnswer struct {
	SchemaVersion int    `json:"schema_version"`
	Connector     string `json:"connector"`
	State         string `json:"state"`
	// Released counts the held tasks that this request returned to the
	// claims, which is 0 for a fill already released.
	Released int `json:"released"`
}

// PushAnswer is what a push or a drop answers: schemas/push.schema.json.
type PushAnswer struct {
	SchemaVersion int          `json:"schema_version"`
	Items         []PushedItem `json:"items"`
}

// PushedItem is one item a push or a drop landed.
type PushedItem struct {
	Key  string     `json:"key"`
	Zone store.Zone `json:"zone"`
	// Created is false where the item was already landed, and this was one
	// more sighting of it.
	Created bool `json:"created"`
}

// ConnectorConfig is what the connector routes serve from.
type ConnectorConfig struct {
	Store     *store.Store
	Lander    *landing.Lander
	Scheduler *connectors.Scheduler
	// Roots names each [serve.homes] root a folder connector may read.
	Roots []string
	Log   *slog.Logger
}

// ConnectorRoutes are the routes for a person to apply the declarations, read
// and run each connector, release a first fill, and drop a file.
func ConnectorRoutes(cfg ConnectorConfig) Routes {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Roots == nil {
		cfg.Roots = []string{}
	}
	h := connectorHandlers{cfg: cfg}
	return func(r *Router) {
		r.Handle(RouteDeclarations, h.apply)
		r.Handle(RouteConnectors, h.list)
		r.Handle(RouteConnector, h.show)
		r.Handle(RouteConnectorRuns, h.run)
		r.Handle(RouteFirstFill, h.firstFill)
		r.Handle(RouteConnectorDrops, h.drop)
	}
}

type connectorHandlers struct{ cfg ConnectorConfig }

func (h connectorHandlers) apply(w http.ResponseWriter, r *http.Request) {
	var body DeclarationsRequest
	if !decode(w, r, &body) {
		return
	}
	if body.Commit == "" || len(body.Commit) > 200 {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "an apply names the commit its declarations come from, in at most 200 characters")
		return
	}
	set := body.Set
	if set.Connectors == nil {
		set.Connectors = []connectors.Connector{}
	}
	applied, err := connectors.Apply(r.Context(), h.cfg.Store, set, body.Commit, body.Replaces, loginOf(r), h.cfg.Roots)
	var moved *connectors.MovedError
	if errors.As(err, &moved) {
		status := http.StatusConflict
		JSON(w, status, declarationsMoved{
			refusal: refusal{
				Error: http.StatusText(status), Code: CodeDeclarationsMoved,
				Message: "the apply replaces " + orNone(moved.Replaces) + ", and " + orNone(moved.Current) +
					" is in force, so nothing is applied",
			},
			Current: moved.Current, Replaces: moved.Replaces,
		})
		return
	}
	var invalid *connectors.InvalidError
	if errors.As(err, &invalid) {
		status := http.StatusUnprocessableEntity
		JSON(w, status, invalidDeclarations{
			refusal: refusal{
				Error: http.StatusText(status), Code: CodeInvalidDeclarations,
				Message: "the set holds invalid declarations, so none of it is applied and the set in force stays as it was",
			},
			Invalid: invalid.Invalid,
		})
		return
	}
	if err != nil {
		h.internal(w, r, "apply the declarations", err)
		return
	}
	if !applied.Unchanged {
		h.cfg.Log.Info("applied declarations", "commit", applied.Commit, "by", applied.AppliedBy,
			"connectors", len(set.Connectors), "routing_rules", len(set.RoutingRules), "destinations", len(set.Destinations))
		if h.cfg.Scheduler != nil {
			h.cfg.Scheduler.Wake()
		}
	}
	JSON(w, http.StatusOK, DeclarationsAnswer{
		SchemaVersion: 1, Commit: applied.Commit, AppliedTS: applied.AppliedTS, AppliedBy: applied.AppliedBy,
		Unchanged: applied.Unchanged, Connectors: len(set.Connectors), RoutingRules: len(set.RoutingRules),
		Destinations: len(set.Destinations),
	})
}

// applied reads the set in force and its commit, which is empty where no set
// has been applied.
func (h connectorHandlers) applied(w http.ResponseWriter, r *http.Request) (connectors.Set, string, bool) {
	declared, ok, err := h.cfg.Store.AppliedDeclarations(r.Context())
	if err != nil {
		h.internal(w, r, "read the declarations in force", err)
		return connectors.Set{}, "", false
	}
	if !ok {
		return connectors.Set{}, "", true
	}
	set, err := connectors.Decode(declared)
	if err != nil {
		h.internal(w, r, "read the declarations in force", err)
		return connectors.Set{}, "", false
	}
	return set, declared.Commit, true
}

// connector reads the applied connector the path names, and refuses with 404
// where the set in force declares none.
func (h connectorHandlers) connector(w http.ResponseWriter, r *http.Request) (connectors.Connector, string, bool) {
	set, commit, ok := h.applied(w, r)
	if !ok {
		return connectors.Connector{}, "", false
	}
	c, found := set.Connector(r.PathValue("name"))
	if !found {
		Refuse(w, http.StatusNotFound, CodeConnectorNotFound, "the declarations in force declare no connector %s", r.PathValue("name"))
		return c, "", false
	}
	return c, commit, true
}

func (h connectorHandlers) list(w http.ResponseWriter, r *http.Request) {
	set, commit, ok := h.applied(w, r)
	if !ok {
		return
	}
	counts, err := h.cfg.Store.ConnectorCounts(r.Context())
	if err != nil {
		h.internal(w, r, "count each connector's items", err)
		return
	}
	answer := ConnectorsAnswer{SchemaVersion: 1, Connectors: make([]ConnectorView, 0, len(set.Connectors))}
	if commit != "" {
		answer.AppliedCommit = &commit
	}
	sorted := slices.Clone(set.Connectors)
	slices.SortFunc(sorted, func(a, b connectors.Connector) int { return strings.Compare(a.Name, b.Name) })
	for _, c := range sorted {
		view, ok := h.view(w, r, c, commit, counts[c.Name])
		if !ok {
			return
		}
		answer.Connectors = append(answer.Connectors, view)
	}
	JSON(w, http.StatusOK, answer)
}

func (h connectorHandlers) show(w http.ResponseWriter, r *http.Request) {
	c, commit, ok := h.connector(w, r)
	if !ok {
		return
	}
	counts, err := h.cfg.Store.ConnectorCounts(r.Context())
	if err != nil {
		h.internal(w, r, "count the connector's items", err)
		return
	}
	view, ok := h.view(w, r, c, commit, counts[c.Name])
	if !ok {
		return
	}
	cur, _, err := h.cfg.Store.Cursor(r.Context(), c.Name)
	if err != nil {
		h.internal(w, r, "read the connector's cursor", err)
		return
	}
	fill := &FirstFillView{State: cur.FirstFill, ItemKey: cur.FirstFillItemKey, Held: counts[c.Name].Held, Tokens: map[string]store.Tokens{}}
	if cur.FirstFillItemKey != nil {
		outputs, err := h.cfg.Store.Outputs(r.Context(), *cur.FirstFillItemKey)
		if err != nil {
			h.internal(w, r, "read the first fill's outputs", err)
			return
		}
		for _, o := range outputs {
			if o.Tokens == nil {
				continue
			}
			sum := fill.Tokens[o.Stage]
			sum.UncachedInput += o.Tokens.UncachedInput
			sum.CacheWrite += o.Tokens.CacheWrite
			sum.CacheRead += o.Tokens.CacheRead
			sum.Output += o.Tokens.Output
			fill.Tokens[o.Stage] = sum
		}
	}
	view.FirstFill = fill
	JSON(w, http.StatusOK, view)
}

func (h connectorHandlers) view(w http.ResponseWriter, r *http.Request, c connectors.Connector, commit string, n store.ConnectorCounts) (ConnectorView, bool) {
	cur, ok, err := h.cfg.Store.Cursor(r.Context(), c.Name)
	if err != nil {
		h.internal(w, r, "read a connector's cursor", err)
		return ConnectorView{}, false
	}
	if !ok {
		h.internal(w, r, "read a connector's cursor", errors.New("connector "+c.Name+" is applied and holds no cursor"))
		return ConnectorView{}, false
	}
	tags := c.Tags
	if tags == nil {
		tags = []string{}
	}
	return ConnectorView{
		Name: c.Name, Mode: c.Mode, Zone: c.Zone, Source: c.Source, Tags: tags, AppliedCommit: commit,
		LastRunTS: cur.LastRunTS, LastDeliveryTS: cur.LastDeliveryTS,
		LastError: cur.LastError, LastErrorTS: cur.LastErrorTS, Failures: cur.Failures,
		Items: n.Items, Waiting: n.Waiting, Review: n.Review, Failed: n.Failed,
		Schedule: optional(c.Schedule), DueInterval: optional(c.DueInterval), Freshness: optional(c.Freshness),
		DueAt: cur.DueAt, Overdue: connectors.Overdue(c, cur, h.cfg.Store.Now()),
	}, true
}

// run makes a poll connector's task now, or makes a folder connector due now
// for the scheduler to scan. It is keyed, so a retry makes no second run.
func (h connectorHandlers) run(w http.ResponseWriter, r *http.Request) {
	key := requestKey(r)
	c, _, ok := h.connector(w, r)
	if !ok {
		return
	}
	if !c.Scheduled() {
		Refuse(w, http.StatusConflict, CodeConnectorMode,
			"%s is a %s connector, which runs as its items arrive, so nothing runs it", c.Name, c.Mode)
		return
	}
	caller, fingerprint := callerOf(r), fingerprintOf("runs", c.Name)
	var replay *store.KeyedRequest
	var answer RunAnswer
	status := http.StatusCreated
	err := h.cfg.Store.Update(r.Context(), func(tx *store.Tx) error {
		prior, found, err := tx.KeyedRequest(r.Context(), caller, key)
		if err != nil || found {
			replay = &prior
			return err
		}
		answer = RunAnswer{SchemaVersion: 1, Connector: c.Name, Mode: c.Mode}
		if c.Mode == connectors.ModePoll {
			task, made, err := connectors.Poll(r.Context(), tx, c)
			if err != nil {
				return err
			}
			if !made {
				status = http.StatusOK
			}
			answer.TaskID = &task.ID
			cur, _, err := tx.Cursor(r.Context(), c.Name)
			if err != nil {
				return err
			}
			answer.DueAt = cur.DueAt
		} else {
			due, err := connectors.DueNow(r.Context(), tx, c)
			if err != nil {
				return err
			}
			answer.DueAt, status = store.Stamp(due), http.StatusAccepted
		}
		return h.keep(r, tx, caller, key, "runs", c.Name, fingerprint, status, answer)
	})
	if err != nil {
		h.internal(w, r, "run the connector", err)
		return
	}
	if replay != nil {
		h.answerReplay(w, *replay, fingerprint)
		return
	}
	if c.Mode == connectors.ModeFolder && h.cfg.Scheduler != nil {
		h.cfg.Scheduler.Wake()
	}
	h.cfg.Log.Info("ran a connector", "connector", c.Name, "mode", c.Mode, "task", text(answer.TaskID), "due_at", answer.DueAt)
	JSON(w, status, answer)
}

// firstFill releases a connector's first fill, which lets every item it holds
// through the model stages. It finds the fill by the connector's cursor, so a
// connector that a later set dropped can still be released. Releasing a fill
// already released returns no task.
func (h connectorHandlers) firstFill(w http.ResponseWriter, r *http.Request) {
	var body FirstFillRequest
	if !decode(w, r, &body) {
		return
	}
	if body.State != store.FirstFillReleased {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a first fill's state is set to %s alone, not %q", store.FirstFillReleased, body.State)
		return
	}
	name := r.PathValue("name")
	var found bool
	var released int
	err := h.cfg.Store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		if _, found, err = tx.Cursor(r.Context(), name); err != nil || !found {
			return err
		}
		released, err = tx.ReleaseFirstFill(r.Context(), name)
		return err
	})
	if err != nil {
		h.internal(w, r, "release the first fill", err)
		return
	}
	if !found {
		Refuse(w, http.StatusNotFound, CodeConnectorNotFound, "no set applied has declared a connector %s", name)
		return
	}
	h.cfg.Log.Info("released a first fill", "connector", name, "tasks", released)
	JSON(w, http.StatusOK, FirstFillAnswer{SchemaVersion: 1, Connector: name, State: store.FirstFillReleased, Released: released})
}

// drop lands the body as one file dropped into a drop connector, through its
// source's drop handler. The body's Content-Type is the file's media type, and
// its Content-Disposition names the file. It is keyed, as Stripe's API keys a
// request: a retry under one Idempotency-Key answers what the first attempt
// landed, and a different file under it answers 409.
func (h connectorHandlers) drop(w http.ResponseWriter, r *http.Request) {
	key := requestKey(r)
	c, _, ok := h.connector(w, r)
	if !ok {
		return
	}
	if c.Mode != connectors.ModeDrop {
		Refuse(w, http.StatusConflict, CodeConnectorMode, "%s is a %s connector, so it takes no dropped file", c.Name, c.Mode)
		return
	}
	mediaType, name, ok := dropHeaders(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxDropBytes))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		Refuse(w, http.StatusRequestEntityTooLarge, CodeDropTooLarge,
			"a dropped file holds at most %d bytes; a folder connector takes larger files by reference", MaxDropBytes)
		return
	}
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the dropped file could not be read: %v", err)
		return
	}
	sum := sha256.Sum256(body)
	caller := callerOf(r)
	fingerprint := fingerprintOf("drops", c.Name, name, mediaType, hex.EncodeToString(sum[:]))
	prior, found, err := h.cfg.Store.KeyedRequest(r.Context(), caller, key)
	if err != nil {
		h.internal(w, r, "read the drop's key", err)
		return
	}
	if found {
		h.answerReplay(w, prior, fingerprint)
		return
	}
	landed, err := connectors.LandDrop(r.Context(), h.cfg.Lander, connectors.Drop{Connector: c, Name: name, MediaType: mediaType, Body: body})
	if err != nil {
		h.cfg.Log.Warn("a drop did not land", "connector", c.Name, "error", err)
		Refuse(w, http.StatusUnprocessableEntity, CodeInvalidDrop, "%s did not land the file: %v", c.Name, err)
		return
	}
	keys := make([]string, len(landed))
	for i, l := range landed {
		keys[i] = l.Key
	}
	zones, err := h.cfg.Store.ItemZones(r.Context(), keys)
	if err != nil {
		h.internal(w, r, "read the landed items", err)
		return
	}
	answer := PushAnswer{SchemaVersion: 1, Items: make([]PushedItem, len(landed))}
	for i, l := range landed {
		answer.Items[i] = PushedItem{Key: l.Key, Zone: zones[l.Key], Created: l.ItemAdded}
	}
	var replay *store.KeyedRequest
	err = h.cfg.Store.Update(r.Context(), func(tx *store.Tx) error {
		prior, found, err := tx.KeyedRequest(r.Context(), caller, key)
		if err != nil || found {
			replay = &prior
			return err
		}
		if err := h.keep(r, tx, caller, key, "drops", c.Name, fingerprint, http.StatusCreated, answer); err != nil {
			return err
		}
		return connectors.Delivered(r.Context(), tx, c)
	})
	if err != nil {
		h.internal(w, r, "record the drop", err)
		return
	}
	if replay != nil {
		h.answerReplay(w, *replay, fingerprint)
		return
	}
	h.cfg.Log.Info("landed a drop", "connector", c.Name, "items", len(landed), "sha256", hex.EncodeToString(sum[:]))
	JSON(w, http.StatusCreated, answer)
}

// keep records a keyed request's answer in tx, beside the write it answers.
func (h connectorHandlers) keep(r *http.Request, tx *store.Tx, caller, key, route, connector, fingerprint string, status int, answer any) error {
	raw, err := json.Marshal(answer)
	if err != nil {
		return err
	}
	return tx.AddKeyedRequest(r.Context(), store.KeyedRequest{
		Caller: caller, IdempotencyKey: key, Route: route, Connector: &connector, Fingerprint: fingerprint, Status: status, Answer: raw,
	})
}

// answerReplay answers a request repeated under a key: 200 with what the
// first answered, where the two match, and 409 where they differ.
func (h connectorHandlers) answerReplay(w http.ResponseWriter, prior store.KeyedRequest, fingerprint string) {
	if prior.Fingerprint != fingerprint {
		Refuse(w, http.StatusConflict, CodeIdempotencyKeyReused,
			"this %s already sent a different request to %s, so nothing was done", IdempotencyKey, prior.Route)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(prior.Answer, '\n'))
}

func (h connectorHandlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.cfg.Log.ErrorContext(r.Context(), what, "error", err, "path", r.URL.Path)
	Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not %s", what)
}

// dropHeaders reads a drop's media type, octet-stream where none is named,
// and its file's name, which is one filename where it is given.
func dropHeaders(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	mediaType := "application/octet-stream"
	if raw := r.Header.Get("Content-Type"); raw != "" {
		parsed, params, err := mime.ParseMediaType(raw)
		if err != nil {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the Content-Type %q is not a media type: %v", raw, err)
			return "", "", false
		}
		mediaType = mime.FormatMediaType(parsed, params)
	}
	raw := r.Header.Get("Content-Disposition")
	if raw == "" {
		return mediaType, "", true
	}
	_, params, err := mime.ParseMediaType(raw)
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the Content-Disposition %q does not parse: %v", raw, err)
		return "", "", false
	}
	name := params["filename"]
	if name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\\\x00") {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a dropped file is named by one filename, with no directory, not %q", name)
		return "", "", false
	}
	return mediaType, name, true
}

// callerOf names the caller of a keyed request: its client, and the person
// where its token names one.
func callerOf(r *http.Request) string {
	id, _ := goapiauth.IdentityFrom(r.Context())
	if id.Subject == "" {
		return id.ClientID
	}
	return id.ClientID + " " + id.Subject
}

// loginOf is the login that a decision records: the person's username where
// the token carries it, else the person, else the client.
func loginOf(r *http.Request) string {
	id, _ := goapiauth.IdentityFrom(r.Context())
	switch {
	case id.Username != "":
		return id.Username
	case id.Subject != "":
		return id.Subject
	}
	return id.ClientID
}

// fingerprintOf is the SHA-256 of parts, which tells a request repeated under
// its key from a different one.
func fingerprintOf(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

// orNone is commit, or "no set" for nil.
func orNone(commit *string) string {
	if commit == nil {
		return "no set"
	}
	return *commit
}

// text is *s, and "" for nil.
func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
