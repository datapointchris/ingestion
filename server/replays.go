package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/store"
)

// ScopeItemsReplay admits a caller to run a stage again over landed items.
const ScopeItemsReplay = "ingestion.items.replay"

// IdempotencyKey is the header that a keyed route finds an earlier request's
// work by, so a retry after a lost answer never starts it twice.
const IdempotencyKey = "Idempotency-Key"

// Refusal codes of a replay.
const (
	CodeIdempotencyKeyMissing = "idempotency_key_missing"
	CodeIdempotencyKeyReused  = "idempotency_key_reused"
	CodeStageNotReplayable    = "stage_not_replayable"
)

// replayStages are the stages that schemas/replay.schema.json names.
var replayStages = []string{store.StageParse, store.StageScan, store.StageQuality, store.StageModelRead, store.StageDeliver}

// ReplayRequest asks to run Stage again over the items that Connector and Key
// select, each absent selecting every item. Confirm runs a model stage once a
// person has read its estimate.
type ReplayRequest struct {
	Stage     string  `json:"stage"`
	Connector *string `json:"connector"`
	Key       *string `json:"key"`
	Confirm   bool    `json:"confirm"`
}

// ReplayAnswer is what POST /api/v1/replays answers:
// schemas/replay.schema.json.
type ReplayAnswer struct {
	SchemaVersion int           `json:"schema_version"`
	Stage         string        `json:"stage"`
	Connector     *string       `json:"connector"`
	Key           *string       `json:"key"`
	Items         int           `json:"items"`
	Queued        bool          `json:"queued"`
	Estimate      *store.Tokens `json:"estimate"`
}

// Replayable is one stage that a replay runs again.
type Replayable struct {
	// Capability is the capability of the stage's task on item, and false
	// where the stage does not run on item.
	Capability func(item store.Item) (string, bool)
	// Estimate is a model stage's tokens by kind over items. Where it is set,
	// a replay answers the estimate and queues nothing until a request
	// confirms it. A model-free stage leaves it nil, and queues at once.
	Estimate func(ctx context.Context, items []store.Item) (store.Tokens, error)
}

// ReplayRoutes is the route for a person to run one of stages again over
// landed items. It is keyed: a request carries an Idempotency-Key, and a
// repeat under the same key answers the replay that the first one queued.
func ReplayRoutes(st *store.Store, stages map[string]Replayable, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := replayHandlers{store: st, stages: stages, log: log}
	return func(r *Router) {
		r.Handle("POST /api/v1/replays", ScopeItemsReplay, h.replay)
	}
}

type replayHandlers struct {
	store  *store.Store
	stages map[string]Replayable
	log    *slog.Logger
}

func (h replayHandlers) replay(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get(IdempotencyKey)
	if idempotencyKey == "" {
		Refuse(w, http.StatusBadRequest, CodeIdempotencyKeyMissing,
			"a replay carries an %s header, which names it across a retry, so nothing was queued", IdempotencyKey)
		return
	}
	var body ReplayRequest
	if !decode(w, r, &body) {
		return
	}
	if !slices.Contains(replayStages, body.Stage) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "stage is one of %s, not %q", strings.Join(replayStages, ", "), body.Stage)
		return
	}
	stage, ok := h.stages[body.Stage]
	if !ok {
		Refuse(w, http.StatusUnprocessableEntity, CodeStageNotReplayable, "no replay runs the %s stage yet, so nothing was queued", body.Stage)
		return
	}
	if body.Connector != nil && *body.Connector == "" {
		body.Connector = nil
	}
	if body.Key != nil {
		key, err := itemkey.Resolve(*body.Key)
		if err != nil {
			Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
			return
		}
		body.Key = &key
	}
	answer := ReplayAnswer{SchemaVersion: 1, Stage: body.Stage, Connector: body.Connector, Key: body.Key}
	if stage.Estimate != nil && !body.Confirm {
		h.estimate(w, r, stage, answer)
		return
	}
	caller, _ := goapiauth.IdentityFrom(r.Context())
	replay, queued, err := h.store.Replay(r.Context(), store.Replay{
		ID: replayID(caller, idempotencyKey), Stage: body.Stage, Connector: body.Connector, ItemKey: body.Key, RequestedBy: requester(caller),
	}, stage.Capability)
	if errors.Is(err, store.ErrReplayDiffers) {
		Refuse(w, http.StatusConflict, CodeIdempotencyKeyReused,
			"this %s already named a replay of another stage or other items, so nothing was queued", IdempotencyKey)
		return
	}
	if err != nil {
		h.log.ErrorContext(r.Context(), "queue a replay", "error", err, "stage", body.Stage)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not queue the replay")
		return
	}
	answer.Items, answer.Queued = replay.Items, true
	status := http.StatusOK
	if queued {
		status = http.StatusCreated
		h.log.Info("queued a replay", "replay", replay.ID, "stage", replay.Stage, "items", replay.Items,
			"connector", body.Connector, "key", body.Key, "requested_by", replay.RequestedBy)
	}
	JSON(w, status, answer)
}

// estimate answers a model stage's count and tokens by kind over the items
// the replay selects, and queues nothing.
func (h replayHandlers) estimate(w http.ResponseWriter, r *http.Request, stage Replayable, answer ReplayAnswer) {
	sel := store.ItemSelection{}
	if answer.Connector != nil {
		sel.Connector = *answer.Connector
	}
	if answer.Key != nil {
		sel.Key = *answer.Key
	}
	selected, err := h.store.SelectItems(r.Context(), sel)
	if err != nil {
		h.log.ErrorContext(r.Context(), "select a replay's items", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not select the replay's items")
		return
	}
	var items []store.Item
	for _, item := range selected {
		if _, ok := stage.Capability(item); ok {
			items = append(items, item)
		}
	}
	tokens, err := stage.Estimate(r.Context(), items)
	if err != nil {
		h.log.ErrorContext(r.Context(), "estimate a replay", "error", err, "stage", answer.Stage)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not estimate the replay")
		return
	}
	answer.Items, answer.Estimate = len(items), &tokens
	JSON(w, http.StatusOK, answer)
}

// replayID names a replay by its caller and Idempotency-Key, so two callers
// sending one key name two replays.
func replayID(caller goapiauth.Identity, idempotencyKey string) string {
	sum := sha256.Sum256([]byte(caller.ClientID + "\x00" + caller.Subject + "\x00" + idempotencyKey))
	return hex.EncodeToString(sum[:16])
}

// requester names the caller as a decision does: person:<login> or
// service:<client id>.
func requester(caller goapiauth.Identity) string {
	switch {
	case caller.Service:
		return "service:" + caller.ClientID
	case caller.Username != "":
		return "person:" + caller.Username
	case caller.Subject != "":
		return "person:" + caller.Subject
	}
	return "person:" + caller.ClientID
}
