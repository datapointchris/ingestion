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
	CodeNoItemFromConnector   = "no_item_from_connector"
	// CodePlacedByNotReplayable is a replay over the labeled items, of a
	// stage that holds no such replay.
	CodePlacedByNotReplayable = "placed_by_not_replayable"
	// CodeModelNotReplayable is a replay that names a model, of a stage that
	// runs on the model its worker's config names.
	CodeModelNotReplayable = "model_not_replayable"
	// CodeModelMissing is a replay of a measurement arm that names no model.
	CodeModelMissing = "model_missing"
)

// ReplayStages are the stages that a replay request may name, as
// schemas/replay.schema.json's stage enum holds them. ReplayRoutes takes the
// stages that the service replays.
var ReplayStages = []string{
	store.StageParse, store.StageStructure, store.StageContents, store.StageScan, store.StageQuality, store.StageQualityArm,
	store.StageModelRead, store.StageDeliver,
}

// ReplayRequest asks to run Stage again over the items that Connector and Key
// select, each absent selecting every item. PlacedBy store.PlacedByPerson
// narrows them to the labeled items. Model names the model that a
// measurement arm asks. Confirm runs a model stage once a person has read its
// estimate.
type ReplayRequest struct {
	Stage     string  `json:"stage"`
	Connector *string `json:"connector"`
	Key       *string `json:"key"`
	PlacedBy  *string `json:"placed_by,omitempty"`
	Model     *string `json:"model,omitempty"`
	Confirm   bool    `json:"confirm"`
}

// ReplayAnswer is what POST /api/v1/replays answers:
// schemas/replay.schema.json.
type ReplayAnswer struct {
	SchemaVersion int           `json:"schema_version"`
	Stage         string        `json:"stage"`
	Connector     *string       `json:"connector"`
	Key           *string       `json:"key"`
	PlacedBy      *string       `json:"placed_by"`
	Model         *string       `json:"model"`
	Items         int           `json:"items"`
	Queued        bool          `json:"queued"`
	Estimate      *store.Tokens `json:"estimate"`
}

// Replayable is one stage that a replay runs again.
type Replayable struct {
	// Capability is the capability of the stage's task on item, and false
	// where the stage does not run on item. A replay queues one task of that
	// capability for each item it covers.
	Capability func(item store.Item) (string, bool)
	// Queue, where it is set, queues the stage's tasks for one item instead,
	// and answers whether it queued any. A stage whose task names more than
	// its item, such as a delivery's destination, sets it.
	Queue store.ReplayQueue
	// Estimate is how many of items a model stage would run on, and its
	// tokens by kind over them. Where it is set, a replay answers the
	// estimate and queues nothing until a request confirms it. A model-free
	// stage leaves it nil, and queues at once.
	// Its tokens are nil where it holds no stored run to estimate from.
	Estimate func(ctx context.Context, items []store.Item) (int, *store.Tokens, error)
	// PlacedByPerson, where it is set, is the stage run only over the
	// labeled items, as a measurement of the stage against each placement. A
	// request asks for it with placed_by person.
	PlacedByPerson *Replayable
	// OnModel, where it is set, is a measurement arm: the stage asked of the
	// model that a request names. A request to such a stage names its model,
	// and one to any other stage names none.
	OnModel func(model string) Replayable
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
		r.Handle(RouteReplays, h.replay)
	}
}

type replayHandlers struct {
	store  *store.Store
	stages map[string]Replayable
	log    *slog.Logger
}

func (h replayHandlers) replay(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := requestKey(r)
	var body ReplayRequest
	if !decode(w, r, &body) {
		return
	}
	if !slices.Contains(ReplayStages, body.Stage) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "stage is one of %s, not %q", strings.Join(ReplayStages, ", "), body.Stage)
		return
	}
	stage, ok := h.stages[body.Stage]
	if !ok {
		Refuse(w, http.StatusUnprocessableEntity, CodeStageNotReplayable, "the service holds no replay of the %s stage, so nothing was queued", body.Stage)
		return
	}
	if body.PlacedBy != nil && *body.PlacedBy == "" {
		body.PlacedBy = nil
	}
	if body.PlacedBy != nil {
		if *body.PlacedBy != store.PlacedByPerson {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "placed_by is %s or absent, not %q", store.PlacedByPerson, *body.PlacedBy)
			return
		}
		if stage.PlacedByPerson == nil {
			Refuse(w, http.StatusUnprocessableEntity, CodePlacedByNotReplayable,
				"the service holds no replay of the %s stage over the items a person placed, so nothing was queued", body.Stage)
			return
		}
		stage = *stage.PlacedByPerson
	}
	if body.Model != nil && strings.TrimSpace(*body.Model) == "" {
		body.Model = nil
	}
	switch {
	case body.Model != nil && stage.OnModel == nil:
		Refuse(w, http.StatusUnprocessableEntity, CodeModelNotReplayable,
			"the %s stage runs on the model its worker's config names, so a replay of it names none, and nothing was queued", body.Stage)
		return
	case body.Model == nil && stage.OnModel != nil:
		Refuse(w, http.StatusBadRequest, CodeModelMissing, "a replay of the %s stage names the model it asks, so nothing was queued", body.Stage)
		return
	case body.Model != nil:
		model := strings.TrimSpace(*body.Model)
		body.Model, stage = &model, stage.OnModel(model)
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
	answer := ReplayAnswer{SchemaVersion: 1, Stage: body.Stage, Connector: body.Connector, Key: body.Key, PlacedBy: body.PlacedBy, Model: body.Model}
	if stage.Estimate != nil && !body.Confirm {
		h.estimate(w, r, stage, answer)
		return
	}
	caller, _ := goapiauth.IdentityFrom(r.Context())
	replay, queued, err := h.store.Replay(r.Context(), store.Replay{
		ID: replayID(caller, idempotencyKey), Stage: body.Stage, Connector: body.Connector, ItemKey: body.Key, PlacedBy: body.PlacedBy,
		Model: body.Model, RequestedBy: requester(caller),
	}, stage.queue(body.Stage))
	if errors.Is(err, store.ErrReplayDiffers) {
		Refuse(w, http.StatusConflict, CodeIdempotencyKeyReused,
			"this %s already named a replay of another stage or other items, so nothing was queued", IdempotencyKey)
		return
	}
	if selectionRefused(w, err) {
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
			"connector", body.Connector, "key", body.Key, "model", body.Model, "requested_by", replay.RequestedBy)
	}
	JSON(w, status, answer)
}

// estimate answers a model stage's count and tokens by kind over the items
// the replay selects, and queues nothing.
func (h replayHandlers) estimate(w http.ResponseWriter, r *http.Request, stage Replayable, answer ReplayAnswer) {
	items, err := h.store.ReplayItems(r.Context(), store.Replay{Stage: answer.Stage, Connector: answer.Connector, ItemKey: answer.Key}, stage.takes)
	if selectionRefused(w, err) {
		return
	}
	if err != nil {
		h.log.ErrorContext(r.Context(), "select a replay's items", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not select the replay's items")
		return
	}
	n, tokens, err := stage.Estimate(r.Context(), items)
	if err != nil {
		h.log.ErrorContext(r.Context(), "estimate a replay", "error", err, "stage", answer.Stage)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not estimate the replay")
		return
	}
	answer.Items, answer.Estimate = n, tokens
	JSON(w, http.StatusOK, answer)
}

// takes says whether the stage runs on item: whether Capability names a task
// for it, and every item where the stage sets Queue alone.
func (s Replayable) takes(item store.Item) bool {
	if s.Capability == nil {
		return true
	}
	_, ok := s.Capability(item)
	return ok
}

// queue is s.Queue, else a task of s's capability for each item that it takes.
func (s Replayable) queue(stage string) store.ReplayQueue {
	if s.Queue != nil {
		return s.Queue
	}
	return func(ctx context.Context, tx *store.Tx, item store.Item) (bool, error) {
		capability, ok := s.Capability(item)
		if !ok {
			return false, nil
		}
		_, err := tx.Enqueue(ctx, store.Task{ItemKey: &item.Key, Stage: stage, Capability: capability})
		return err == nil, err
	}
}

// selectionRefused answers 404 where a replay's key names no item or its
// connector sighted none, so a mistyped selection is refused rather than
// queued over nothing, and says whether it answered.
func selectionRefused(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, store.ErrNoItem), errors.Is(err, store.ErrNoSighting):
		code := CodeItemNotFound
		if errors.Is(err, store.ErrNoSighting) {
			code = CodeNoItemFromConnector
		}
		Refuse(w, http.StatusNotFound, code, "%v, so nothing was queued", err)
	default:
		return false
	}
	return true
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
		return store.DecidedByService(caller.ClientID)
	case caller.Username != "":
		return store.DecidedByPerson(caller.Username)
	case caller.Subject != "":
		return store.DecidedByPerson(caller.Subject)
	}
	return store.DecidedByPerson(caller.ClientID)
}
