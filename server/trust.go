package server

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/store"
)

// ScopeMarksWrite admits a caller to mark an item for a person, who its body
// may name. ScopeTrustPromote admits it to promote a gate family for the
// person whose login its body names.
const (
	ScopeMarksWrite   = "ingestion.marks.write"
	ScopeTrustPromote = "ingestion.trust.promote"
)

// CodeFamilyNotFound refuses a promotion naming a family that the store holds
// no standing of.
const CodeFamilyNotFound = "family_not_found"

// MarkRequest marks an item low-quality or read. A service marking for a
// person may name the person's Login, since its token carries none. Without
// one, the mark is the service's, which relays a person's word under its
// scope.
type MarkRequest struct {
	Mark  string `json:"mark"`
	Login string `json:"login,omitempty"`
}

// MarkAnswer is what a mark answers: schemas/item-mark.schema.json. Families
// is each family's standing after the mark moved it, and empty where the mark
// moved none.
type MarkAnswer struct {
	SchemaVersion int               `json:"schema_version"`
	Mark          store.Mark        `json:"mark"`
	Families      []store.GateTrust `json:"families"`
}

// PromoteRequest promotes a gate family. A service promoting for a person
// names the person's Login, since its token carries none.
type PromoteRequest struct {
	Login string `json:"login,omitempty"`
}

// PromotionAnswer is what a promotion answers:
// schemas/gate-trust-promotion.schema.json.
type PromotionAnswer struct {
	SchemaVersion int             `json:"schema_version"`
	Family        store.GateTrust `json:"family"`
}

// TrustRule is the ladder that every family climbs, as a reader shows it.
type TrustRule struct {
	// Run is the straight run of a person's agreeing marks that earns asking,
	// and climbs each rung above 1.
	Run   int     `json:"run"`
	First float64 `json:"first"`
	Lower float64 `json:"lower"`
	Floor float64 `json:"floor"`
	// Shares is the share of drops shown on each rung from 1 up.
	Shares []float64 `json:"shares"`
}

// GateTrustAnswer is what GET /api/v1/gates/trust answers:
// schemas/gate-trust-list.schema.json.
type GateTrustAnswer struct {
	SchemaVersion int               `json:"schema_version"`
	Rule          TrustRule         `json:"rule"`
	Families      []store.GateTrust `json:"families"`
}

// TrustRoutes are the routes for a person, or a service acting for one, to
// mark an item, read each gate family's standing, and promote a family. A
// mark or a promotion sent again with the same body changes nothing.
func TrustRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := trustHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle(RouteItemMarks, h.mark)
		r.Handle(RouteGateTrust, h.list)
		r.Handle(RouteGateTrustPromote, h.promote)
	}
}

type trustHandlers struct {
	store *store.Store
	log   *slog.Logger
}

func (h trustHandlers) mark(w http.ResponseWriter, r *http.Request) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
		return
	}
	var body MarkRequest
	if !decode(w, r, &body) {
		return
	}
	if !slices.Contains(gatetrust.Marks, body.Mark) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a mark is %s or %s, not %q", gatetrust.MarkLowQuality, gatetrust.MarkRead, body.Mark)
		return
	}
	m := gatetrust.Marking{Mark: body.Mark}
	if caller, _ := goapiauth.IdentityFrom(r.Context()); caller.Service && body.Login == "" {
		m.DecidedBy, m.ClientID = requester(caller), caller.ClientID
	} else {
		var login string
		if !h.decider(w, r, body.Login, &login, &m.ClientID) {
			return
		}
		m.DecidedBy = store.DecidedByPerson(login)
	}
	var answer MarkAnswer
	err = h.store.Update(r.Context(), func(tx *store.Tx) error {
		answer.Mark, answer.Families, err = gatetrust.Mark(r.Context(), tx, key, m)
		return err
	})
	switch {
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "mark an item", "error", err, "item", key)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not mark the item")
		return
	}
	answer.SchemaVersion = 1
	if answer.Families == nil {
		answer.Families = []store.GateTrust{}
	}
	h.log.Info("a person marked an item", "item", key, "mark", m.Mark, "decided_by", answer.Mark.DecidedBy, "client", m.ClientID,
		"agreement", answer.Mark.Agreement, "families", answer.Mark.Families)
	JSON(w, http.StatusOK, answer)
}

func (h trustHandlers) list(w http.ResponseWriter, r *http.Request) {
	families, err := h.store.GateTrusts(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "read each gate family's standing", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the gate families")
		return
	}
	rule := gatetrust.Rule
	JSON(w, http.StatusOK, GateTrustAnswer{
		SchemaVersion: 1, Families: families,
		Rule: TrustRule{Run: rule.Run, First: rule.First, Lower: rule.Lower, Floor: rule.Floor, Shares: rule.Shares()},
	})
}

func (h trustHandlers) promote(w http.ResponseWriter, r *http.Request) {
	family := r.PathValue("family")
	var body PromoteRequest
	if !decode(w, r, &body) {
		return
	}
	var login, client string
	if !h.decider(w, r, body.Login, &login, &client) {
		return
	}
	var promoted store.GateTrust
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		promoted, err = gatetrust.Promote(r.Context(), tx, family, login, client)
		return err
	})
	switch {
	case errors.Is(err, gatetrust.ErrNoFamily):
		Refuse(w, http.StatusNotFound, CodeFamilyNotFound, "no gate family %s: `ingestion gates trust list` names each one", family)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "promote a gate family", "error", err, "family", family)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not promote the family")
		return
	}
	h.log.Info("a person promoted a gate family", "family", family, "promoted_by", promoted.PromotedBy, "client", client,
		"rung", promoted.Rung, "straight_run", promoted.StraightRun)
	JSON(w, http.StatusOK, PromotionAnswer{SchemaVersion: 1, Family: promoted})
}

// decider writes who decides into login and client, and refuses the request
// where it cannot say.
func (h trustHandlers) decider(w http.ResponseWriter, r *http.Request, named string, login, client *string) bool {
	var err error
	*login, *client, err = decider(r, named)
	switch {
	case errors.Is(err, errActingForAnother):
		Refuse(w, http.StatusForbidden, CodeActingForAnother, "%v", err)
		return false
	case err != nil:
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%v", err)
		return false
	}
	return true
}
