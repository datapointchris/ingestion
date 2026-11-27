package server

import (
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/schemas"
	"github.com/datapointchris/ingestion/store"
)

// ScopeRoutingRulesWrite admits a caller to accept or retire a routing rule
// for a person, who its body may name.
const ScopeRoutingRulesWrite = "ingestion.routing-rules.write"

// CodeRoutingRuleRefused refuses a rule that the set in force refuses, such as
// one naming a connector or a destination that it does not declare.
// CodeRoutingRuleNotFound refuses a retirement of a rule that nobody accepted.
const (
	CodeRoutingRuleRefused  = "routing_rule_refused"
	CodeRoutingRuleNotFound = "routing_rule_not_found"
)

// acceptedRuleSchema is what a PUT of a routing rule holds to.
const acceptedRuleSchema = "routing-rules.schema.json#/$defs/accepted"

// ruleName is how a routing rule's name is spelled, as
// schemas/routing-rules.schema.json holds it.
var ruleName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,99}$`)

// RoutingRuleRequest accepts a routing rule:
// schemas/routing-rules.schema.json#/$defs/accepted. A service writing it for
// a person names the person's Login, and writes it as its own where it names
// none.
type RoutingRuleRequest struct {
	Action      string           `json:"action"`
	Match       connectors.Match `json:"match"`
	Destination string           `json:"destination,omitempty"`
	ReviewShare *float64         `json:"review_share,omitempty"`
	Login       string           `json:"login,omitempty"`
	Proposal    string           `json:"proposal,omitempty"`
}

// RetireRuleRequest retires a routing rule, for the person a service names.
type RetireRuleRequest struct {
	Login string `json:"login,omitempty"`
}

// RoutingRuleView is one version of an accepted routing rule:
// schemas/routing-rules.schema.json#/$defs/version.
type RoutingRuleView struct {
	Name        string           `json:"name"`
	Sequence    int              `json:"sequence"`
	Action      string           `json:"action"`
	Match       connectors.Match `json:"match"`
	Destination *string          `json:"destination"`
	ReviewShare float64          `json:"review_share"`
	Retired     bool             `json:"retired"`
	Proposal    *string          `json:"proposal"`
	DecidedBy   string           `json:"decided_by"`
	ClientID    *string          `json:"client_id"`
	DecidedTS   string           `json:"decided_ts"`
}

func ruleView(r store.RoutingRule) RoutingRuleView {
	return RoutingRuleView{
		Name: r.Name, Sequence: r.Sequence, Action: r.Action, Match: connectors.MatchOf(r), Destination: r.Destination, ReviewShare: r.ReviewShare,
		Retired: r.Retired, Proposal: r.Proposal, DecidedBy: r.DecidedBy, ClientID: r.ClientID, DecidedTS: r.DecidedTS,
	}
}

// RoutingRuleAnswer is what a write of a routing rule answers:
// schemas/routing-rule-version.schema.json.
type RoutingRuleAnswer struct {
	SchemaVersion int             `json:"schema_version"`
	Rule          RoutingRuleView `json:"rule"`
	Appended      bool            `json:"appended"`
}

// RoutingRulesAnswer is what GET /api/v1/routing-rules answers:
// schemas/routing-rules-list.schema.json.
type RoutingRulesAnswer struct {
	SchemaVersion int               `json:"schema_version"`
	Rules         []RoutingRuleView `json:"rules"`
}

// RoutingRuleRoutes are the routes for a person, or a service acting for one,
// to accept a routing rule, retire one, and read each rule's newest version.
// A write sent again with the same body appends nothing.
func RoutingRuleRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := ruleHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle(RouteRoutingRules, h.list)
		r.Handle(RouteRoutingRule, h.put)
		r.Handle(RouteRoutingRuleRetire, h.retire)
	}
}

type ruleHandlers struct {
	store *store.Store
	log   *slog.Logger
}

func (h ruleHandlers) list(w http.ResponseWriter, r *http.Request) {
	rules, err := h.store.RoutingRules(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the routing rules", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the routing rules")
		return
	}
	answer := RoutingRulesAnswer{SchemaVersion: 1, Rules: make([]RoutingRuleView, 0, len(rules))}
	for _, rule := range rules {
		answer.Rules = append(answer.Rules, ruleView(rule))
	}
	JSON(w, http.StatusOK, answer)
}

func (h ruleHandlers) put(w http.ResponseWriter, r *http.Request) {
	name, ok := pathRuleName(w, r)
	if !ok {
		return
	}
	var body RoutingRuleRequest
	if !decode(w, r, &body) {
		return
	}
	schema, err := schemas.Compiled(acceptedRuleSchema)
	if err != nil {
		h.log.ErrorContext(r.Context(), "compile the routing rule's schema", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not check the routing rule")
		return
	}
	if problems := schemas.Problems(schema, body); len(problems) > 0 {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the rule does not hold to schemas/%s: %s", acceptedRuleSchema, strings.Join(problems, "; "))
		return
	}
	decidedBy, clientID, ok := ruleDecider(w, r, body.Login)
	if !ok {
		return
	}
	m := body.Match
	rule := connectors.WithMatch(store.RoutingRule{
		Name: name, Action: body.Action, Destination: optional(body.Destination), ReviewShare: *body.ReviewShare, Proposal: optional(body.Proposal),
		DecidedBy: decidedBy, ClientID: optional(clientID),
	}, m)
	var written store.RoutingRule
	var appended bool
	err = h.store.Update(r.Context(), func(tx *store.Tx) error {
		written, appended, err = route.AcceptRule(r.Context(), tx, rule, h.log)
		return err
	})
	var refused *route.RuleError
	switch {
	case errors.As(err, &refused):
		Refuse(w, http.StatusUnprocessableEntity, CodeRoutingRuleRefused, "%v, so nothing was written", refused)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "accept a routing rule", "error", err, "rule", name)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not accept the routing rule")
		return
	}
	h.log.Info("a routing rule was accepted", "rule", name, "sequence", written.Sequence, "appended", appended, "action", written.Action,
		"connector", m.Connector, "tag", m.Tag, "field", m.Field, "destination", body.Destination, "review_share", written.ReviewShare,
		"decided_by", written.DecidedBy, "client", clientID, "proposal", body.Proposal)
	JSON(w, http.StatusOK, RoutingRuleAnswer{SchemaVersion: 1, Rule: ruleView(written), Appended: appended})
}

func (h ruleHandlers) retire(w http.ResponseWriter, r *http.Request) {
	name, ok := pathRuleName(w, r)
	if !ok {
		return
	}
	var body RetireRuleRequest
	if !decode(w, r, &body) {
		return
	}
	decidedBy, clientID, ok := ruleDecider(w, r, body.Login)
	if !ok {
		return
	}
	var written store.RoutingRule
	var appended bool
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		written, appended, err = route.RetireRule(r.Context(), tx, name, decidedBy, optional(clientID), h.log)
		return err
	})
	switch {
	case errors.Is(err, route.ErrNoRule):
		Refuse(w, http.StatusNotFound, CodeRoutingRuleNotFound, "nobody accepted a routing rule %s: `ingestion gates routing-rules list` names each one", name)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "retire a routing rule", "error", err, "rule", name)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not retire the routing rule")
		return
	}
	h.log.Info("a routing rule was retired", "rule", name, "sequence", written.Sequence, "appended", appended, "decided_by", written.DecidedBy,
		"client", clientID)
	JSON(w, http.StatusOK, RoutingRuleAnswer{SchemaVersion: 1, Rule: ruleView(written), Appended: appended})
}

// pathRuleName is the rule's name that the path holds, and refuses the
// request where it is not spelled as a rule's name is.
func pathRuleName(w http.ResponseWriter, r *http.Request) (string, bool) {
	name := r.PathValue("name")
	if !ruleName.MatchString(name) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody,
			"a routing rule's name is lowercase letters, digits and hyphens from a letter, at most 100 of them, not %q", name)
		return "", false
	}
	return name, true
}

// ruleDecider is who writes a routing rule, and the client that sent it, and
// refuses the request where it cannot say. A service that names no login
// writes it as service:<client id>.
func ruleDecider(w http.ResponseWriter, r *http.Request, login string) (decidedBy, clientID string, ok bool) {
	if caller, _ := goapiauth.IdentityFrom(r.Context()); caller.Service && login == "" {
		return requester(caller), caller.ClientID, true
	}
	login, clientID, err := decider(r, login)
	switch {
	case errors.Is(err, errActingForAnother):
		Refuse(w, http.StatusForbidden, CodeActingForAnother, "%v", err)
		return "", "", false
	case err != nil:
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%v", err)
		return "", "", false
	}
	return store.DecidedByPerson(login), clientID, true
}
