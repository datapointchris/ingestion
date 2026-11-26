package server

import (
	"context"
	"net/http"
	"strings"
)

// Repeat says what a second copy of a route's request does, so a client's
// retry and a handler's dedupe read one answer.
type Repeat string

const (
	// RepeatRead reads, so a second copy changes nothing.
	RepeatRead Repeat = "read"
	// RepeatKeyed writes once per Idempotency-Key. The client sets one key
	// per call and sends it on every retry of that call, and the router
	// refuses a request that carries none.
	RepeatKeyed Repeat = "keyed"
	// RepeatByContent writes what its body names, so a second copy of one
	// body leaves the state as the first copy left it.
	RepeatByContent Repeat = "byContent"
	// RepeatOnce may do its work twice when sent twice, so a client sends it
	// again only where the first copy never reached the server.
	RepeatOnce Repeat = "once"
)

// Route is one row of the API's route table: its pattern as ServeMux reads it
// ("POST /api/v1/x"), the scope a service's token needs, and what a repeat
// does. An empty scope admits a person's token alone.
type Route struct {
	Pattern string
	Scope   string
	Repeat  Repeat
}

// Method is the route's HTTP method.
func (r Route) Method() string {
	method, _, _ := strings.Cut(r.Pattern, " ")
	return method
}

// The route table. Each package registers its rows with Router.Handle, and a
// client sends through the row of the route it calls.
var (
	RouteVersion        = Route{"GET /api/v1/version", ScopeTasksClaim, RepeatRead}
	RouteTaskClaims     = Route{"POST /api/v1/tasks/claims", ScopeTasksClaim, RepeatOnce}
	RouteTaskHeartbeats = Route{"POST /api/v1/tasks/{id}/heartbeats", ScopeTasksClaim, RepeatByContent}
	RouteTaskReports    = Route{"POST /api/v1/tasks/{id}/reports", ScopeTasksClaim, RepeatByContent}
	RouteTaskParse      = Route{"GET /api/v1/tasks/{id}/parse", ScopeTasksClaim, RepeatRead}
	RouteTaskRetries    = Route{"POST /api/v1/tasks/{id}/retries", "", RepeatByContent}
	RouteTasks          = Route{"GET /api/v1/tasks", "", RepeatRead}
	RouteBlob           = Route{"GET /api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatRead}
	RouteBlobWrite      = Route{"PUT /api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatByContent}
	RouteBlobsUnpaired  = Route{"GET /api/v1/blobs/unpaired", "", RepeatRead}
	RouteItems          = Route{"GET /api/v1/items", ScopeItemsRead, RepeatRead}
	RouteItem           = Route{"GET /api/v1/items/{key}", ScopeItemsRead, RepeatRead}
	RouteItemParse      = Route{"GET /api/v1/items/{key}/parse", ScopeItemsRead, RepeatRead}
	RouteReplays        = Route{"POST /api/v1/replays", ScopeItemsReplay, RepeatKeyed}
	RouteDeclarations   = Route{"PUT /api/v1/declarations", ScopeDeclarationsApply, RepeatByContent}
	RouteConnectors     = Route{"GET /api/v1/connectors", ScopeConnectorsRead, RepeatRead}
	RouteConnector      = Route{"GET /api/v1/connectors/{name}", ScopeConnectorsRead, RepeatRead}
	RouteConnectorRuns  = Route{"POST /api/v1/connectors/{name}/runs", ScopeConnectorsRun, RepeatKeyed}
	RouteFirstFill      = Route{"PATCH /api/v1/connectors/{name}/first-fill", ScopeFirstFillsRelease, RepeatByContent}
	RouteConnectorDrops = Route{"POST /api/v1/connectors/{name}/drops", ScopeItemsDrop, RepeatKeyed}
	RouteReview         = Route{"GET /api/v1/review", ScopeReviewRead, RepeatRead}
	RouteReviewPlace    = Route{"POST /api/v1/review/{key}/place", ScopeReviewPlace, RepeatByContent}
	RouteReviewDrop     = Route{"POST /api/v1/review/{key}/drop", ScopeReviewPlace, RepeatByContent}
	RouteGateAgreement  = Route{"GET /api/v1/gates/agreement", ScopeItemsRead, RepeatRead}
	RouteItemMarks      = Route{"POST /api/v1/items/{key}/marks", ScopeMarksWrite, RepeatByContent}
	RouteGateTrust      = Route{"GET /api/v1/gates/trust", ScopeItemsRead, RepeatRead}
	// RouteGateTrustPromote's family is <connector>@<prompt version>@<model>@<effort>.
	RouteGateTrustPromote = Route{"POST /api/v1/gates/trust/{family}/promote", ScopeTrustPromote, RepeatByContent}
	RouteGateProposals    = Route{"GET /api/v1/gates/proposals", ScopeItemsRead, RepeatRead}
)

// Handle serves route with h. A service reaches it only while its token
// carries the route's scope. A keyed route refuses a request with no
// Idempotency-Key before h runs, and h reads the key with requestKey.
func (r *Router) Handle(route Route, h http.HandlerFunc) {
	if route.Repeat == RepeatKeyed {
		h = keyed(h)
	}
	r.mux.HandleFunc(route.Pattern, h)
	if route.Scope != "" {
		r.scopes[route.Scope] = append(r.scopes[route.Scope], route.Pattern)
	}
}

type requestKeyContext struct{}

// keyed reads a keyed route's Idempotency-Key into the request, and refuses
// a request that carries none.
func keyed(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimSpace(r.Header.Get(IdempotencyKey))
		if key == "" || len(key) > 200 {
			Refuse(w, http.StatusBadRequest, CodeIdempotencyKeyMissing,
				"this route carries an %s header of at most 200 characters, which names the request across a retry, so nothing was done", IdempotencyKey)
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), requestKeyContext{}, key)))
	}
}

// requestKey is the Idempotency-Key that a keyed route's request carries.
func requestKey(r *http.Request) string {
	key, _ := r.Context().Value(requestKeyContext{}).(string)
	return key
}
