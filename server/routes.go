package server

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// Repeat says what a route does with a request sent again. A client's retry
// and the router's key check both read it from the route's row.
type Repeat string

const (
	// RepeatRead changes nothing, so a repeat answers as the first did.
	RepeatRead Repeat = "read"
	// RepeatByContent is a write whose request names its result, such as the
	// same bytes, the same set or the same state, so a repeat changes nothing
	// more.
	RepeatByContent Repeat = "byContent"
	// RepeatKeyed is a write named by the request's Idempotency-Key. A repeat
	// under the key answers what the first answered, and the router refuses a
	// request that carries no key.
	RepeatKeyed Repeat = "keyed"
	// RepeatOnce is a write that a repeat would do again, so a client sends it
	// again only where the first never reached the server.
	RepeatOnce Repeat = "once"
)

// Route is one row of the route table: the method and path that ServeMux
// serves, the scope a service's token needs, and what a repeat does. An empty
// scope admits a person's token alone.
type Route struct {
	Method string
	Path   string
	Scope  string
	Repeat Repeat
}

// The route table. Each package registers its handler for its rows.
var (
	RouteVersion = Route{http.MethodGet, "/api/v1/version", ScopeTasksClaim, RepeatRead}

	RouteBlob          = Route{http.MethodGet, "/api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatRead}
	RouteBlobWrite     = Route{http.MethodPut, "/api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatByContent}
	RouteBlobsUnpaired = Route{http.MethodGet, "/api/v1/blobs/unpaired", "", RepeatRead}

	RouteTaskClaims     = Route{http.MethodPost, "/api/v1/tasks/claims", ScopeTasksClaim, RepeatOnce}
	RouteTaskHeartbeats = Route{http.MethodPost, "/api/v1/tasks/{id}/heartbeats", ScopeTasksClaim, RepeatByContent}
	RouteTaskReports    = Route{http.MethodPost, "/api/v1/tasks/{id}/reports", ScopeTasksClaim, RepeatOnce}
	RouteTasks          = Route{http.MethodGet, "/api/v1/tasks", "", RepeatRead}

	RouteDeclarations   = Route{http.MethodPut, "/api/v1/declarations", ScopeDeclarationsApply, RepeatByContent}
	RouteConnectors     = Route{http.MethodGet, "/api/v1/connectors", ScopeConnectorsRead, RepeatRead}
	RouteConnector      = Route{http.MethodGet, "/api/v1/connectors/{name}", ScopeConnectorsRead, RepeatRead}
	RouteConnectorRuns  = Route{http.MethodPost, "/api/v1/connectors/{name}/runs", ScopeConnectorsRun, RepeatKeyed}
	RouteFirstFill      = Route{http.MethodPatch, "/api/v1/connectors/{name}/first-fill", ScopeFirstFillsRelease, RepeatByContent}
	RouteConnectorDrops = Route{http.MethodPost, "/api/v1/connectors/{name}/drops", ScopeItemsDrop, RepeatKeyed}

	RouteItems = Route{http.MethodGet, "/api/v1/items", ScopeItemsRead, RepeatRead}
	RouteItem  = Route{http.MethodGet, "/api/v1/items/{key}", ScopeItemsRead, RepeatRead}
)

// Pattern is the route as ServeMux reads it, such as "POST /api/v1/x".
func (rt Route) Pattern() string { return rt.Method + " " + rt.Path }

// URL is the route's path with each {name} filled, in order, from values,
// each path-escaped.
func (rt Route) URL(values ...string) string {
	var b strings.Builder
	rest := rt.Path
	for _, v := range values {
		open := strings.IndexByte(rest, '{')
		end := strings.IndexByte(rest, '}')
		if open < 0 || end < open {
			break
		}
		b.WriteString(rest[:open])
		b.WriteString(url.PathEscape(v))
		rest = rest[end+1:]
	}
	b.WriteString(rest)
	return b.String()
}

// Handle serves route with h. A service reaches it only while its token
// carries the route's scope. A keyed route refuses a request with no
// Idempotency-Key before h runs, and h reads the key with requestKey.
func (r *Router) Handle(route Route, h http.HandlerFunc) {
	if route.Repeat == RepeatKeyed {
		h = keyed(h)
	}
	r.mux.HandleFunc(route.Pattern(), h)
	if route.Scope != "" {
		r.scopes[route.Scope] = append(r.scopes[route.Scope], route.Pattern())
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
