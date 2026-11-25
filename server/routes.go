package server

import "strings"

// Repeat says what a second copy of a route's request does, so a client's
// retry and a handler's dedupe read one answer.
type Repeat string

const (
	// RepeatRead reads, so a second copy changes nothing.
	RepeatRead Repeat = "read"
	// RepeatKeyed writes once per Idempotency-Key. The client sets one key
	// per call and sends it on every retry of that call.
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
	RouteTasks          = Route{"GET /api/v1/tasks", "", RepeatRead}
	RouteBlob           = Route{"GET /api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatRead}
	RouteBlobWrite      = Route{"PUT /api/v1/blobs/{sha256}", ScopeTasksClaim, RepeatByContent}
	RouteBlobsUnpaired  = Route{"GET /api/v1/blobs/unpaired", "", RepeatRead}
	RouteItems          = Route{"GET /api/v1/items", ScopeItemsRead, RepeatRead}
	RouteItem           = Route{"GET /api/v1/items/{key}", ScopeItemsRead, RepeatRead}
	RouteItemParse      = Route{"GET /api/v1/items/{key}/parse", ScopeItemsRead, RepeatRead}
	RouteReplays        = Route{"POST /api/v1/replays", ScopeItemsReplay, RepeatKeyed}
)
