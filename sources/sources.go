// Package sources holds each source that this build carries, as the source
// registers it once from its own init. A registration names the rules for
// the source's connectors, the scopes for its service client, and an opener
// for each stage that a worker runs for it. A set checks its connectors
// against the same registration, through connectors.RegisterSource, and the
// builtin extractors take its extractors, through extract.Register. A worker
// opens one client per [source.<name>] table and asks the registration for
// the handler of each of the source's stages. So a new stage of a source
// changes this package and the source, and nothing in cmd.
package sources

import (
	"log/slog"
	"maps"
	"net/http"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deletion"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Registration is one source as this build carries it.
type Registration struct {
	// Name is what a connector's source names, the suffix of its tasks'
	// capabilities, and its table's name under [source] in a worker's config.
	Name string
	// Scopes is every scope that the source's service client asks its token
	// for, one client serving each of the source's stages.
	Scopes []string
	// Rules answers why a connector of the source cannot run, one problem
	// each. A source with none takes no source_parameters.
	Rules func(connectors.Connector) []string
	// Poller opens the source's poller, which reaches it at api through
	// client. client carries the service client's token. A source that no
	// worker polls leaves it nil.
	Poller func(api string, client *http.Client) polling.Poller
	// Deleter opens the source's deleter as Poller opens its poller. A source
	// that nothing deletes from leaves it nil.
	Deleter func(api string, client *http.Client) deletion.Deleter
	// Extractors are the source's extractors, each of which ReadsURL: it reads
	// an item that holds no bytes at the source, through the reader that its
	// Open opens on the source's client.
	Extractors []extract.Extractor
}

var registered = map[string]Registration{}

// Register records r, records with connectors what a set checks of it, and
// adds its extractors to the builtin ones, so landing queues their parses.
// Registering a source twice panics, as a duplicate is a build error.
func Register(r Registration) {
	connectors.RegisterSource(connectors.Source{Name: r.Name, Polls: r.Poller != nil, Deletes: r.Deleter != nil, Rules: r.Rules})
	extract.Register(r.Extractors...)
	registered[r.Name] = r
}

// Lookup is the source registered as name, and false where none is.
func Lookup(name string) (Registration, bool) {
	r, ok := registered[name]
	return r, ok
}

// Names names, in order, each source that this build registers.
func Names() []string { return slices.Sorted(maps.Keys(registered)) }

// Capabilities are the capabilities of the tasks that r's stages claim.
func (r Registration) Capabilities() []string {
	var out []string
	if r.Poller != nil {
		out = append(out, store.PollCapability(r.Name))
	}
	if r.Deleter != nil {
		out = append(out, store.DeleteCapability(r.Name))
	}
	for _, e := range r.Extractors {
		out = append(out, extract.Capability(e))
	}
	return out
}

// Handlers are the worker handlers of each stage that r serves, which reach
// the source at api through client.
func (r Registration) Handlers(api string, client *http.Client, log *slog.Logger) []worker.Handler {
	var out []worker.Handler
	if r.Poller != nil {
		out = append(out, polling.Handler(r.Name, r.Poller(api, client), log))
	}
	if r.Deleter != nil {
		out = append(out, deletion.Handler(r.Name, r.Deleter(api, client)))
	}
	for _, e := range r.Extractors {
		if h, ok := extract.SourceHandler(e, api, client, log); ok {
			out = append(out, h)
		}
	}
	return out
}
