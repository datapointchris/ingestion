package cmd

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"

	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemflow"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/ypl"
)

// tracedRequest is one request that a stand-in answered, by its method and
// path, with the trace context it carried.
type tracedRequest struct{ route, traceparent, baggage string }

// traceRecorder serves a stand-in's handler and keeps each request's trace
// context.
type traceRecorder struct {
	*httptest.Server
	mu   sync.Mutex
	seen []tracedRequest
}

func recordTraces(t *testing.T, stand http.Handler) *traceRecorder {
	t.Helper()
	r := &traceRecorder{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.seen = append(r.seen, tracedRequest{
			route: req.Method + " " + req.URL.Path, traceparent: req.Header.Get("traceparent"), baggage: req.Header.Get("baggage"),
		})
		r.mu.Unlock()
		stand.ServeHTTP(w, req)
	}))
	t.Cleanup(r.Close)
	return r
}

// sampledTraceparent is a W3C traceparent of version 00 with the sampled flag.
var sampledTraceparent = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-01$`)

// carriesFlow fails t unless r saw each of routes, and every request it saw
// carried flow as its trace id and key as its baggage's item key.
func (r *traceRecorder) carriesFlow(t *testing.T, stand, flow, key string, routes ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := []string{}
	for _, req := range r.seen {
		seen = append(seen, req.route)
		parent := sampledTraceparent.FindStringSubmatch(req.traceparent)
		carried, _ := baggage.Parse(req.baggage)
		if len(parent) != 3 || parent[1] != flow || parent[2] == "0000000000000000" || carried.Member(itemflow.KeyBaggage).Value() != key {
			t.Errorf("%s's %s carried traceparent %q and baggage %q, want trace %s and %s=%s",
				stand, req.route, req.traceparent, req.baggage, flow, itemflow.KeyBaggage, key)
		}
	}
	for _, want := range routes {
		if !slices.Contains(seen, want) {
			t.Errorf("%s answered %v, want %s among them", stand, seen, want)
		}
	}
}

// flowDestinations are withLife's destinations beside one for learning and
// one for watching.
const flowDestinations = "destinations:\n  - name: reading-list\n    side: personal\n    deliverer: life-articles\n    default_for_side: true\n" +
	"  - name: learning-plan\n    side: personal\n    deliverer: learning-resources\n" +
	"  - name: watch\n    side: personal\n    deliverer: ypl-watch\n" +
	"  - name: work-inbox\n    side: fleet\n    default_for_side: true\n  - name: dropped\n"

// A save's quality gate dispatches its run, reads the dispatch while it waits,
// then reads its answer, whose guess delivers the save to life. A video is
// delivered to learning and to a playlist through ypl. Every request that the
// dispatch service, life, learning and ypl answer carries a traceparent whose
// trace id is its item's flow_id, and baggage naming its item's key.
func TestEachDeliveryAndModelRunCarriesItsItemsFlow(t *testing.T) {
	ctx := t.Context()
	p := newGatedPipeline(t, lifeSet(t, flowDestinations))
	life, learning, dispatch := newStandInLife(t, p.provider), newStandInLearning(t, p.provider), newStandInDispatch(t, p.provider)
	videos, yplAPI := newStandInYPL(t)
	videos.playlistIDs["Later"] = "PLlater"
	lifeSeen, learningSeen := recordTraces(t, life.Config.Handler), recordTraces(t, learning.Config.Handler)
	dispatchSeen, yplSeen := recordTraces(t, dispatch.Config.Handler), recordTraces(t, yplAPI.Config.Handler)

	save := "reddit/t3_bb2"
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	models := dispatch.models(t, "", "")
	models.Queue.API = dispatchSeen.URL
	dispatch.set(func(d *standInDispatch) { d.answer, d.hold = answerBy(nil), "queued behind another run" })
	gate := p.qualityGate(t, models)
	p.run(t, gate)
	dispatch.set(func(d *standInDispatch) { d.hold = "" })
	p.advance(time.Hour)
	p.run(t, gate)

	landed, err := p.lander.Land(ctx, landing.Arrival{
		Connector: "video-capture", Zone: store.Outside, MediaType: ypl.MediaTypeVideo,
		URL: "https://www.youtube.com/watch?v=flowvideo01", Title: "Test video",
	})
	if err != nil {
		t.Fatal(err)
	}
	video := landed.Key
	if err := p.st.Update(ctx, func(tx *store.Tx) error {
		item, _, err := tx.Item(ctx, video)
		if err != nil {
			return err
		}
		if _, err := tx.QueueDelivery(ctx, item, nil, nil, nil, "learning-plan", deliver.LearningResources); err != nil {
			return err
		}
		_, err = tx.QueueDelivery(ctx, item, nil, nil, ptr("Later"), "watch", route.WatchDeliverer)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	t.Setenv(deliverSecretEnv(deliver.LifeArticles), lifeSecret)
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	opened := openDeliverers(ctx, fileConfig{Deliver: map[string]deliverTable{
		deliver.LifeArticles:      {serviceClient: serviceClient{API: lifeSeen.URL, ClientID: lifeClient}},
		deliver.LearningResources: {serviceClient: serviceClient{API: learningSeen.URL, ClientID: learningClient}},
	}}, p.provider.URL, quiet)
	watch := yplHandlers(t, p, yplSeen.URL)[store.DeliverCapability(route.WatchDeliverer)]
	if ran := p.run(t, deliver.Handler(deliver.LifeArticles, opened[deliver.LifeArticles]),
		deliver.Handler(deliver.LearningResources, opened[deliver.LearningResources]), watch); ran != 3 {
		t.Fatalf("the worker ran %d deliveries, want the save's to life and the video's to learning and to its playlist", ran)
	}

	saveFlow, videoFlow := p.shown(t, save).Item.FlowID, p.shown(t, video).Item.FlowID
	dispatchSeen.carriesFlow(t, "the dispatch service", saveFlow, save,
		"POST /api/v1/dispatches/", "GET /api/v1/dispatches/dispatch-1/", "GET /api/v1/outside-text-answers/dispatch-1")
	lifeSeen.carriesFlow(t, "life", saveFlow, save, "POST /api/v1/articles")
	learningSeen.carriesFlow(t, "learning", videoFlow, video, "POST /api/v1/resources")
	yplSeen.carriesFlow(t, "ypl", videoFlow, video, "POST /api/v1/youtube/playlists/Later/items")
}
