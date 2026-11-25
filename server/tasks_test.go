package server_test

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth"
	"github.com/datapointchris/goapiauth/authtest"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// Two workers race one task and one wins. Its lease lapses, the other claims
// it under a higher token, and the first worker's heartbeat and report are
// refused. A person's token reads the list, which holds to its schema, and a
// worker's token cannot.
func TestTwoWorkersRaceOneTaskALapsedLeaseReturnsItAndTheStaleReportIsRefused(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	now := time.Now()
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "ingestion.db"), store.WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	provider := authtest.NewProvider(t)
	provider.Register("ingestion-svc-worker", "worker-secret", server.ScopeTasksClaim)
	gate := readyGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
		server.TaskRoutes(st, log)))
	t.Cleanup(api.Close)
	service := (&clientcredentials.Config{
		ClientID: "ingestion-svc-worker", ClientSecret: "worker-secret", TokenURL: provider.URL + "/token",
		Scopes: []string{server.ScopeTasksClaim},
	}).Client(ctx)
	first, second := worker.NewClient(api.URL, service), worker.NewClient(api.URL, service)

	key := "url/0123456789abcdef"
	var task store.Task
	if err := st.Update(ctx, func(tx *store.Tx) error {
		if err := tx.PutItem(ctx, store.Item{Key: key, MediaType: "text/html", Zone: store.Outside}); err != nil {
			return err
		}
		task, err = tx.Enqueue(ctx, store.Task{ItemKey: key, Stage: store.StageParse, Capability: "html"})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	claim := func(c *worker.Client, id string) server.ClaimAnswer {
		answer, err := c.Claim(ctx, server.ClaimRequest{WorkerID: id, Capabilities: []string{"html"}, Version: "test", Max: 1, LeaseSeconds: 60})
		if err != nil {
			t.Error(err)
		}
		return answer
	}
	var answers [2]server.ClaimAnswer
	var wg sync.WaitGroup
	wg.Go(func() { answers[0] = claim(first, "worker-a") })
	wg.Go(func() { answers[1] = claim(second, "worker-b") })
	wg.Wait()
	won := len(answers[0].Tasks) + len(answers[1].Tasks)
	if won != 1 {
		t.Fatalf("two racing claims leased the task %d times, want once: %+v", won, answers)
	}
	winner, loser, loserID := first, second, "worker-b"
	lease := answers[0]
	if len(answers[1].Tasks) == 1 {
		winner, loser, loserID, lease = second, first, "worker-a", answers[1]
	}
	stale := lease.Tasks[0]
	if stale.ID != task.ID || stale.LeaseToken != 1 {
		t.Fatalf("the winning claim leased %+v, want task %s under token 1", stale, task.ID)
	}

	mu.Lock()
	now = now.Add(61 * time.Second)
	mu.Unlock()
	taken := claim(loser, loserID)
	if len(taken.Tasks) != 1 || taken.Tasks[0].LeaseToken != 2 || taken.Tasks[0].Attempts != 2 {
		t.Fatalf("a claim after the lease lapsed took %+v, want the task under token 2 at attempt 2", taken.Tasks)
	}
	if err := winner.Heartbeat(ctx, stale.ID, server.HeartbeatRequest{Token: stale.LeaseToken}); !errors.Is(err, worker.ErrStaleLease) {
		t.Errorf("a heartbeat under the stale token answered %v, want the stale lease refused", err)
	}
	text := "page text"
	err = winner.Report(ctx, stale.ID, server.ReportRequest{
		Token: stale.LeaseToken, Outcome: server.OutcomeDone,
		Outputs: []server.ReportOutput{{Producer: "html", ProducerVersion: "1", State: store.OutputDone, Text: &text}},
	})
	if !errors.Is(err, worker.ErrStaleLease) {
		t.Errorf("a report under the stale token answered %v, want the stale lease refused", err)
	}
	if records, _ := st.Item(ctx, key); len(records.Outputs) != 0 {
		t.Errorf("a refused report wrote %d outputs", len(records.Outputs))
	}
	current := server.ReportRequest{
		Token: 2, Outcome: server.OutcomeDone,
		Outputs: []server.ReportOutput{{Producer: "html", ProducerVersion: "1", State: store.OutputDone, Text: &text}},
	}
	if err := loser.Report(ctx, stale.ID, current); err != nil {
		t.Fatalf("the current token's report: %v", err)
	}
	// A worker whose report's answer was lost, as while serve rolls, sends it
	// again under the same token and is answered as the first report ended it.
	if err := loser.Report(ctx, stale.ID, current); err != nil {
		t.Errorf("the same report sent again answered %v, want the ended task", err)
	}
	records, err := st.Item(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if records.Phase() != "parsed" || records.Tasks[0].State != store.TaskDone {
		t.Errorf("after the report the item is %q and its task %s", records.Phase(), records.Tasks[0].State)
	}

	person := authtest.Bearer(provider.Person(t, "ingestion-cli-desk", "person"))
	listReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/api/v1/tasks?state=done", nil)
	resp, err := person.Do(listReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("a person's list answered %d", resp.StatusCode)
	}
	instance, err := jsonschema.UnmarshalJSON(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat()
	schema, err := compiler.Compile(mustAbs(t, "../schemas/tasks-list.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(instance); err != nil {
		t.Errorf("the list does not hold to its schema: %v", err)
	}
	listed := instance.(map[string]any)
	if tasks := listed["tasks"].([]any); len(tasks) != 1 {
		t.Errorf("the list holds %d done tasks, want 1", len(tasks))
	}
	if workers := listed["workers"].([]any); len(workers) != 2 {
		t.Errorf("the list holds %d workers, want both", len(workers))
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+"/api/v1/tasks", nil)
	resp, err = service.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("a worker's token reading the list answered %d, want 403", resp.StatusCode)
	}
}

// readyGate is a verifier already holding its provider's keys.
type readyGate struct{ *goapiauth.Verifier }

func (readyGate) Ready() error { return nil }

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}
