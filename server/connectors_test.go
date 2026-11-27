package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// A drop retried under its Idempotency-Key answers the first landing, and
// another file under that key is refused. A poll connector's run makes one
// task. A drop connector's first fill runs one item through the model stages
// and holds the rest, across a service restart, until a person releases it.
// A person's hold on the poll connector keeps its queued poll from a claim
// across the restart, and its release returns it. An apply that replaces a
// commit not in force is refused, and a later set that drops the connector
// leaves it holdable and releasable. A task made for its item while it is
// held is made held, and its release returns that task and the fill's in
// their order. Each answer holds to its schema.
func TestADropRetriedUnderItsKeyLandsOnceAndAFirstFillOrAHoldHoldsAcrossARestartUntilReleased(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	provider := authtest.NewProvider(t)
	gate := readyGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	person := provider.Person(t, "ingestion-cli-test", "person")
	log := slog.New(slog.DiscardHandler)
	schemas := compileAnswerSchemas(t)

	var st *store.Store
	var api *httptest.Server
	start := func() {
		t.Helper()
		var err error
		if st, err = store.Open(ctx, filepath.Join(dir, "ingestion.db")); err != nil {
			t.Fatal(err)
		}
		blobs, err := landing.OpenBlobs(filepath.Join(dir, "blobs"))
		if err != nil {
			t.Fatal(err)
		}
		lander, err := landing.New(landing.Config{Store: st, Blobs: blobs})
		if err != nil {
			t.Fatal(err)
		}
		cfg := server.ConnectorConfig{Store: st, Lander: lander, Scheduler: connectors.NewScheduler(st, lander, log), Log: log}
		api = httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
			server.ConnectorRoutes(cfg)))
	}
	stop := func() { api.Close(); _ = st.Close() }
	start()
	t.Cleanup(func() { stop() })
	send := func(method, path string, header http.Header, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, api.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range header {
			req.Header[k] = v
		}
		req.Header.Set("Authorization", "Bearer "+person)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, raw
	}
	keyed := func(key, filename string) http.Header {
		return http.Header{
			server.IdempotencyKey: {key}, "Content-Type": {"text/plain"},
			"Content-Disposition": {`attachment; filename="` + filename + `"`},
		}
	}

	status, raw := send(http.MethodPut, "/api/v1/declarations", http.Header{"Content-Type": {"application/json"}}, `{
		"commit": "0123456789abcdef0123456789abcdef01234567", "replaces": null,
		"connectors": [
			{"name": "exports", "mode": "drop", "source": "bookmarklet-exports", "due_interval": "30d", "zone": "outside"},
			{"name": "feeds", "mode": "poll", "source": "rss-feeds", "schedule": "@hourly", "zone": "outside"}
		]}`)
	if status != http.StatusOK {
		t.Fatalf("apply answered %d %s", status, raw)
	}
	validate(t, schemas, "declarations-applied", raw)

	status, first := send(http.MethodPost, "/api/v1/connectors/exports/drops", keyed("drop-1", "a.txt"), "the first export")
	if status != http.StatusCreated {
		t.Fatalf("a drop answered %d %s", status, first)
	}
	validate(t, schemas, "push", first)
	if status, again := send(http.MethodPost, "/api/v1/connectors/exports/drops", keyed("drop-1", "a.txt"), "the first export"); status != http.StatusOK || !bytes.Equal(again, first) {
		t.Errorf("the drop retried under its key answered %d %s, want 200 and %s", status, again, first)
	}
	if status, raw := send(http.MethodPost, "/api/v1/connectors/exports/drops", keyed("drop-1", "b.txt"), "another export"); status != http.StatusConflict {
		t.Errorf("another file under a used key answered %d %s, want 409", status, raw)
	}
	status, second := send(http.MethodPost, "/api/v1/connectors/exports/drops", keyed("drop-2", "b.txt"), "another export")
	if status != http.StatusCreated {
		t.Fatalf("a second drop answered %d %s", status, second)
	}

	status, raw = send(http.MethodPost, "/api/v1/connectors/feeds/runs", http.Header{server.IdempotencyKey: {"run-1"}}, "")
	var run server.RunAnswer
	if status != http.StatusCreated || json.Unmarshal(raw, &run) != nil || run.TaskID == nil {
		t.Fatalf("a poll's run answered %d %s, want 201 with its task", status, raw)
	}
	validate(t, schemas, "connector-run", raw)
	status, raw = send(http.MethodPost, "/api/v1/connectors/feeds/runs", http.Header{server.IdempotencyKey: {"run-2"}}, "")
	var again server.RunAnswer
	if status != http.StatusOK || json.Unmarshal(raw, &again) != nil || again.TaskID == nil || *again.TaskID != *run.TaskID {
		t.Errorf("a run while the poll is in flight answered %d %s, want 200 naming task %s", status, raw, *run.TaskID)
	}

	var items []string
	for _, answer := range [][]byte{first, second} {
		var pushed server.PushAnswer
		if err := json.Unmarshal(answer, &pushed); err != nil || len(pushed.Items) != 1 {
			t.Fatalf("a drop answered %s, want one item", answer)
		}
		items = append(items, pushed.Items[0].Key)
	}
	if err := st.Update(ctx, func(tx *store.Tx) error {
		for _, key := range items {
			if _, err := tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageQuality, Capability: "model"}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	claim := func(worker string, capabilities ...string) []store.Leased {
		t.Helper()
		leased, err := st.Claim(ctx, store.Claim{Worker: store.Worker{ID: worker, Capabilities: capabilities}, Max: 10, Lease: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		return leased
	}
	if leased := claim("worker-1", "model"); len(leased) != 1 || *leased[0].ItemKey != items[0] {
		t.Fatalf("a claim under the first fill leased %+v, want the first item's task alone", leased)
	}
	status, raw = send(http.MethodPost, "/api/v1/connectors/feeds/hold", nil, "")
	var held server.HoldAnswer
	if status != http.StatusOK || json.Unmarshal(raw, &held) != nil || held.Held != 1 || held.HoldDecidedBy == "" {
		t.Fatalf("holding feeds answered %d %s, want its queued poll held", status, raw)
	}
	validate(t, schemas, "connector-hold", raw)

	stop()
	start()
	status, raw = send(http.MethodGet, "/api/v1/connectors/exports", nil, "")
	var shown server.ConnectorView
	if status != http.StatusOK || json.Unmarshal(raw, &shown) != nil || shown.FirstFill == nil {
		t.Fatalf("show after a restart answered %d %s", status, raw)
	}
	validate(t, schemas, "connectors-list#connector", raw)
	if f := shown.FirstFill; f.State != store.FirstFillHeld || f.Held != 1 || f.ItemKey == nil || *f.ItemKey != items[0] || shown.Waiting != 2 {
		t.Errorf("after a restart the first fill is %+v with %d waiting, want held on %s with 1 behind it and both waiting at the model stage", *f, shown.Waiting, items[0])
	}
	status, raw = send(http.MethodGet, "/api/v1/connectors/feeds", nil, "")
	var feeds server.ConnectorView
	if status != http.StatusOK || json.Unmarshal(raw, &feeds) != nil || feeds.HeldTS == nil || feeds.HeldTasks != 1 {
		t.Errorf("show feeds after a restart answered %d %s, want it held with its poll", status, raw)
	}
	validate(t, schemas, "connectors-list#connector", raw)
	if leased := claim("worker-2", "model", "poll:rss-feeds"); len(leased) != 0 {
		t.Errorf("a claim after a restart leased %+v, want none while the fill and the hold hold", leased)
	}
	status, raw = send(http.MethodPost, "/api/v1/connectors/feeds/release", nil, "")
	var released server.ReleaseAnswer
	if status != http.StatusOK || json.Unmarshal(raw, &released) != nil || released.Released != 1 {
		t.Fatalf("releasing feeds answered %d %s, want its poll returned", status, raw)
	}
	validate(t, schemas, "connector-release", raw)
	if leased := claim("worker-2", "poll:rss-feeds"); len(leased) != 1 || leased[0].ID != *run.TaskID {
		t.Errorf("a claim after the release leased %+v, want the poll task %s", leased, *run.TaskID)
	}

	feedsOnly := `"connectors": [{"name": "feeds", "mode": "poll", "source": "rss-feeds", "schedule": "@hourly", "zone": "outside"}]}`
	jsonBody := http.Header{"Content-Type": {"application/json"}}
	if status, raw := send(http.MethodPut, "/api/v1/declarations", jsonBody, `{"commit": "1111111", "replaces": null, `+feedsOnly); status != http.StatusConflict || !bytes.Contains(raw, []byte(server.CodeDeclarationsMoved)) {
		t.Errorf("an apply that saw no set in force answered %d %s, want 409 %s", status, raw, server.CodeDeclarationsMoved)
	}
	if status, raw := send(http.MethodPut, "/api/v1/declarations", jsonBody, `{"commit": "2222222", "replaces": "0123456789abcdef0123456789abcdef01234567", `+feedsOnly); status != http.StatusOK {
		t.Fatalf("an apply dropping exports answered %d %s", status, raw)
	}
	if status, raw := send(http.MethodPost, "/api/v1/connectors/exports/hold", nil, ""); status != http.StatusOK {
		t.Fatalf("holding a dropped connector answered %d %s", status, raw)
	}
	if err := st.Update(ctx, func(tx *store.Tx) error {
		_, err := tx.Enqueue(ctx, store.Task{ItemKey: &items[0], Stage: store.StageScan, Capability: "scan"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if leased := claim("worker-3", "model", "scan"); len(leased) != 0 {
		t.Errorf("a claim while exports is held leased %+v, want none", leased)
	}
	status, raw = send(http.MethodPost, "/api/v1/connectors/exports/release", nil, "")
	if status != http.StatusOK || json.Unmarshal(raw, &released) != nil || released.Released != 2 {
		t.Fatalf("releasing a dropped connector answered %d %s, want the fill's task and the hold's returned", status, raw)
	}
	leased := claim("worker-3", "model", "scan")
	if len(leased) != 2 || *leased[0].ItemKey != items[1] || leased[0].Stage != store.StageQuality ||
		*leased[1].ItemKey != items[0] || leased[1].Stage != store.StageScan {
		t.Errorf("a claim after the release leased %+v, want the held quality task, then the scan made while exports was held", leased)
	}
}
