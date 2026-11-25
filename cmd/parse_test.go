package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/schemas"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// redditExportsEnv names a directory of stored Reddit exports, one JSON file
// each, for the test that parses them. Without it that test skips.
const redditExportsEnv = "INGESTION_REDDIT_EXPORTS_DIR"

// pipeline is the service's store, landing and routes, a worker's client, and
// a CLI config that reaches them.
type pipeline struct {
	st       *store.Store
	lander   *landing.Lander
	api      *httptest.Server
	provider *authtest.Provider
	config   string
	person   *http.Client
	// service is the worker's own client, under its service token.
	service *http.Client
	worker  *worker.Client
	// ahead is how far the store's clock runs past the system's.
	ahead *atomic.Int64
}

// advance moves the store's clock on by d, so a task sent back with backoff
// falls due.
func (p *pipeline) advance(d time.Duration) { p.ahead.Add(int64(d)) }

// newPipeline serves the routes with after run in the write that ends each
// item's task done, as serve runs the gates there.
func newPipeline(t *testing.T, extractors *extract.Registry, after server.AfterStage) *pipeline {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	ahead := &atomic.Int64{}
	st, err := store.Open(ctx, filepath.Join(dir, "ingestion.db"),
		store.WithClock(func() time.Time { return time.Now().Add(time.Duration(ahead.Load())) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	blobs, err := landing.OpenBlobs(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	lander, err := landing.New(landing.Config{Store: st, Blobs: blobs, QueueParse: extractors.QueueParse})
	if err != nil {
		t.Fatal(err)
	}
	provider := authtest.NewProvider(t)
	provider.Register("ingestion-svc-worker", "worker-secret", server.ScopeTasksClaim)
	gate := verifiedGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
		server.TaskRoutes(st, log, after), server.BlobRoutes(lander, log), server.ItemRoutes(st, log), server.ReviewRoutes(st, log),
		server.ConnectorRoutes(server.ConnectorConfig{
			Store: st, Lander: lander, Scheduler: connectors.NewScheduler(st, lander, log),
			AfterApply: route.AfterApply(log), Log: log,
		}),
		server.ReplayRoutes(st, replayableStages(st, extractors), log)))
	t.Cleanup(api.Close)
	person := provider.Person(t, "ingestion-cli-test", "person")
	tokenFile, config := filepath.Join(dir, "token.json"), filepath.Join(dir, "config.toml")
	for path, body := range map[string]string{
		tokenFile: `{"access_token":"` + person + `","token_type":"Bearer","expires_in":300}`,
		config: `api = "` + api.URL + `"` + "\n" + `issuer = "` + provider.URL + `"` + "\n" +
			`client_id = "ingestion-cli-test"` + "\n" + `token_command = ["cat", "` + tokenFile + `"]` + "\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	service := (&clientcredentials.Config{
		ClientID: "ingestion-svc-worker", ClientSecret: "worker-secret", TokenURL: provider.URL + "/token",
		Scopes: []string{server.ScopeTasksClaim},
	}).Client(ctx)
	return &pipeline{
		st: st, lander: lander, api: api, provider: provider, config: config, service: service, ahead: ahead,
		worker: worker.NewClient(api.URL, service),
		person: oauth2.NewClient(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: person})),
	}
}

// parse runs every queued parse task through the handlers of extractors, as a
// worker does, and counts the tasks it ran.
func (p *pipeline) parse(t *testing.T, extractors *extract.Registry) int {
	t.Helper()
	return p.run(t, extractors.Handlers(p.worker, slog.New(slog.NewTextHandler(io.Discard, nil)))...)
}

// run claims and runs every task that one of handlers holds the capability
// of, until no claim answers one, and reports each as a worker does. It counts
// the tasks it ran. A task sent back with backoff waits past this run.
func (p *pipeline) run(t *testing.T, workers ...worker.Handler) int {
	t.Helper()
	ctx := t.Context()
	handlers := map[string]worker.Handler{}
	var capabilities []string
	for _, h := range workers {
		handlers[h.Capability()] = h
		capabilities = append(capabilities, h.Capability())
	}
	ran := 0
	for {
		answer, err := p.worker.Claim(ctx, server.ClaimRequest{WorkerID: "test", Capabilities: capabilities, Version: "test", Max: 50, LeaseSeconds: 60})
		if err != nil {
			t.Fatal(err)
		}
		if len(answer.Tasks) == 0 {
			return ran
		}
		for _, task := range answer.Tasks {
			result := handlers[task.Capability].Run(ctx, task)
			if err := p.worker.Report(ctx, task.ID, server.ReportRequest{
				Token: task.LeaseToken, Outcome: result.Outcome, Error: result.Error, Outputs: result.Outputs,
				RetryAfterSeconds: int(result.RetryAfter.Seconds()), Delivery: result.Delivery,
			}); err != nil {
				t.Fatal(err)
			}
			ran++
		}
	}
}

func (p *pipeline) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := p.st.Reader().QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// readParse reads the item's parse through the route, at version where it is
// set, and holds the answer to its schema.
func (p *pipeline) readParse(t *testing.T, key, version string) server.ParseAnswer {
	t.Helper()
	route := p.api.URL + "/api/v1/items/" + url.PathEscape(key) + "/parse"
	if version != "" {
		route += "?version=" + version
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, route, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.person.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the parse of %s at version %q answered %d", key, version, resp.StatusCode)
	}
	holdsTo(t, raw, "item-parse.schema.json")
	var answer server.ParseAnswer
	if err := json.Unmarshal(raw, &answer); err != nil {
		t.Fatal(err)
	}
	return answer
}

// holdsTo fails the test where raw does not hold to the named file under
// schemas/. Every file there is a resource under its $id, so one can refer to
// another.
func holdsTo(t *testing.T, raw []byte, file string) {
	t.Helper()
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	names, err := fs.Glob(schemas.Files, "*.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.AddResource(schemas.Base+name, doc); err != nil {
			t.Fatal(err)
		}
	}
	schema, err := c.Compile(schemas.Base + file)
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(instance); err != nil {
		t.Errorf("the answer does not hold to %s: %v", file, err)
	}
}

// replay posts a replay of parse over connector's items under one
// Idempotency-Key, and answers its status and body.
func (p *pipeline) replay(t *testing.T, idempotencyKey, connector string) (int, server.ReplayAnswer) {
	t.Helper()
	body, _ := json.Marshal(server.ReplayRequest{Stage: store.StageParse, Connector: &connector})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, p.api.URL+"/api/v1/replays", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(server.IdempotencyKey, idempotencyKey)
	resp, err := p.person.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var answer server.ReplayAnswer
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		holdsTo(t, raw, "replay.schema.json")
		_ = json.Unmarshal(raw, &answer)
	}
	return resp.StatusCode, answer
}

func mustAbs(t *testing.T, path string) string {
	t.Helper()
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

// parsedThenReplayed lands each export, parses every save with thread-text
// v1, then replays the parse with v2 through `items replay`. It checks that
// each save holds a done v1 and v2 parse with units, both readable through the
// parse route, and answers the counts. It names no item and quotes no text,
// because a real export is outsider text.
func parsedThenReplayed(t *testing.T, exports map[string][]byte) (saves, units int) {
	t.Helper()
	ctx := t.Context()
	v1 := extract.Builtin()
	p := newPipeline(t, v1, nil)
	for _, name := range slices.Sorted(maps.Keys(exports)) {
		if _, err := redditexport.Land(ctx, p.lander, redditexport.Drop{Connector: "reddit-exports", Name: name, Raw: exports[name]}); err != nil {
			t.Fatalf("an export failed to land: %v", errorClass(err))
		}
	}
	saves = p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, redditexport.MediaTypeSave)
	if queued := p.count(t, `SELECT count(*) FROM tasks WHERE stage = 'parse'`); queued != saves {
		t.Fatalf("landing %d saves queued %d parses, want one each", saves, queued)
	}
	p.parse(t, v1)
	parsed := func(version string) int {
		return p.count(t, `SELECT count(*) FROM outputs o WHERE stage = 'parse' AND producer = 'thread-text' AND producer_version = ? AND state = 'done'
			AND EXISTS (SELECT 1 FROM parse_units u WHERE u.item_key = o.item_key AND u.extractor = o.producer AND u.extractor_version = o.producer_version)`, version)
	}
	if n := parsed("1"); n != saves {
		t.Fatalf("thread-text v1 parsed %d of %d saves with units", n, saves)
	}

	out, err := runCLI(t, "items", "replay", "--stage", "parse", "--connector", "reddit-exports", "--config", p.config)
	if want := fmt.Sprintf("Queued parse on %d items.\n", saves); err != nil || out != want {
		t.Fatalf("items replay printed %q, %v; want %q", out, err, want)
	}
	v2 := extract.ThreadText
	v2.Version = "2"
	next, err := extract.NewRegistry(v2)
	if err != nil {
		t.Fatal(err)
	}
	if ran := p.parse(t, next); ran != saves {
		t.Fatalf("the replay ran %d parses, want %d", ran, saves)
	}
	if n, m := parsed("1"), parsed("2"); n != saves || m != saves {
		t.Fatalf("after the replay %d saves hold v1 and %d hold v2, want %d each", n, m, saves)
	}

	var key string
	if err := p.st.Reader().QueryRowContext(ctx, `SELECT key FROM items WHERE media_type = ? ORDER BY key LIMIT 1`,
		redditexport.MediaTypeSave).Scan(&key); err != nil {
		t.Fatal(err)
	}
	newest, first := p.readParse(t, key, ""), p.readParse(t, key, "1")
	if newest.ExtractorVersion != "2" || first.ExtractorVersion != "1" || newest.Text != first.Text || len(first.Units) == 0 {
		t.Errorf("the parse route read versions %s and %s with %d and %d units, want 2 then 1, each the same text",
			newest.ExtractorVersion, first.ExtractorVersion, len(newest.Units), len(first.Units))
	}
	if records, err := p.st.Item(ctx, key); err != nil || records.Phase() != "parsed" {
		t.Errorf("a parsed save's phase is %q, %v", records.Phase(), err)
	}

	tasks := p.count(t, `SELECT count(*) FROM tasks`)
	if status, answer := p.replay(t, "one-key", "reddit-exports"); status != http.StatusCreated || answer.Items != saves {
		t.Errorf("a replay answered %d with %d items, want 201 and %d", status, answer.Items, saves)
	}
	if status, answer := p.replay(t, "one-key", "reddit-exports"); status != http.StatusOK || answer.Items != saves {
		t.Errorf("the replay sent again answered %d with %d items, want 200 and %d", status, answer.Items, saves)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks`) - tasks; n != saves {
		t.Errorf("a replay and its repeat under one key queued %d tasks, want %d", n, saves)
	}
	if status, _ := p.replay(t, "one-key", "phone"); status != http.StatusConflict {
		t.Errorf("another connector under the same key answered %d, want 409", status)
	}
	return saves, p.count(t, `SELECT count(*) FROM parse_units WHERE extractor_version = '1'`)
}

// errorClass names which kind of failure err is, without its text.
func errorClass(err error) string {
	return fmt.Sprintf("key mismatch %t, missing parent %t", errors.Is(err, landing.ErrKeyMismatch), errors.Is(err, store.ErrNoItem))
}

// Every landed save is queued a parse, and thread-text parses each with its
// units. `items replay` queues every save again, a new version writes beside
// the old, and both read through the parse route. A replay sent again under
// its Idempotency-Key queues nothing more.
func TestLandedSavesParseOnceEachAndAReplayWritesTheNextVersionBeside(t *testing.T) {
	post := func(name string) string {
		return `{"kind":"t3","data":{"name":"` + name + `","author":"op","subreddit":"golang","title":"A post","selftext":"Its body","is_self":true,` +
			`"permalink":"/r/golang/comments/` + name[3:] + `/a_post/"}}`
	}
	thread := func(name string) string {
		return `[{"data":{"children":[` + post(name) + `]}},{"data":{"children":[` +
			`{"kind":"t1","data":{"name":"t1_c1","author":"alice","body":"A comment","replies":""}}]}}]`
	}
	export := `{"kind":"reddit-saved","exported_at":"2026-10-30T19:18:39Z","items":[` +
		`{"child":` + post("t3_aa1") + `,"thread":` + thread("t3_aa1") + `},` +
		`{"child":` + post("t3_bb2") + `,"thread":null,"thread_error":"403"}]}`
	saves, units := parsedThenReplayed(t, map[string][]byte{"first": []byte(export)})
	if saves != 2 || units != 3 {
		t.Errorf("the export parsed as %d saves with %d units, want 2 with 3", saves, units)
	}
}

// An extractor program reads the item's bytes on stdin and writes a unit per
// line, and neither inherits nor can read the worker's environment. One that
// exits non-zero fails its task with its stderr kept, and one that hangs
// fails at its time limit, and each item waits in review. A replay at an
// unchanged version whose rerun fails keeps the done parse, and a replay
// naming an item or connector that is not there is refused.
func TestAnExtractorProgramThatFailsOrHangsFailsItsTaskAndAFailedRerunKeepsTheDoneParse(t *testing.T) {
	ctx := t.Context()
	t.Setenv(clientSecretEnv, "the worker's secret")
	if err := worker.MarkUndumpable(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	script := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	failing := script("failing", "cat >/dev/null\necho 'cannot read page 3' >&2\nexit 3\n")
	hanging := script("hanging", "cat >/dev/null\nexec sleep 60\n")
	working := script("working", `[ -z "$`+clientSecretEnv+`" ] || { echo "the worker's secret reached the program" >&2; exit 9; }`+"\n"+
		`! cat "/proc/$PPID/environ" >/dev/null 2>&1 || { echo "the worker's environment is readable" >&2; exit 8; }`+"\n"+
		"cat >/dev/null\n"+`printf '%s\n' '{"label":"page 1","text":"one"}' '' '{"text":"two"}'`+"\n")
	hangs := extract.Program("hanging", "1", "application/x-hanging", hanging)
	hangs.Timeout = 200 * time.Millisecond
	extractors, err := extract.NewRegistry(
		extract.Program("failing", "1", "application/x-failing", failing), hangs,
		extract.Program("working", "1", "application/x-working", working))
	if err != nil {
		t.Fatal(err)
	}
	p := newPipeline(t, extractors, nil)
	keys := map[string]string{}
	for _, name := range []string{"failing", "hanging", "working"} {
		landed, err := p.lander.Land(ctx, landing.Arrival{
			Connector: "folder", Zone: store.Outside, MediaType: "application/x-" + name, Body: strings.NewReader(name),
		})
		if err != nil {
			t.Fatal(err)
		}
		keys[name] = landed.Key
	}
	p.parse(t, extractors)

	for name, want := range map[string]string{"failing": "failing exited 3: cannot read page 3", "hanging": "hanging ran past its 200ms limit"} {
		records, err := p.st.Item(ctx, keys[name])
		if err != nil {
			t.Fatal(err)
		}
		if task := records.Tasks[0]; task.State != store.TaskFailed || task.Error == nil || *task.Error != want {
			t.Errorf("the %s program's task is %s with error %v, want failed with %q", name, task.State, task.Error, want)
		}
		if phase := p.phase(t, keys[name]); phase != "review" {
			t.Errorf("items show of the %s parse printed phase %q, want review", name, phase)
		}
	}
	if parse := p.readParse(t, keys["working"], ""); parse.Text != "one\n\ntwo" || len(parse.Units) != 2 || *parse.Units[0].Label != "page 1" {
		t.Errorf("the working program's parse is %+v, want its two lines as units", parse)
	}

	script("working", "cat >/dev/null\nexit 137\n")
	if out, err := runCLI(t, "items", "replay", "--stage", "parse", "--key", keys["working"], "--config", p.config); err != nil || out != "Queued parse on 1 items.\n" {
		t.Fatalf("items replay of the working item printed %q, %v", out, err)
	}
	p.parse(t, extractors)
	if parse := p.readParse(t, keys["working"], ""); parse.Text != "one\n\ntwo" {
		t.Errorf("after a failed rerun at an unchanged version the parse is %+v, want the done one kept", parse)
	}
	if phase := p.phase(t, keys["working"]); phase != "parsed" {
		t.Errorf("after a failed rerun the working item's phase is %q, want parsed", phase)
	}
	for _, selection := range [][]string{{"--key", "reddit/t3_nosuch"}, {"--connector", "no-such-connector"}} {
		out, err := runCLI(t, append([]string{"items", "replay", "--stage", "parse", "--config", p.config}, selection...)...)
		if err == nil {
			t.Errorf("items replay %v printed %q and succeeded, want it refused", selection, out)
		}
	}
}

// phase is the item's phase as items show --json prints it.
func (p *pipeline) phase(t *testing.T, key string) string {
	t.Helper()
	out, err := runCLI(t, "items", "show", key, "--json", "--config", p.config)
	var shown server.ItemAnswer
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil {
		t.Fatalf("items show %s printed %q, %v", key, out, err)
	}
	return shown.Item.Phase
}

// The stored Reddit exports parse as the synthetic one does: one thread-text
// parse per save, and a replay's beside it. The exports are outsider text kept
// out of this public repo, so the test reads them only from redditExportsEnv,
// and it reports counts alone.
func TestTheStoredRedditExportsParseOnceEachSaveAndReplayBeside(t *testing.T) {
	exports := storedExports(t)
	saves, units := parsedThenReplayed(t, exports)
	t.Logf("%d exports, %d saves, each parsed by thread-text v1 and v2, %d units at v1", len(exports), saves, units)
}

// storedExports reads each stored Reddit export under redditExportsEnv by its
// name, and skips the test where the variable names no directory.
func storedExports(t *testing.T) map[string][]byte {
	t.Helper()
	dir := os.Getenv(redditExportsEnv)
	if dir == "" {
		t.Skip(redditExportsEnv + " names no directory of stored Reddit exports")
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s holds no JSON files: %v", dir, err)
	}
	exports := map[string][]byte{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		exports[strings.TrimSuffix(filepath.Base(path), ".json")] = raw
	}
	return exports
}
