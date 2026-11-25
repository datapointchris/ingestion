package outbox

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"

	"github.com/datapointchris/ingestion/store"
)

// An item put in by hand gains each stage's record, and its phase follows.
// Every change reaches the collector once, a row's outsider-written fields
// only as ingestion.text, and each body holds to its published schema.
func TestAnItemGainsEachStageRecordAndEachChangeReachesTheCollectorOnce(t *testing.T) {
	ctx := t.Context()
	st := openStore(t)
	collector := newCollector(t)
	relay := openRelay(t, st, collector.url)
	key := "reddit/t3_aq7ggd"
	title, url, text := "Stop writing CRITICAL", "https://www.reddit.com/r/x/comments/aq7ggd/", "the thread's whole text"
	sha := strings.Repeat("a", 64)
	confidence := 0.86
	quality := 7
	side := "fleet"

	steps := []struct {
		phase string
		write func(*store.Tx) error
	}{
		{"landed", func(tx *store.Tx) error {
			if err := tx.PutBlob(ctx, store.Blob{SHA256: sha, MD5: strings.Repeat("b", 32), SizeBytes: 1200, MediaType: "application/json"}); err != nil {
				return err
			}
			if err := tx.PutItem(ctx, store.Item{Key: key, MediaType: "application/json", Zone: store.Outside, BlobSHA256: &sha, Title: &title, URL: &url}); err != nil {
				return err
			}
			if err := tx.PutHome(ctx, store.Home{BlobSHA256: sha, Root: "books", PathSHA256: strings.Repeat("c", 64), Path: "shelf/a.pdf"}, false); err != nil {
				return err
			}
			return tx.PutSighting(ctx, store.Sighting{ItemKey: key, Connector: "reddit-exports", Zone: store.Outside, ExternalKey: "https://redd.it/aq7ggd"})
		}},
		{"parsed", func(tx *store.Tx) error {
			if err := tx.PutOutput(ctx, store.Output{
				ItemKey: key, Stage: store.StageParse, Producer: "thread-text", ProducerVersion: "1", State: store.OutputDone,
			}); err != nil {
				return err
			}
			return tx.PutUnits(ctx, key, "thread-text", "1", []store.Unit{{N: 1, Text: text}})
		}},
		{"scanned", func(tx *store.Tx) error {
			return tx.PutOutput(ctx, store.Output{
				ItemKey: key, Stage: store.StageScan, Producer: "rules", ProducerVersion: "atr@c9d371c",
				State: store.OutputDone, Findings: []store.Finding{{Rule: "shell.command", Severity: "medium"}},
				Spans: []store.Span{{Rule: "shell.command", Span: "sed -i 's/CRITICAL//' CLAUDE.md"}},
			})
		}},
		{"gated", func(tx *store.Tx) error {
			if err := tx.PutOutput(ctx, store.Output{
				ItemKey: key, Stage: store.StageQuality, Producer: "quality-gate", ProducerVersion: "1",
				State: store.OutputDone, Quality: &quality, SideGuess: &side, SideConfidence: &confidence,
				Answer: json.RawMessage(`{"genre":"how-to","idea":"loud markers hurt adherence"}`),
				Tokens: &store.Tokens{UncachedInput: 12, CacheWrite: 3100, Output: 210},
			}); err != nil {
				return err
			}
			_, err := tx.AddDecision(ctx, store.Decision{ItemKey: key, Gate: store.GateQuality, Verdict: "pass", DecidedBy: "model:8812", Confidence: &confidence, Cause: "guess"})
			return err
		}},
		{"placed", func(tx *store.Tx) error {
			_, err := tx.AddDecision(ctx, store.Decision{ItemKey: key, Gate: store.GateSide, Verdict: "fleet", DecidedBy: "model:8812", Confidence: &confidence, Cause: "guess"})
			return err
		}},
		{"read", func(tx *store.Tx) error {
			_, err := tx.AddDecision(ctx, store.Decision{ItemKey: key, Gate: store.GateRead, Verdict: "model", DecidedBy: "rule:default-read", Cause: "rule"})
			return err
		}},
		{"read", func(tx *store.Tx) error {
			dispatch := "dispatch-1"
			return tx.PutOutput(ctx, store.Output{
				ItemKey: key, Stage: store.StageModelRead, Producer: "model-read", ProducerVersion: "1",
				State: store.OutputDone, DispatchID: &dispatch, Answer: json.RawMessage(`{"summary":"s","suggestion":"t"}`),
			})
		}},
		{"delivered", func(tx *store.Tx) error {
			return tx.PutDelivery(ctx, store.Delivery{ItemKey: key, Destination: "reading-list", Outcome: store.DeliveryDelivered, Attempts: 1})
		}},
	}
	for _, step := range steps {
		if err := st.Update(ctx, step.write); err != nil {
			t.Fatalf("%s: %v", step.phase, err)
		}
		records, err := st.Item(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if got := records.Phase(); got != step.phase {
			t.Errorf("after the %s records the item's phase is %q, from %+v", step.phase, got, records.Conditions())
		}
		drain(t, relay)
	}
	if err := st.Update(ctx, func(tx *store.Tx) error {
		_, err := tx.Enqueue(ctx, store.Task{ItemKey: key, Stage: store.StageParse, Capability: "reddit"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	drain(t, relay)
	if err := st.Update(ctx, func(tx *store.Tx) error {
		if err := tx.PutCursor(ctx, store.Cursor{Connector: "reddit-exports", DueAt: store.Stamp(time.Now()), FirstFill: "held", FirstFillItemKey: &key}); err != nil {
			return err
		}
		return tx.AddDeclarations(ctx, store.Declarations{Commit: "a41c9e2", AppliedBy: "person", Connectors: []json.RawMessage{
			json.RawMessage(`{"name":"reddit-exports","mode":"drop","source":"reddit-bookmarklet-exports","zone":"outside","tags":[]}`),
		}})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, store.Claim{Worker: store.Worker{ID: "w1", Capabilities: []string{"reddit"}, Version: "test"}, Max: 1, Lease: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Replay(ctx, store.Replay{ID: strings.Repeat("d", 32), Stage: store.StageParse, RequestedBy: "person:test"},
		func(store.Item) (string, bool) { return "", false }); err != nil {
		t.Fatal(err)
	}
	drain(t, relay)

	got := collector.records()
	seen := map[string]int{}
	for _, r := range got {
		seen[r.fleetKind+" "+r.key()]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s reached the collector %d times, want once", k, n)
		}
	}
	for _, tab := range store.Tables {
		if !collector.has(RecordKind, tab.Name) {
			t.Errorf("no %s row reached the collector as %s", tab.Name, RecordKind)
		}
	}
	// The item changed in place only once, so it ships one version.
	if n := collector.count(RecordKind, "items"); n != 1 {
		t.Errorf("the items table shipped %d records, want the one version", n)
	}
	// Each task change ships a version: the enqueue and the claim.
	if n := collector.count(RecordKind, "tasks"); n != 2 {
		t.Errorf("the tasks table shipped %d records, want the enqueue and the claim", n)
	}

	schemas := compileSchemas(t)
	outsider := []string{title, url, text, "redd.it", "shelf/a.pdf", "sed -i", "loud markers"}
	for _, r := range got {
		tab, _ := store.TableNamed(r.kind)
		if r.service != "ingestion" || r.serviceVersion != "v0.0.0-test" {
			t.Errorf("%s carries service %q at %q", r.key(), r.service, r.serviceVersion)
		}
		if r.fleetKind == RecordKind && r.version != Version(tab, r.stamp(tab)) {
			t.Errorf("%s carries version %q, not its stamp's", r.key(), r.version)
		}
		if r.fleetKind == TextKind && seen[RecordKind+" "+r.key()] != 1 {
			t.Errorf("text %s joins no %s on kind, id and version", r.key(), RecordKind)
		}
		if r.sentTS == "" {
			t.Errorf("%s carries no sent_ts", r.key())
		}
		schema := schemas[r.fleetKind][r.kind]
		instance, err := jsonschema.UnmarshalJSON(strings.NewReader(r.raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(instance); err != nil {
			t.Errorf("%s %s does not hold to its published schema: %v\n%s", r.fleetKind, r.key(), err, r.raw)
		}
		for name := range r.body {
			if tab.IsText(name) != (r.fleetKind == TextKind) {
				t.Errorf("%s %s carries column %s", r.fleetKind, r.key(), name)
			}
		}
		if r.fleetKind == RecordKind {
			for _, words := range outsider {
				if strings.Contains(r.raw, words) {
					t.Errorf("%s %s carries the outsider's %q: %s", RecordKind, r.key(), words, r.raw)
				}
			}
		}
	}
	for _, kind := range []string{"items", "sightings", "homes", "outputs", "parse_units"} {
		if !collector.has(TextKind, kind) {
			t.Errorf("no %s text reached the collector as %s", kind, TextKind)
		}
	}
	if collector.has(TextKind, "tasks") {
		t.Errorf("a task with no error shipped an %s record", TextKind)
	}
}

// A service restarted cleanly ships from its marks and sends nothing again.
// A resend after a crash, which lost the marks, folds in the collector on
// kind, id and version into what it already holds.
func TestARestartShipsFromItsMarksAndAResendFoldsOnKindIDAndVersion(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "ingestion.db")
	st, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	collector := newCollector(t)
	put := func(key string) {
		t.Helper()
		if err := st.Update(ctx, func(tx *store.Tx) error {
			return tx.PutItem(ctx, store.Item{Key: key, MediaType: "text/html", Zone: store.Outside})
		}); err != nil {
			t.Fatal(err)
		}
	}
	put("url/0000000000000001")
	put("url/0000000000000002")
	drain(t, openRelay(t, st, collector.url))
	before := collector.folded()
	if len(before) != 2 {
		t.Fatalf("the first run shipped %d distinct records, want 2", len(before))
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if st, err = store.Open(ctx, path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	sent := len(collector.records())
	drain(t, openRelay(t, st, collector.url))
	if n := len(collector.records()) - sent; n != 0 {
		t.Errorf("a clean restart sent %d records again, want none", n)
	}
	put("url/0000000000000003")
	drain(t, openRelay(t, st, collector.url))
	if n := len(collector.records()) - sent; n != 1 {
		t.Errorf("a restart shipped %d records for one new item, want 1", n)
	}

	// A crash between the collector's acceptance and the mark's write loses
	// the mark, and the relay ships those rows again.
	for _, tab := range store.Tables {
		if err := st.KeepOutboxMark(ctx, store.OutboxMark{Table: tab.Name}, "v0.0.0-test"); err != nil {
			t.Fatal(err)
		}
	}
	sent = len(collector.records())
	drain(t, openRelay(t, st, collector.url))
	if n := len(collector.records()) - sent; n != 3 {
		t.Errorf("a resend after losing the marks sent %d records, want all 3 again", n)
	}
	after := collector.folded()
	if len(after) != 3 {
		t.Errorf("folding on kind, id and version leaves %d records, want 3", len(after))
	}
	for k, body := range before {
		if after[k] != body {
			t.Errorf("%s folded to another body", k)
		}
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "ingestion.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func openRelay(t *testing.T, st *store.Store, url string) *Relay {
	t.Helper()
	r, err := Open(t.Context(), st, url, "v0.0.0-test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func drain(t *testing.T, r *Relay) {
	t.Helper()
	for range 50 {
		if !r.pass(t.Context(), time.Now()) {
			for _, ts := range r.tables {
				if ts.lastErr != nil {
					t.Fatalf("%s: %v", ts.table.Name, ts.lastErr)
				}
			}
			return
		}
	}
	t.Fatal("the relay still had rows waiting after 50 passes")
}

// compileSchemas is each table's published schema, by kind attribute.
func compileSchemas(t *testing.T) map[string]map[string]*jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	out := map[string]map[string]*jsonschema.Schema{RecordKind: {}, TextKind: {}}
	for kind, file := range map[string]string{RecordKind: "ingestion-record", TextKind: "ingestion-text"} {
		path, err := filepath.Abs(filepath.Join("..", "schemas", file+".schema.json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, tab := range store.Tables {
			if kind == TextKind && len(tab.Text) == 0 {
				continue
			}
			s, err := c.Compile(path + "#/$defs/" + tab.Name)
			if err != nil {
				t.Fatalf("compile %s's %s: %v", file, tab.Name, err)
			}
			out[kind][tab.Name] = s
		}
	}
	return out
}

// shipped is one record as the collector received it.
type shipped struct {
	fleetKind, kind, id, version, sentTS string
	service, serviceVersion              string
	raw                                  string
	body                                 map[string]any
}

func (r shipped) key() string { return r.kind + "/" + r.id + "@" + r.version }

func (r shipped) stamp(tab store.Table) string {
	for _, c := range tab.Stamp {
		if s, ok := r.body[c].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// collector is an in-process OTLP/HTTP logs receiver.
type collector struct {
	url string
	mu  sync.Mutex
	got []shipped
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			unzipped, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body = unzipped
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var export collogspb.ExportLogsServiceRequest
		if err := proto.Unmarshal(raw, &export); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var got []shipped
		for _, resource := range export.ResourceLogs {
			res := attributes(resource.GetResource().GetAttributes())
			for _, scope := range resource.ScopeLogs {
				for _, lr := range scope.LogRecords {
					a := attributes(lr.Attributes)
					s := shipped{
						fleetKind: a["fleet.kind"], kind: a["ingestion.record.kind"], id: a["ingestion.record.id"],
						version: a["ingestion.record.version"], sentTS: a["ingestion.record.sent_ts"],
						service: res["service.name"], serviceVersion: res["service.version"], raw: lr.GetBody().GetStringValue(),
					}
					if err := json.Unmarshal([]byte(s.raw), &s.body); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					got = append(got, s)
				}
			}
		}
		c.mu.Lock()
		c.got = append(c.got, got...)
		c.mu.Unlock()
		encoded, _ := proto.Marshal(&collogspb.ExportLogsServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(encoded)
	}))
	t.Cleanup(server.Close)
	c.url = server.URL + "/v1/logs"
	return c
}

func (c *collector) records() []shipped {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]shipped(nil), c.got...)
}

func (c *collector) count(fleetKind, kind string) int {
	n := 0
	for _, r := range c.records() {
		if r.fleetKind == fleetKind && r.kind == kind {
			n++
		}
	}
	return n
}

func (c *collector) has(fleetKind, kind string) bool { return c.count(fleetKind, kind) > 0 }

// folded is every record as a ReplacingMergeTree ordered by kind, id and
// version keeps it: one body per key, the last received.
func (c *collector) folded() map[string]string {
	out := map[string]string{}
	for _, r := range c.records() {
		out[r.fleetKind+" "+r.key()] = r.raw
	}
	return out
}

func attributes(kvs []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, kv := range kvs {
		out[kv.Key] = kv.GetValue().GetStringValue()
	}
	return out
}
