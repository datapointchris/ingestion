package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/freshrss"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/publicweb"
	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
	"github.com/datapointchris/ingestion/ypl"
)

// parseInputSchema is a copy of psyche's schemas/parse-input.schema.json at
// the schema_version that push.SchemaVersion names. A new version is copied in
// once a psyche release reads it.
const parseInputSchema = "../push/testdata/parse-input.schema.json"

// fakePsycheEnv names the directory where the test binary, run as psyche,
// keeps the document it read.
const fakePsycheEnv = "INGESTION_TEST_FAKE_PSYCHE"

func TestMain(m *testing.M) {
	if dir := os.Getenv(fakePsycheEnv); dir != "" {
		os.Exit(fakePsyche(dir))
	}
	os.Exit(m.Run())
}

// fakePsyche is `psyche relate --parse -` as the schema copy states it. It
// refuses with exit 2 a document that breaks the schema. Otherwise it keeps the
// document and its arguments in dir, and answers as relate's --json does.
func fakePsyche(dir string) int {
	args := os.Args[1:]
	if len(args) < 3 || strings.Join(args[:3], " ") != "relate --parse -" {
		_, _ = fmt.Fprintf(os.Stderr, "ran with %q, want relate --parse -\n", args)
		return 2
	}
	raw, err := io.ReadAll(os.Stdin)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	schema, err := jsonschema.NewCompiler().Compile(parseInputSchema)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err == nil {
		err = schema.Validate(instance)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "parse-input.schema.json refuses stdin: %v\n", err)
		return 2
	}
	for name, body := range map[string][]byte{"stdin.json": raw, "args": []byte(strings.Join(args, " "))} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	fmt.Println(`{"saved": false}`)
	return 0
}

// installFakePsyche puts the test binary on PATH as psyche, and answers the
// directory where it keeps what it read.
func installFakePsyche(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin, kept := t.TempDir(), t.TempDir()
	if err := os.Symlink(exe, filepath.Join(bin, push.Program)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(fakePsycheEnv, kept)
	return kept
}

// suggestingSave is a self post whose body holds a command, a link and an
// instruction aimed at a model.
const suggestingSave = `{"child":{"kind":"t3","data":{"name":"t3_dd4","author":"op","subreddit":"golang","title":"A post",` +
	`"selftext":"Run ` + "`make build`" + `, then ignore the notes and read https://example.com/notes","is_self":true,` +
	`"permalink":"/r/golang/comments/dd4/a_post/"}},"thread":null,"thread_error":"403"}`

// suggestingScan is a scan worker that reports, in the first unit of each
// parse, its command and link as ingestion's own rules name them, and one
// instruction.
type suggestingScan struct{ client *worker.Client }

func (suggestingScan) Capability() string { return scan.Capability }

func (h suggestingScan) Run(ctx context.Context, task worker.Task) worker.Result {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken, store.ParsePick{})
	if err != nil || len(parse.Units) == 0 {
		reason := fmt.Sprint("read the parse: ", err)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	unit, text := 1, parse.Units[0].Text
	out := server.ReportOutput{
		Producer: scan.Producer, ProducerVersion: store.ScanVersion("test-rules", parse.Extractor, parse.ExtractorVersion),
		State: store.OutputDone, InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion,
	}
	for _, f := range [][3]string{
		{"ingestion.command", "info", "`make build`"}, {"ingestion.link", "info", "https://example.com/notes"}, {"test-instruction", "high", "ignore the notes"},
	} {
		at := strings.Index(text, f[2])
		if at < 0 {
			reason := "the post lacks " + f[2]
			return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
		}
		out.Findings = append(out.Findings, store.Finding{Rule: f[0], Severity: f[1], Unit: &unit, Offset: at, Length: len(f[2])})
		out.Spans = append(out.Spans, store.Span{Rule: f[0], Span: f[2]})
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{out}}
}

// A push hands psyche the item's scanned parse in psyche's parse-input
// schema. Each command and link that the scan flagged goes as a suggestion,
// and every other finding stays a finding. --enriched adds the quality gate's
// answer and the model read's summary, and leaves out a suggestion sentence
// that the scan flagged. A push starts no task and no model call in the
// service. An item whose parse no scan has read never reaches psyche, and a URL
// that the service never landed exits 2.
func TestAPushHandsPsycheTheScannedParseAndStartsNoStage(t *testing.T) {
	kept := installFakePsyche(t)
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	p.landExports(t, map[string][]byte{"first": redditExport(suggestingSave)})
	p.parse(t, extract.Builtin())
	const key = "reddit/t3_dd4"

	_, err := runCLI(t, "push", key, "--to", "psyche", "--relate", "--wait", "0", "--config", p.config)
	if code := exitCode(io.Discard, err); code != 1 {
		t.Errorf("a push before the scan exited %d (%v), want 1", code, err)
	}
	items := p.count(t, `SELECT count(*) FROM items`)
	_, err = runCLI(t, "push", "https://www.reddit.com/r/golang/comments/zz9/unseen/", "--to", "psyche", "--relate", "--config", p.config)
	if code := exitCode(io.Discard, err); code != 2 || p.count(t, `SELECT count(*) FROM items`) != items {
		t.Errorf("a push of a link that no extractor reads exited %d (%v), or stored an item, want 2 and nothing stored", code, err)
	}
	if _, err := os.Stat(filepath.Join(kept, "stdin.json")); err == nil {
		t.Fatal("psyche ran on a parse that no scan has read, or on a link the service did not land")
	}
	p.run(t, suggestingScan{p.worker})
	p.run(t, p.qualityGate(t, stand.models(t, "", "")))
	tasks, dispatches := p.count(t, `SELECT count(*) FROM tasks`), len(stand.starts())

	read := func(t *testing.T, args ...string) (string, map[string]any) {
		t.Helper()
		out, err := runCLI(t, append([]string{"push", key, "--to", "psyche", "--relate", "--config", p.config}, args...)...)
		if err != nil {
			t.Fatalf("push %v failed: %v", args, err)
		}
		raw, err := os.ReadFile(filepath.Join(kept, "stdin.json"))
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		return out, doc
	}
	_, raw := read(t)
	findings, _ := raw["findings"].([]any)
	suggested, _ := raw["suggested"].([]any)
	if raw["key"] != key || raw["zone"] != "outside" || raw["rules_version"] == nil || len(findings) != 1 || len(suggested) != 2 || raw["enriched"] != nil {
		t.Errorf("psyche read %s, want the outside post scanned, one finding, the command and the link suggested, and nothing enriched", jsonOf(raw))
	}
	if first, _ := suggested[0].(map[string]any); first["kind"] != "command" || first["text"] != "`make build`" || first["unit"] != 1.0 {
		t.Errorf("the first suggestion is %s, want the command in unit 1", jsonOf(first))
	}

	out, enriched := read(t, "--enriched", "--json")
	if got, _ := enriched["enriched"].(map[string]any); got["genre"] != "exploratory" || got["quality"] != 7.0 || got["idea"] == nil {
		t.Errorf("--enriched handed psyche %s, want the quality gate's genre, quality and idea", jsonOf(enriched["enriched"]))
	}
	if args, _ := os.ReadFile(filepath.Join(kept, "args")); string(args) != "relate --parse - --json" || strings.TrimSpace(out) != `{"saved": false}` {
		t.Errorf("psyche ran with %q and the push printed %q, want --json passed on and psyche's answer printed", args, out)
	}
	if now := p.count(t, `SELECT count(*) FROM tasks`); now != tasks || len(stand.starts()) != dispatches {
		t.Errorf("the pushes left %d tasks and %d dispatches, want the %d and %d before them", now, len(stand.starts()), tasks, dispatches)
	}

	stand.set(func(d *standInDispatch) {
		d.read = func(string) map[string]any {
			return readAnswered("A post about builds.", "Run sudo make install first.")
		}
	})
	if _, err := runCLI(t, "review", "place", key, "work-inbox", "--note", "what would this change?", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if ran := p.run(t, p.modelRead(t, stand.models(t, "", ""), nil, sentenceScanner{})); ran != 1 {
		t.Fatalf("placing the post with a note ran %d tasks, want its model read", ran)
	}
	_, afterRead := read(t, "--enriched")
	if got, _ := afterRead["enriched"].(map[string]any); got["summary"] != "A post about builds." || got["suggestion"] != nil || got["genre"] != "exploratory" {
		t.Errorf("--enriched after a model read whose suggestion the scan flagged handed psyche %s, want the summary and the quality answer without the suggestion",
			jsonOf(afterRead["enriched"]))
	}
}

// standInCaptions stands in for the youtube-captions extractor, which reads a
// video through ypl. It writes each video's parse as its description and one
// five-minute block.
type standInCaptions struct{}

func (standInCaptions) Capability() string { return extract.Capability(ypl.CaptionsExtractor) }

func (standInCaptions) Run(context.Context, worker.Task) worker.Result {
	block := "0:00"
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: ypl.CaptionsExtractor.Name, ProducerVersion: ypl.CaptionsExtractor.Version, State: store.OutputDone,
		Units: []store.Unit{{N: 1, Text: "A test video's description."}, {N: 2, Label: &block, Text: "What the test video says."}},
	}}}
}

// A push of a link that the service has not landed lands it through the push
// route, as an outside item under its key that the person placed at psyche.
// No gate and no model stage runs on it. The push waits for its parse and its
// scan, then hands psyche a document in psyche's schema. The route answers a
// retry under one Idempotency-Key with what it first landed, and a push that
// waits past its deadline exits naming the stage it waited at.
func TestAPushOfANewLinkLandsItPlacedAtPsycheAndWaitsForItsScan(t *testing.T) {
	kept := installFakePsyche(t)
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	const link, key = "https://youtu.be/pushedvid01?si=shared", "youtube/pushedvid01"

	type ended struct{ err error }
	done := make(chan ended, 1)
	go func() {
		_, err := runCLI(t, "push", link, "--to", "psyche", "--relate", "--config", p.config)
		done <- ended{err}
	}()
	for landedBy := time.Now().Add(10 * time.Second); p.count(t, `SELECT count(*) FROM items WHERE key = ?`, key) == 0; {
		select {
		case e := <-done:
			t.Fatalf("the push ended before its link landed: %v", e.err)
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(landedBy) {
			t.Fatal("the push landed no item")
		}
	}
	if ran := p.run(t, standInCaptions{}); ran != 1 {
		t.Fatalf("%d captions parses ran, want the pushed video's", ran)
	}
	if ran := p.scanFindingNothing(t); ran != 1 {
		t.Fatalf("%d scans ran, want the pushed video's", ran)
	}
	select {
	case e := <-done:
		if e.err != nil {
			t.Fatalf("the push failed: %v", e.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the push went on waiting after its parse was scanned")
	}
	raw, err := os.ReadFile(filepath.Join(kept, "stdin.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc push.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Key != key || doc.Zone != store.Outside || doc.URL == nil || *doc.URL != ypl.WatchURL("pushedvid01") ||
		doc.Extractor != ypl.CaptionsExtractor.Name || len(doc.Units) != 2 || doc.RulesVersion == nil {
		t.Errorf("psyche read %s, want the outside video's scanned captions parse under the watch URL that ypl's poll lands", jsonOf(doc))
	}
	if n := p.count(t, `SELECT count(*) FROM items i JOIN sightings s ON s.item_key = i.key
		WHERE i.key = ? AND i.zone = 'outside' AND i.media_type = ? AND s.connector = 'cli-push'`, key, ypl.MediaTypeVideo); n != 1 {
		t.Errorf("%d outside videos sighted by cli-push hold the key, want 1", n)
	}
	rows, err := p.st.Reader().QueryContext(t.Context(),
		`SELECT gate || ' ' || verdict || ' ' || cause || ' ' || coalesce(destination, '-') || ' ' || substr(decided_by, 1, 7)
		FROM decisions WHERE item_key = ? ORDER BY gate DESC, sequence`, key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var decisions []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		decisions = append(decisions, d)
	}
	if want := []string{"side personal push psyche person:", "read as-is push - person:"}; !slices.Equal(decisions, want) || rows.Err() != nil {
		t.Errorf("the pushed video holds decisions %q, want the person's %q alone", decisions, want)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage NOT IN ('parse', 'scan')`, key) +
		p.count(t, `SELECT count(*) FROM deliveries WHERE item_key = ?`, key); n != 0 {
		t.Errorf("the pushed video holds %d tasks past its scan or deliveries, want none", n)
	}

	post := func(t *testing.T, idempotencyKey, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, p.api.URL+"/api/v1/items", strings.NewReader(body))
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
		return resp.StatusCode, raw
	}
	const second = `{"connector": "cli-push", "url": "https://www.youtube.com/watch?v=pushedvid02", "destination": "psyche"}`
	status, first := post(t, "retry-1", second)
	holdsTo(t, first, "push.schema.json")
	landed := p.count(t, `SELECT (SELECT count(*) FROM items) + (SELECT count(*) FROM sightings) + (SELECT count(*) FROM tasks) +
		(SELECT count(*) FROM decisions)`)
	again, retried := post(t, "retry-1", second)
	if status != http.StatusCreated || again != http.StatusOK || string(retried) != string(first) || !strings.Contains(string(first), `"created":true`) {
		t.Errorf("a push and its retry answered %d %s and %d %s, want 201 creating the item, then 200 with the same answer", status, first, again, retried)
	}
	if now := p.count(t, `SELECT (SELECT count(*) FROM items) + (SELECT count(*) FROM sightings) + (SELECT count(*) FROM tasks) +
		(SELECT count(*) FROM decisions)`); now != landed {
		t.Errorf("the retry wrote %d rows, want none", now-landed)
	}
	if status, _ := post(t, "retry-1", `{"connector": "cli-push", "url": "https://www.youtube.com/watch?v=pushedvid03"}`); status != http.StatusConflict {
		t.Errorf("another link under the same Idempotency-Key answered %d, want 409", status)
	}

	_, err = runCLI(t, "push", "youtube/pushedvid02", "--to", "psyche", "--relate", "--wait", "300ms", "--config", p.config)
	if code := exitCode(io.Discard, err); code != 1 || !strings.Contains(fmt.Sprint(err), "still waits at its parse") {
		t.Errorf("a push past its deadline exited %d (%v), want 1 naming the parse it waited at", code, err)
	}

	// A parse whose task failed with no output, as one whose leases lapsed
	// does, ends the wait at once rather than at its deadline.
	if ran := p.run(t, failingCaptions{}); ran != 1 {
		t.Fatalf("%d captions parses ran, want the second video's", ran)
	}
	started := time.Now()
	_, err = runCLI(t, "push", "youtube/pushedvid02", "--to", "psyche", "--relate", "--wait", "30s", "--config", p.config)
	if code := exitCode(io.Discard, err); code != 1 || !strings.Contains(fmt.Sprint(err), "parse failed: "+failingCaptionsError) ||
		time.Since(started) > 10*time.Second {
		t.Errorf("a push on a parse that failed with no output exited %d after %s (%v), want 1 at once with the task's error", code, time.Since(started), err)
	}

	// psyche takes only a push's placement, so review neither offers nor takes it.
	_, err = runCLI(t, "review", "place", "youtube/pushedvid02", "psyche", "--config", p.config)
	if code := exitCode(io.Discard, err); code != 2 || !strings.Contains(fmt.Sprint(err), "only a push places an item at psyche") {
		t.Errorf("a placement from review at psyche exited %d (%v), want 2 naming the push", code, err)
	}
}

// A pushed link to a web page lands one outside text/html item under its
// link's url key, holding no bytes. The worker's parse GETs the page through
// publicweb, and the service keeps it as the item's bytes and its title as the
// item's. The same page from a FreshRSS feed adds that feed's sighting to the
// one item and queues no second parse. A page that refuses its GET fails the
// parse, and `ingestion push` prints why. A refusal that may clear fetches the
// page again later.
func TestAPushedPageLandsOnceAndItsParseFetchesIt(t *testing.T) {
	stand, api := newStandInFreshRSS(t)
	pages := newStandInPages(t, []int{6}, []int{8})
	p, extractors := newFreshRSSPipeline(t)
	// meanwhile runs inside the parse's GET of /post/10, which then answers a
	// page that differs from the one that a GET before it answered.
	var meanwhile func()
	racing := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/post/10" || meanwhile == nil {
			return standInPagesAlone(pages).RoundTrip(r)
		}
		meanwhile()
		meanwhile = nil
		changed := strings.Replace(standInPage(10), "Page text", "Changed text", 1)
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/html"}}, Body: io.NopCloser(strings.NewReader(changed)), Request: r}, nil
	})
	parse := extractors.Handlers(p.worker, publicweb.NewReader(&http.Client{Timeout: 10 * time.Second, Transport: racing}), quiet)
	link := func(n int) string { return fmt.Sprintf("%s/post/%d", pages.URL, n) }
	keyOfLink := func(link string) string {
		key, err := itemkey.FromURL(link)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	keyOf := func(n int) string { return keyOfLink(link(n)) }
	pushLink := func(t *testing.T, link, wait string) error {
		t.Helper()
		_, err := runCLI(t, "push", link, "--to", "psyche", "--relate", "--wait", wait, "--config", p.config)
		return err
	}
	pushOf := func(t *testing.T, n int, wait string) error {
		t.Helper()
		return pushLink(t, link(n), wait)
	}

	if code := exitCode(io.Discard, pushOf(t, 1, "0")); code != 1 {
		t.Fatalf("a push of a page with --wait 0 exited %d, want 1 waiting at its parse", code)
	}
	landed := p.item(t, keyOf(1)).Item
	if landed.MediaType != extract.MediaTypeHTML || landed.Zone != store.Outside || landed.BlobSHA256 != nil || landed.URL == nil || *landed.URL != link(1) {
		t.Errorf("the pushed page landed as %s, want an outside text/html item under its link with no bytes", jsonOf(landed))
	}
	if ran := p.run(t, parse...); ran != 1 {
		t.Fatalf("%d parses ran, want the pushed page's", ran)
	}
	fetched := p.item(t, keyOf(1)).Item
	if p.blobText(t, fetched.BlobSHA256) != standInPage(1) || fetched.Title == nil || *fetched.Title != "Test entry 1" {
		t.Errorf("the parse kept %s, want the page as the item's bytes and its title, cut of the site's name, as the item's", jsonOf(fetched))
	}
	if parsed := p.readParse(t, keyOf(1), ""); parsed.Extractor != extract.ArticleTrafilatura.Name || len(parsed.Units) != 1 ||
		parsed.Units[0].Text != "Page text of test entry 1." {
		t.Errorf("the page parsed as %s, want article-trafilatura's one paragraph", jsonOf(parsed))
	}

	poll := &recording{Handler: polling.Handler(freshrss.Source, newStandInPoller(api, pages), p.worker, quiet)}
	stand.add(pages, time.Now().Add(-2*time.Hour), "Uncategorized", 2)
	p.runConnector(t, "freshrss")
	p.run(t, poll)
	stand.add(pages, time.Now().Add(-time.Hour), "Uncategorized", 1)
	p.runConnector(t, "freshrss")
	p.run(t, poll)
	if got := poll.landedKeys(t); !slices.Equal(got, []string{keyOf(1)}) {
		t.Fatalf("the feed's poll landed %q, want the pushed page's key", got)
	}
	if n := p.count(t, `SELECT count(*) FROM sightings WHERE item_key = ? AND connector IN ('cli-push', 'freshrss')`, keyOf(1)); n != 2 ||
		p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'parse'`, keyOf(1)) != 1 {
		t.Errorf("the page holds %d of the push's and the feed's sightings, want both on one item with one parse", n)
	}

	_ = pushOf(t, 6, "0")
	if ran := p.run(t, parse...); ran != 1 {
		t.Fatalf("%d parses ran, want the refused page's", ran)
	}
	err := pushOf(t, 6, "30s")
	if code := exitCode(io.Discard, err); code != 1 || !strings.Contains(fmt.Sprint(err), "parse failed: the page was not fetched: HTTP 403") {
		t.Errorf("a push of a page that answered 403 exited %d (%v), want 1 naming the refusal", code, err)
	}
	// A feed that the person follows carries the page whose GET was refused.
	stand.add(pages, time.Now(), "Uncategorized", 6)
	p.runConnector(t, "freshrss")
	p.run(t, poll)
	if ran := p.run(t, parse...); ran != 1 {
		t.Fatalf("%d parses ran once the feed landed the refused page's content, want its failed parse run again", ran)
	}
	if parsed := p.readParse(t, keyOf(6), ""); len(parsed.Units) != 1 || parsed.Units[0].Text != "Feed content of test entry 6." {
		t.Errorf("the refused page parsed as %s, want the feed's paragraph", jsonOf(parsed))
	}

	// The phone shares a link that serves a PDF, as a paper's link does.
	paper := pages.URL + "/post/9.pdf"
	_ = pushLink(t, paper, "0")
	if ran := p.run(t, parse...); ran != 2 {
		t.Fatalf("%d parses ran on a pushed link to a PDF, want the page's parse that hands it on and the PDF's", ran)
	}
	handed := p.item(t, keyOfLink(paper)).Item
	if handed.MediaType != extract.MediaTypePDF || p.blobText(t, handed.BlobSHA256) != standInPDF(9) {
		t.Errorf("the pushed PDF kept %s, want the PDF as the item's bytes and its type", jsonOf(handed))
	}
	if parsed := p.readParse(t, keyOfLink(paper), ""); parsed.Extractor != extract.PDFText.Name || len(parsed.Units) != 1 {
		t.Errorf("the pushed PDF parsed as %s, want pdf-text's one page", jsonOf(parsed))
	}

	// The feed lands the pushed page while the parse GETs it.
	meanwhile = func() {
		stand.add(pages, time.Now(), "Uncategorized", 10)
		p.runConnector(t, "freshrss")
		p.run(t, poll)
	}
	_ = pushOf(t, 10, "0")
	if ran := p.run(t, parse...); ran != 2 {
		t.Fatalf("%d parses ran where the feed landed the page during the parse's GET, want that parse and one over the feed's page", ran)
	}
	if raced := p.item(t, keyOf(10)).Item; p.blobText(t, raced.BlobSHA256) != standInPage(10) ||
		p.readParse(t, keyOf(10), "").Units[0].Text != "Page text of test entry 10." {
		t.Errorf("the page that a feed landed during the parse's GET parsed as %s, want the bytes that the item holds",
			jsonOf(p.readParse(t, keyOf(10), "")))
	}

	_ = pushOf(t, 8, "0")
	if ran := p.run(t, parse...); ran != 1 || p.item(t, keyOf(8)).Item.BlobSHA256 != nil {
		t.Fatalf("%d parses ran on a page that answered 503, want one that kept nothing", ran)
	}
	p.advance(15*time.Minute + time.Second)
	if ran := p.run(t, parse...); ran != 1 || p.blobText(t, p.item(t, keyOf(8)).Item.BlobSHA256) != standInPage(8) {
		t.Errorf("%d parses ran once the 503's wait passed, want one that kept the page", ran)
	}
}

const failingCaptionsError = "the stand-in read failed"

// failingCaptions fails a video's captions parse and writes no output, as a
// task does whose leases lapsed.
type failingCaptions struct{}

func (failingCaptions) Capability() string { return extract.Capability(ypl.CaptionsExtractor) }

func (failingCaptions) Run(context.Context, worker.Task) worker.Result {
	reason := failingCaptionsError
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
}
