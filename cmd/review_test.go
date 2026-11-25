package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/datapointchris/goclikit"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// declarationsEnv names a declaration set's directory, for the test that
// gates the stored Reddit exports under its routing rules. Without it, that
// test skips.
const declarationsEnv = "INGESTION_DECLARATIONS_DIR"

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newGatedPipeline is a pipeline that queues each scan and runs the gates as
// serve does. It applies the declaration set in dir first, and none where dir
// is empty.
func newGatedPipeline(t *testing.T, dir string) *pipeline {
	t.Helper()
	p := newPipeline(t, extract.Builtin(), afterStage(quiet))
	if dir != "" {
		p.apply(t, dir)
	}
	return p
}

// apply applies the declaration set in dir as the first set, and runs the
// gates again as serve does.
func (p *pipeline) apply(t *testing.T, dir string) {
	t.Helper()
	set, err := connectors.Read(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectors.Apply(t.Context(), p.st, set, "test", nil, "person:test", nil, route.AfterApply(quiet)); err != nil {
		t.Fatal(err)
	}
}

// nothingFound is a scan worker that reads each leased task's parse through
// the task parse route and reports a done scan of it with no findings. It
// stands in for yr, which the tests of the gates do not need.
type nothingFound struct{ client *worker.Client }

func (nothingFound) Capability() string { return scan.Capability }

func (h nothingFound) Run(ctx context.Context, task worker.Task) worker.Result {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken)
	if err != nil {
		reason := err.Error()
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: scan.Producer, ProducerVersion: store.ScanVersion("none", parse.Extractor, parse.ExtractorVersion),
		State: store.OutputDone, InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion,
		Findings: []store.Finding{}, Spans: []store.Span{},
	}}}
}

// scanFindingNothing runs every queued scan task through nothingFound, and
// counts the tasks it ran.
func (p *pipeline) scanFindingNothing(t *testing.T) int {
	t.Helper()
	return p.run(t, nothingFound{p.worker})
}

// redditExport is a Reddit export holding saves, each from redditSave.
func redditExport(saves ...string) []byte {
	return []byte(`{"kind":"reddit-saved","exported_at":"2026-10-30T19:18:39Z","items":[` + strings.Join(saves, ",") + `]}`)
}

// redditSave is one self post saved from subreddit, as an export holds it.
func redditSave(name, subreddit string) string {
	post := `{"kind":"t3","data":{"name":"` + name + `","author":"op","subreddit":"` + subreddit + `","title":"A post","selftext":"Its body","is_self":true,` +
		`"permalink":"/r/` + subreddit + `/comments/` + name[3:] + `/a_post/"}}`
	return `{"child":` + post + `,"thread":[{"data":{"children":[` + post + `]}},{"data":{"children":[]}}]}`
}

// landExports lands each export through the reddit-exports connector.
func (p *pipeline) landExports(t *testing.T, exports map[string][]byte) {
	t.Helper()
	for _, name := range slices.Sorted(maps.Keys(exports)) {
		if _, err := redditexport.Land(t.Context(), p.lander, redditexport.Drop{Connector: "reddit-exports", Name: name, Raw: exports[name]}); err != nil {
			t.Fatalf("an export failed to land: %v", errorClass(err))
		}
	}
}

// gateCounts is what the side gate did with the saves: how many there are,
// how many a rule placed, how many wait at a model stage, and how many wait
// in review for each reason.
type gateCounts struct {
	saves, byRule, waiting int
	review                 map[string]int
}

// gated checks that every save with a done scan holds a side decision or
// waits at a model stage, reads the review queue through `review list --json`
// held to its schema, and counts. It names no save and quotes no text,
// because a real export is outsider text.
func (p *pipeline) gated(t *testing.T) gateCounts {
	t.Helper()
	c := gateCounts{review: map[string]int{}}
	c.saves = p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, redditexport.MediaTypeSave)
	c.waiting = p.count(t, `SELECT count(*) FROM items WHERE media_type = ? AND phase = 'waiting'`, redditexport.MediaTypeSave)
	if n := p.count(t, `SELECT count(*) FROM items i WHERE media_type = ? AND phase != 'waiting'
		AND EXISTS (SELECT 1 FROM outputs o WHERE o.item_key = i.key AND o.stage = 'scan' AND o.state = 'done')
		AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.item_key = i.key AND d.gate = 'side')`, redditexport.MediaTypeSave); n != 0 {
		t.Errorf("%d scanned saves hold no side decision and wait at no model stage", n)
	}
	c.byRule = p.count(t, `SELECT count(*) FROM decisions WHERE gate = 'side' AND decided_by LIKE 'rule:%'`)
	out, err := runCLI(t, "review", "list", "--json", "--config", p.config)
	if err != nil {
		t.Fatalf("review list failed: %v", err)
	}
	holdsTo(t, []byte(out), "review-list.schema.json")
	var queue server.ReviewAnswer
	if err := json.Unmarshal([]byte(out), &queue); err != nil {
		t.Fatal(err)
	}
	for _, it := range queue.Items {
		c.review[it.Reason]++
	}
	return c
}

// decisions is the item's decisions as `items show --json` prints them, and
// the newest at the side and read gates.
func (p *pipeline) decisions(t *testing.T, key string) (all []server.DecisionView, side, read server.DecisionView) {
	t.Helper()
	out, err := runCLI(t, "items", "show", key, "--json", "--config", p.config)
	var shown server.ItemAnswer
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil {
		t.Fatalf("items show %s printed %q, %v", key, out, err)
	}
	holdsTo(t, []byte(out), "item.schema.json")
	for _, d := range shown.Decisions {
		switch d.Gate {
		case store.GateSide:
			side = d
		case store.GateRead:
			read = d
		}
	}
	return shown.Decisions, side, read
}

// A save from a subreddit that a rule names is placed by the rule and read as
// is. A save that no rule names waits at the quality gate, and an item whose
// parse failed waits in review. A person places the waiting save with a cause
// and a note, which withdraws its quality task, and a service places the other
// for the person its body names. Each decision records who decided, the
// client, the cause and the note. A second copy of a placement appends
// nothing, a destination the set does not declare is refused, and a drop
// leaves the item dropped.
func TestASaveIsPlacedByItsRuleOrWaitsInReviewForAPersonToPlace(t *testing.T) {
	ctx := t.Context()
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"))})
	unparsable, err := p.lander.Land(ctx, landing.Arrival{
		Connector: "reddit-exports", Zone: store.Outside, MediaType: redditexport.MediaTypeSave, Body: strings.NewReader("not a save"),
	})
	if err != nil {
		t.Fatal(err)
	}
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)

	if c := p.gated(t); c.saves != 3 || c.byRule != 1 || c.waiting != 1 || !maps.Equal(c.review, map[string]int{"no-parse": 1}) {
		t.Fatalf("the gate placed %d of %d saves by rule, left %d waiting and sent %v to review, want 1 of 3, 1 waiting and the no-parse one",
			c.byRule, c.saves, c.waiting, c.review)
	}
	if d, side, read := p.decisions(t, "reddit/t3_aa1"); len(d) != 2 || side.Verdict != "personal" || *side.Destination != "reading-list" ||
		side.DecidedBy != "rule:cooking-subreddits" || read.Verdict != connectors.ReadAsIs {
		t.Errorf("the ruled save's decisions are %+v, want personal on reading-list by its rule, read as-is", d)
	}

	place := []string{"review", "place", "reddit/t3_bb2", "work-inbox", "--cause", "subreddit", "--note", "what would this change?", "--config", p.config}
	out, err := runCLI(t, place...)
	if want := "Placed reddit/t3_bb2 on work-inbox: side fleet, read by a model under the fleet lens.\n"; err != nil || out != want {
		t.Fatalf("review place printed %q, %v; want %q", out, err, want)
	}
	if _, err := runCLI(t, place...); err != nil {
		t.Fatalf("the same placement again failed: %v", err)
	}
	if slices.ContainsFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageQuality }) {
		t.Errorf("after a person placed the save its quality task is still open: %s", jsonOf(p.tasks(t)))
	}
	d, side, read := p.decisions(t, "reddit/t3_bb2")
	if len(d) != 2 || side.DecidedBy != "person:person" || side.Cause != "subreddit" || side.Note == nil || *side.Note != "what would this change?" ||
		side.ClientID == nil || *side.ClientID != "ingestion-cli-test" || read.Lens == nil || *read.Lens != "fleet" || read.Note == nil {
		t.Errorf("after placing twice the save holds %+v, want one person's placement with its cause and note, and a model read carrying the note", d)
	}

	p.provider.Register("ingestion-svc-desk", "desk-secret", server.ScopeReviewRead, server.ScopeReviewPlace)
	desk := (&clientcredentials.Config{
		ClientID: "ingestion-svc-desk", ClientSecret: "desk-secret", TokenURL: p.provider.URL + "/token",
		Scopes: []string{server.ScopeReviewRead, server.ScopeReviewPlace},
	}).Client(ctx)
	post := func(body string) (int, []byte) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.api.URL+"/api/v1/review/"+url.PathEscape(unparsable.Key)+"/place", strings.NewReader(body))
		resp, err := desk.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	if status, _ := post(`{"destination":"reading-list"}`); status != http.StatusBadRequest {
		t.Errorf("a service's placement naming no login answered %d, want 400", status)
	}
	status, raw := post(`{"destination":"reading-list","cause":"title","login":"alice"}`)
	if status != http.StatusOK {
		t.Fatalf("a service's placement for alice answered %d: %s", status, raw)
	}
	holdsTo(t, raw, "review-placement.schema.json")
	var placed server.PlacementAnswer
	_ = json.Unmarshal(raw, &placed)
	if by := placed.Decisions[0]; by.DecidedBy != "person:alice" || by.ClientID == nil || *by.ClientID != "ingestion-svc-desk" {
		t.Errorf("the service's placement was decided by %s from %v, want person:alice from ingestion-svc-desk", by.DecidedBy, by.ClientID)
	}
	if c := p.gated(t); len(c.review) != 0 {
		t.Errorf("after both placements the review queue holds %v, want nothing", c.review)
	}

	_, err = runCLI(t, "review", "place", "reddit/t3_aa1", "nowhere", "--config", p.config)
	if !errors.Is(err, goclikit.ErrUsage) || !strings.Contains(err.Error(), "reading-list") {
		t.Errorf("placing on an undeclared destination answered %v, want a usage error naming the declared ones", err)
	}
	if out, err := runCLI(t, "review", "drop", "reddit/t3_aa1", "--cause", "subreddit", "--config", p.config); err != nil ||
		out != "Dropped reddit/t3_aa1, cause subreddit.\n" || p.phase(t, "reddit/t3_aa1") != "dropped" {
		t.Errorf("review drop printed %q, %v, and left the save %s", out, err, p.phase(t, "reddit/t3_aa1"))
	}
	if out, err := runCLI(t, "review", "drop", "reddit/t3_bb2", "--config", p.config); err != nil || out != "Dropped reddit/t3_bb2, cause unstated.\n" {
		t.Errorf("a drop naming no cause printed %q, %v, want it kept as unstated", out, err)
	}
	if out, err := runCLI(t, "items", "show", "reddit/t3_bb2", "--config", p.config); err != nil || !strings.Contains(out, "drop  by person:") || !strings.Contains(out, "cause unstated") {
		t.Errorf("items show printed %q, %v, want the drop with its unstated cause", out, err)
	}
}

// A save that waits on the gate for want of a rule is placed once a rule or
// the quality gate's answer arrives after its parse: by the rule of a set
// applied later, as on a first deploy that applies its set after the first
// parses, which withdraws its quality task, or by the gate's confident guess.
// An apply that changes nothing for a save appends nothing to it. A save
// that the guess places on the fleet side then waits at its model read.
func TestASaveWaitingOnTheGateIsPlacedByARuleOrGuessThatArrivesLater(t *testing.T) {
	p := newGatedPipeline(t, "")
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(map[string]map[string]any{"bb2": judged(qualitygate.AnswerPass, 0.9, connectors.SideFleet, 0.95)})
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	if c := p.gated(t); c.waiting != 2 || len(c.review) != 0 {
		t.Fatalf("with no set applied %d saves wait and %v are in review, want both waiting at the quality gate", c.waiting, c.review)
	}

	p.apply(t, mustAbs(t, "../examples/declarations"))
	if _, side, read := p.decisions(t, "reddit/t3_aa1"); side.DecidedBy != "rule:cooking-subreddits" || read.Verdict != connectors.ReadAsIs {
		t.Errorf("after the apply the Cooking save is %s by %s and read %s, want placed by its rule and read as-is", side.Verdict, side.DecidedBy, read.Verdict)
	}
	if d, _, _ := p.decisions(t, "reddit/t3_bb2"); len(d) != 0 {
		t.Errorf("the apply appended to the save that no rule names, which holds %+v", d)
	}

	p.run(t, p.qualityGate(t, stand.models(t, "", "")))
	if n := len(stand.starts()); n != 1 {
		t.Errorf("the gate sent %d dispatches, want one for the save no rule names and none for the save the apply placed", n)
	}
	if _, side, read := p.decisions(t, "reddit/t3_bb2"); side.Verdict != connectors.SideFleet || !strings.HasPrefix(side.DecidedBy, "model:") ||
		read.Verdict != connectors.ReadModel {
		t.Errorf("after a confident guess the save is %s by %s and read %s, want fleet by the model's run and read by a model", side.Verdict, side.DecidedBy, read.Verdict)
	}
	if c, at := p.gated(t), p.shown(t, "reddit/t3_bb2").Item.WaitingAt; len(c.review) != 0 || c.waiting != 1 || text(at) != store.StageModelRead {
		t.Errorf("%d saves wait and the review queue holds %v, want the fleet save alone waiting at its model read", c.waiting, c.review)
	}
}

// The stored Reddit exports pass the side gate under the routing rules of the
// declaration set in declarationsEnv, and a rule places at least one save. The exports are outsider text kept out
// of this public repo, so the test reads them only from redditExportsEnv, and
// it reports counts alone.
func TestTheStoredRedditExportsPassTheSideGateUnderTheDeclaredRules(t *testing.T) {
	exportsDir, declarations := os.Getenv(redditExportsEnv), os.Getenv(declarationsEnv)
	if exportsDir == "" || declarations == "" {
		t.Skip(redditExportsEnv + " and " + declarationsEnv + " name no stored Reddit exports and no declaration set")
	}
	files, err := filepath.Glob(filepath.Join(exportsDir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s holds no JSON files: %v", exportsDir, err)
	}
	exports := map[string][]byte{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		exports[strings.TrimSuffix(filepath.Base(path), ".json")] = raw
	}
	p := newGatedPipeline(t, declarations)
	p.landExports(t, exports)
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	c := p.gated(t)
	waiting := 0
	var reasons bytes.Buffer
	for _, reason := range slices.Sorted(maps.Keys(c.review)) {
		waiting += c.review[reason]
		reasons.WriteString(" " + reason + "=" + strconv.Itoa(c.review[reason]))
	}
	if c.byRule == 0 {
		t.Errorf("no rule in the set placed any of the %d saves", c.saves)
	}
	t.Logf("%d exports, %d saves: %d placed by rule, %d sent to review:%s", len(files), c.saves, c.byRule, waiting, reasons.String())
}
