package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"
	"github.com/datapointchris/goclikit"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// dispatchClient is the service client that a worker dispatches to the
// stand-in dispatch service as, with dispatchSecret.
const (
	dispatchClient = "dispatch-svc-ingestion"
	dispatchSecret = "dispatch-secret"
)

// standInDispatch answers a dispatch service's three routes as its published
// shapes state them. POST /api/v1/dispatches/ starts one dispatch under each
// Idempotency-Key and answers the same dispatch to the key again. GET
// /api/v1/dispatches/{id}/ reads it, and GET /api/v1/outside-text-answers/{id}
// answers its run once it finished. While hold is set, every dispatch waits
// with hold as its note. Once hold clears, a read finishes it.
type standInDispatch struct {
	*httptest.Server
	mu sync.Mutex
	// hold is the waiting note of every unfinished dispatch, and "" to answer.
	hold string
	// fail is the failure code that the next run to finish ends with.
	fail string
	// answer is the structured output of a run on prompt.
	answer     func(prompt string) map[string]any
	dispatches map[string]*standInRun
	byKey      map[string]string
	started    []dispatchStart
	reads      int
}

// standInRun is one dispatch and, once finished, its stored answer.
type standInRun struct {
	id, prompt, model, effort string
	finished                  time.Time
	answer                    map[string]any
}

// dispatchStart is what one new dispatch asked for.
type dispatchStart struct {
	Key, Agent, Model, Effort string
}

func newStandInDispatch(t *testing.T, provider *authtest.Provider) *standInDispatch {
	t.Helper()
	provider.Register(dispatchClient, dispatchSecret, modelport.QueueScopes...)
	verifier := provider.Verifier(t, "dispatch-cli-", "dispatch-svc-")
	d := &standInDispatch{dispatches: map[string]*standInRun{}, byKey: map[string]string{}}
	authorized := func(w http.ResponseWriter, r *http.Request) bool {
		token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		caller, err := verifier.Verify(r.Context(), token)
		if !bearer || err != nil || slices.ContainsFunc(modelport.QueueScopes, func(s string) bool { return !slices.Contains(caller.Scopes, s) }) {
			writeDispatch(w, http.StatusUnauthorized, map[string]string{"error": "Unauthorized", "message": "no token", "code": "unauthorized"})
			return false
		}
		return true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/dispatches/{$}", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		key := r.Header.Get("Idempotency-Key")
		var body struct {
			Agent  string `json:"agent"`
			Inputs struct {
				Prompt string         `json:"prompt"`
				Schema map[string]any `json:"schema"`
			} `json:"inputs"`
			Model  string `json:"model"`
			Effort string `json:"effort"`
		}
		if key == "" || json.NewDecoder(r.Body).Decode(&body) != nil || body.Inputs.Prompt == "" || body.Inputs.Schema == nil {
			writeDispatch(w, http.StatusBadRequest, map[string]string{"error": "Bad Request", "message": "a dispatch names its key, prompt and schema", "code": "invalid_body"})
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if id, ok := d.byKey[key]; ok {
			writeDispatch(w, http.StatusAccepted, d.view(d.dispatches[id]))
			return
		}
		run := &standInRun{id: fmt.Sprintf("dispatch-%d", len(d.dispatches)+1), prompt: body.Inputs.Prompt, model: body.Model, effort: body.Effort}
		d.dispatches[run.id], d.byKey[key] = run, run.id
		d.started = append(d.started, dispatchStart{Key: key, Agent: body.Agent, Model: body.Model, Effort: body.Effort})
		d.settle(run)
		writeDispatch(w, http.StatusAccepted, d.view(run))
	})
	mux.HandleFunc("GET /api/v1/dispatches/{id}/{$}", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		d.reads++
		run, ok := d.dispatches[r.PathValue("id")]
		if !ok {
			writeDispatch(w, http.StatusNotFound, map[string]string{"error": "Not Found", "message": "no dispatch", "code": "dispatch_not_found"})
			return
		}
		d.settle(run)
		writeDispatch(w, http.StatusOK, d.view(run))
	})
	mux.HandleFunc("GET /api/v1/outside-text-answers/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		run, ok := d.dispatches[r.PathValue("id")]
		if !ok || run.answer == nil {
			writeDispatch(w, http.StatusNotFound, map[string]string{"error": "Not Found", "message": "no answer", "code": "answer_not_found"})
			return
		}
		writeDispatch(w, http.StatusOK, run.answer)
	})
	d.Server = httptest.NewServer(mux)
	t.Cleanup(d.Close)
	return d
}

// settle finishes run where no hold stands, with its answer or the failure
// that fail names. The caller holds mu.
func (d *standInDispatch) settle(run *standInRun) {
	if !run.finished.IsZero() || d.hold != "" {
		return
	}
	run.finished = time.Now().UTC()
	run.answer = map[string]any{
		"run": "run-" + strings.TrimPrefix(run.id, "dispatch-"), "model": run.model, "effort": run.effort,
		"tokens": map[string]int{"input": 12, "cache_write": 3400, "cache_read": 0, "output": 180},
	}
	if d.fail != "" {
		run.answer["state"], run.answer["failure"], run.answer["failure_detail"] = modelport.Failed, d.fail, "claude exited 1"
		d.fail = ""
		return
	}
	run.answer["state"], run.answer["structured_output"] = modelport.Succeeded, d.answer(run.prompt)
}

// view is run as the service answers a dispatch.
func (d *standInDispatch) view(run *standInRun) map[string]any {
	v := map[string]any{"id": run.id, "finished": run.finished, "dropped": []string{}}
	if run.finished.IsZero() {
		v["waiting"] = d.hold
	}
	return v
}

// set changes the stand-in under its lock.
func (d *standInDispatch) set(change func(d *standInDispatch)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	change(d)
}

// starts is every new dispatch so far.
func (d *standInDispatch) starts() []dispatchStart {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.started)
}

// readCount is how many times a dispatch was read.
func (d *standInDispatch) readCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads
}

func writeDispatch(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// models is a [model] table that dispatches to d, asking for model and effort.
func (d *standInDispatch) models(t *testing.T, model, effort string) modelConfig {
	t.Helper()
	t.Setenv(modelSecretEnv, dispatchSecret)
	var cfg modelConfig
	cfg.Adapter = modelport.AdapterQueue
	cfg.Queue.API, cfg.Queue.ClientID = d.URL, dispatchClient
	cfg.Quality = stageModel{Model: model, Effort: effort}
	return cfg
}

// qualityGate is the quality handler that a worker opens from models, and nil
// where it opens none.
func (p *pipeline) qualityGate(t *testing.T, models modelConfig) worker.Handler {
	t.Helper()
	handlers := workerHandlers(p.worker, nil, openModelPort(t.Context(), models, p.provider.URL, quiet), models, quiet)
	i := slices.IndexFunc(handlers, func(h worker.Handler) bool { return h.Capability() == qualitygate.Capability })
	if i < 0 {
		return nil
	}
	return handlers[i]
}

// answerBy answers the save whose permalink holds each key of answers with
// its answer, and passes any other.
func answerBy(answers map[string]map[string]any) func(prompt string) map[string]any {
	return func(prompt string) map[string]any {
		for id, a := range answers {
			if strings.Contains(prompt, "/comments/"+id+"/") {
				return a
			}
		}
		return judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.9)
	}
}

// judged is an answer holding to prompts/quality/answer.schema.json.
func judged(verdict string, sure float64, side string, sideSure float64) map[string]any {
	return map[string]any{
		"genre": "exploratory", "domains": []string{"software"}, "themes": []string{"a theme"}, "quality": 7,
		"idea": "The " + verdict + " idea in one line.", "verdict": verdict, "verdict_confidence": sure,
		"side": side, "side_confidence": sideSure,
	}
}

// A save that no rule names waits at the quality gate. A worker whose config
// names no model adapter claims no quality task and starts no call, and the
// save names the setting it lacks. A worker with the queue adapter sends one
// dispatch per save under the config's model and effort, and none for the
// save its rule placed. The connector's first fill holds the rest until it is
// released, and shows the first run's tokens. The gate passes a good save onto
// its guessed side, drops a confident junk one softly with its idea, and sends
// an unsure one to review.
func TestTheQualityGateJudgesOnlyTheSavesNoRulePlaces(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(map[string]map[string]any{
		"bb2": judged(qualitygate.AnswerPass, 0.9, connectors.SideFleet, 0.95),
		"cc3": judged(qualitygate.AnswerDrop, 0.95, connectors.SidePersonal, 0.4),
		"dd4": judged(qualitygate.AnswerUnsure, 0.5, "unsure", 0.3),
	})
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"),
		redditSave("t3_cc3", "funny"), redditSave("t3_dd4", "misc"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)

	if gate := p.qualityGate(t, modelConfig{}); gate != nil {
		t.Fatal("a worker whose config names no [model] adapter opened a quality handler")
	}
	reasons := map[string]int{}
	for _, key := range []string{"reddit/t3_bb2", "reddit/t3_cc3", "reddit/t3_dd4"} {
		w := p.shown(t, key).Waiting
		if w == nil {
			t.Errorf("with no worker running the gate %s shows no waiting", key)
			continue
		}
		reasons[w.Reason]++
	}
	if !maps.Equal(reasons, map[string]int{server.WaitingUnclaimed: 1, server.WaitingFirstFill: 2}) {
		t.Errorf("the saves waiting at the gate show %v, want the first unclaimed and the 2 behind it held by the first fill", reasons)
	}

	gate := p.qualityGate(t, stand.models(t, "sonnet", "high"))
	if ran := p.run(t, gate); ran != 1 {
		t.Fatalf("under the first fill the gate ran %d tasks, want the first alone", ran)
	}
	out, err := runCLI(t, "connectors", "show", "reddit-exports", "--json", "--config", p.config)
	var shown server.ConnectorView
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil || shown.FirstFill == nil {
		t.Fatalf("connectors show printed %q, %v", out, err)
	}
	if f := shown.FirstFill; f.State != store.FirstFillHeld || f.Held != 2 || f.Tokens[store.StageQuality].CacheWrite != 3400 {
		t.Errorf("the held first fill shows %s, want 2 held and the first run's tokens at quality", jsonOf(f))
	}
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if ran := p.run(t, gate); ran != 2 {
		t.Fatalf("after the release the gate ran %d tasks, want the 2 it held", ran)
	}

	starts := stand.starts()
	if len(starts) != 3 || slices.ContainsFunc(starts, func(s dispatchStart) bool {
		return s.Agent != modelport.DefaultAgent || s.Model != "sonnet" || s.Effort != "high"
	}) {
		t.Errorf("the stand-in started %s, want 3 %s dispatches on sonnet at high, none for the save its rule placed", jsonOf(starts), modelport.DefaultAgent)
	}
	if _, side, _ := p.decisions(t, "reddit/t3_aa1"); side.DecidedBy != "rule:cooking-subreddits" {
		t.Errorf("the Cooking save is %s by %s, want placed by its rule", side.Verdict, side.DecidedBy)
	}
	if _, side, read := p.decisions(t, "reddit/t3_bb2"); side.Verdict != connectors.SideFleet || !strings.HasPrefix(side.DecidedBy, "model:") ||
		read.Verdict != connectors.ReadModel {
		t.Errorf("the passed save is %s by %s and read %s, want fleet by the model's run and read by a model", side.Verdict, side.DecidedBy, read.Verdict)
	}
	if phase := p.phase(t, "reddit/t3_cc3"); phase != "dropped" {
		t.Errorf("the junk save's phase is %s, want dropped", phase)
	}
	if _, side, _ := p.decisions(t, "reddit/t3_cc3"); side.Cause != route.CauseQualityDrop {
		t.Errorf("the junk save is %s for %s, want dropped for %s", side.Verdict, side.Cause, route.CauseQualityDrop)
	}
	if out, err := runCLI(t, "items", "show", "reddit/t3_cc3", "--config", p.config); err != nil ||
		!strings.Contains(out, `idea "The drop idea in one line."`) {
		t.Errorf("items show on the dropped save printed %q, %v, want its idea quoted", out, err)
	}
	if c := p.gated(t); !maps.Equal(c.review, map[string]int{store.ReasonQualityReview: 1}) {
		t.Errorf("the review queue holds %v, want the unsure save as quality-review", c.review)
	}
}

// With every dispatch held, a save that a rule places is still parsed,
// scanned, placed and delivered, while the save that waits on the gate holds
// no worker slot. connectors list counts it waiting, and items show names the
// dispatch and the service's note.
func TestAHeldDispatchHoldsUpOnlyItsOwnSave(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	stand.hold = "the model budget holds new runs"
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	if ran := p.run(t, p.qualityGate(t, stand.models(t, "", "")), p.lifeDeliverer(t, life)); ran != 2 {
		t.Fatalf("the worker ran %d tasks, want the save's one dispatch and the other's delivery", ran)
	}
	if articles, _ := life.counts(); articles != 1 {
		t.Errorf("the stand-in life holds %d articles, want the ruled save's", articles)
	}
	shown := p.shown(t, "reddit/t3_bb2")
	if w := shown.Waiting; shown.Item.Phase != "waiting" || w == nil || w.Reason != server.WaitingDispatched ||
		!strings.Contains(w.Message, "the model budget holds new runs") {
		t.Errorf("the held save is %s and shows waiting %s, want waiting on its dispatch with the service's note", shown.Item.Phase, jsonOf(w))
	}
	out, err := runCLI(t, "connectors", "list", "--json", "--config", p.config)
	var listed server.ConnectorsAnswer
	if err != nil || json.Unmarshal([]byte(out), &listed) != nil {
		t.Fatalf("connectors list printed %q, %v", out, err)
	}
	if i := slices.IndexFunc(listed.Connectors, func(c server.ConnectorView) bool { return c.Name == "reddit-exports" }); i < 0 || listed.Connectors[i].Waiting != 1 {
		t.Errorf("connectors list shows %s, want reddit-exports with 1 waiting", out)
	}
	if s := stand.starts(); len(s) != 1 || s[0].Model != qualitygate.DefaultModel || s[0].Effort != qualitygate.DefaultEffort {
		t.Errorf("the stand-in started %s, want one dispatch on %s at %s", jsonOf(s), qualitygate.DefaultModel, qualitygate.DefaultEffort)
	}

	// A rule that a later set adds places the held save. That withdraws its
	// quality task, so the save stops waiting at the gate and is delivered.
	ruled := lifeSet(t, withLife)
	rules, err := os.ReadFile(filepath.Join(ruled, connectors.RoutingRulesFile))
	if err != nil {
		t.Fatal(err)
	}
	rules = append(rules, "  - name: golang-is-personal\n    match:\n      connector: reddit-exports\n      field: subreddit\n"+
		"      values: [golang]\n    destination: reading-list\n"...)
	if err := os.WriteFile(filepath.Join(ruled, connectors.RoutingRulesFile), rules, 0o600); err != nil {
		t.Fatal(err)
	}
	set, err := connectors.Read(ruled, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectors.Apply(t.Context(), p.st, set, "test-ruled", new("test"), "person:test", nil, route.AfterApply(quiet)); err != nil {
		t.Fatal(err)
	}
	p.run(t, p.lifeDeliverer(t, life))
	if shown := p.shown(t, "reddit/t3_bb2"); shown.Item.Phase != "delivered" || shown.Waiting != nil {
		t.Errorf("once a rule placed the held save it is %s and shows waiting %s, want delivered", shown.Item.Phase, jsonOf(shown.Waiting))
	}
}

// A dispatch is sent once under its key. A worker started again reads the run
// that the stored dispatch names, rather than sending another. `tasks retry`
// queues nothing new while that run waits, and once the run fails, it sends
// one new dispatch, whose answer places the save.
func TestAModelCallIsSentOnceAndAgainOnlyAfterItsRunFails(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	stand.hold = "queued behind other runs"
	key := "reddit/t3_bb2"
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	models := stand.models(t, "", "")
	p.run(t, p.qualityGate(t, models))
	task := slices.IndexFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageQuality })
	id := p.tasks(t)[task].ID

	if out, err := runCLI(t, "tasks", "retry", id, "--json", "--config", p.config); err != nil || !strings.Contains(out, `"state":"queued"`) {
		t.Errorf("tasks retry on a waiting task printed %q, %v, want it queued as it stands", out, err)
	} else {
		holdsTo(t, []byte(out), "task-retry.schema.json")
	}
	p.advance(time.Minute)
	if ran := p.run(t, p.qualityGate(t, models)); ran != 1 || len(stand.starts()) != 1 || stand.readCount() == 0 {
		t.Fatalf("a worker started again ran %d tasks and the stand-in saw %d dispatches and %d reads, want 1 task reading the first dispatch",
			ran, len(stand.starts()), stand.readCount())
	}

	stand.set(func(d *standInDispatch) { d.hold, d.fail = "", modelport.FailureClaudeFailed })
	p.advance(time.Minute)
	p.run(t, p.qualityGate(t, models))
	shown := p.shown(t, key)
	if i := slices.IndexFunc(shown.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageQuality }); shown.Item.Phase != "failed" ||
		i < 0 || shown.Outputs[i].State != store.OutputFailed || !strings.HasPrefix(text(shown.Outputs[i].Error), modelport.FailureClaudeFailed) {
		t.Fatalf("after its run failed the save is %s with outputs %s, want failed with the run's failure", shown.Item.Phase, jsonOf(shown.Outputs))
	}
	if _, err := runCLI(t, "tasks", "retry", id, "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, p.qualityGate(t, models))
	if s := stand.starts(); len(s) != 2 || s[0].Key == s[1].Key {
		t.Errorf("after a retry the stand-in started %s, want a second dispatch under a new key", jsonOf(s))
	}
	if _, side, _ := p.decisions(t, key); side.Verdict != connectors.SidePersonal || !strings.HasPrefix(side.DecidedBy, "model:") {
		t.Errorf("after the retried run the save is %s by %s, want personal by the model's run", side.Verdict, side.DecidedBy)
	}
	if _, err := runCLI(t, "tasks", "retry", id, "--config", p.config); !errors.Is(err, goclikit.ErrUsage) {
		t.Errorf("tasks retry on a done task answered %v, want it refused as a usage error", err)
	}
}
