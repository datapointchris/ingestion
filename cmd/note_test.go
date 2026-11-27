package cmd

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/datapointchris/goapiauth/authtest"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/lens"
	"github.com/datapointchris/ingestion/note"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/views/review"
	"github.com/datapointchris/ingestion/worker"
)

// slateClient is the service client that a worker reads the stand-in slate
// as, with slateSecret.
const (
	slateClient = "slate-svc-ingestion"
	slateSecret = "slate-secret"
)

// standInSlate answers slate's focus and active initiatives with the answers
// that lens/testdata/slate holds to slate's published schemas, to a caller
// whose token carries lens.SlateScopes, and records each request it answered.
type standInSlate struct {
	*httptest.Server
	mu    sync.Mutex
	asked []string
}

func newStandInSlate(t *testing.T, provider *authtest.Provider) *standInSlate {
	t.Helper()
	provider.Register(slateClient, slateSecret, lens.SlateScopes...)
	verifier := provider.Verifier(t, "slate-cli-", "slate-svc-")
	answers := map[string]string{"/api/v1/focus": "focus.json", "/api/v1/initiatives": "initiatives.json"}
	s := &standInSlate{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		caller, err := verifier.Verify(r.Context(), token)
		if !bearer || err != nil || slices.ContainsFunc(lens.SlateScopes, func(scope string) bool { return !slices.Contains(caller.Scopes, scope) }) {
			http.Error(w, `{"detail":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		file, ok := answers[r.URL.Path]
		if !ok || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		raw, err := os.ReadFile(filepath.Join("..", "lens", "testdata", "slate", file))
		if err != nil {
			t.Error(err)
		}
		s.mu.Lock()
		s.asked = append(s.asked, r.URL.RequestURI())
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	t.Cleanup(s.Close)
	return s
}

// requests is each request the stand-in answered so far.
func (s *standInSlate) requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.asked)
}

// lensReaders is the context readers that a worker opens from a config whose
// [lens.fleet.context] names the slate reader at api, with the client's
// secret in the environment.
func (p *pipeline) lensReaders(t *testing.T, api string, log *slog.Logger) lens.Readers {
	t.Helper()
	t.Setenv(lensSecretEnv("fleet"), slateSecret)
	table := lensContextTable{serviceClient: serviceClient{API: api, ClientID: slateClient}, Reader: lens.Slate}
	return openLensReaders(t.Context(), fileConfig{Lens: map[string]lensTable{"fleet": {Context: table}}}, p.provider.URL, log)
}

// noteDrafter is the note stage's handler that a worker opens from models,
// with contexts as its lenses' context readers.
func (p *pipeline) noteDrafter(t *testing.T, models modelConfig, contexts lens.Readers) worker.Handler {
	t.Helper()
	handlers := workerHandlers(p.worker, nil, nil, openModelPort(t.Context(), models, p.provider.URL, quiet), nil, contexts, models, quiet)
	i := slices.IndexFunc(handlers, func(h worker.Handler) bool { return h.Capability() == note.Capability })
	if i < 0 {
		t.Fatal("the worker holds no note handler")
	}
	return handlers[i]
}

// drafted is an answer holding to prompts/note/answer.schema.json, whose
// every field names topic.
func drafted(confidence float64, topic string) map[string]any {
	return map[string]any{
		"questions": []string{"What would the " + topic + " change?"}, "why_saved": "It bears on the " + topic + ".",
		"relation": "The " + topic + " relation.", "summary": "The " + topic + " summary.", "idea": "The " + topic + " idea.",
		"confidence": confidence,
	}
}

// draftHeading opens the drafted note in the model read's prompt, and
// currentWork is the focus that the stand-in slate answers.
const (
	draftHeading = "A model drafted these questions from the item's opening"
	currentWork  = "Focus until 2030-04-12: Mostly the rocket garden's irrigation controller for six weeks."
)

// promptOfRun is the prompt of the dispatch whose id is id.
func (d *standInDispatch) promptOfRun(id string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if run, ok := d.dispatches[id]; ok {
		return run.prompt
	}
	return ""
}

// A person places saves on the fleet side with no note, and a model drafts
// each one's note on haiku at medium, from its opening beside the owner's
// current work that the stand-in slate answers. A draft at or above the note
// floor becomes the read's note: the model read quotes its questions under
// their own heading, beside the same current work, which slate answered once
// for the batch. A draft below the floor waits in review as note-unclear,
// showing its summary, relation and idea in the review list and the review
// tab. The person then places it with a note, which the read answers in the
// draft's place. A worker that opens no context reader drafts without the
// current work, and placing its save with no note takes its draft below the
// floor. No draft reaches a decision's note, and gates agreement counts each
// draft by its confidence as kept or rewritten.
func TestANoteDraftAtTheFloorIsTheReadsNoteAndOneBelowWaitsOnAPerson(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.note = func(prompt string) map[string]any {
		switch {
		case strings.Contains(prompt, "/comments/bb2/"):
			return drafted(0.9, "valve")
		case strings.Contains(prompt, "/comments/cc3/"):
			return drafted(0.4, "ledger")
		}
		return drafted(0.5, "compost")
	}
	stand.read = func(string) map[string]any { return readAnswered("A post.", "") }
	life := newStandInLife(t, p.provider)
	slate := newStandInSlate(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(
		redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"), redditSave("t3_cc3", "golang"), redditSave("t3_dd4", "golang"),
	)})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	place := func(key string, flags ...string) {
		t.Helper()
		if _, err := runCLI(t, append([]string{"review", "place", key, "work-inbox", "--config", p.config}, flags...)...); err != nil {
			t.Fatal(err)
		}
	}
	place("reddit/t3_bb2")
	place("reddit/t3_cc3")
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}

	models := stand.models(t, "", "")
	models.Note = stageModel{Model: "haiku", Effort: "medium"}
	contexts := p.lensReaders(t, slate.URL, quiet)
	reader := p.modelRead(t, models, contexts, sentenceScanner{})
	if ran := p.run(t, p.noteDrafter(t, models, contexts), reader, p.lifeDeliverer(t, life)); ran != 4 {
		t.Fatalf("the worker ran %d tasks, want two drafts, the confident one's model read and the ruled save's delivery", ran)
	}
	starts := stand.starts()
	drafts := slices.DeleteFunc(slices.Clone(starts), func(s dispatchStart) bool { return !strings.HasSuffix(s.Schema, "/prompts/note/answer.schema.json") })
	if len(starts) != 3 || len(drafts) != 2 || slices.ContainsFunc(drafts, func(s dispatchStart) bool { return s.Model != "haiku" || s.Effort != "medium" }) {
		t.Fatalf("the stand-in started %s, want two drafts on haiku at medium and one model read", jsonOf(starts))
	}
	for _, d := range drafts {
		prompt := stand.prompt(d.Key)
		for _, want := range []string{currentWork, "Garden automation, 70% of the work", "102 The kitchen ledger imports each receipt", "Its body"} {
			if !strings.Contains(prompt, want) {
				t.Errorf("a draft's prompt lacks %q", want)
			}
		}
	}
	if got := slate.requests(); !slices.Equal(got, []string{"/api/v1/focus", "/api/v1/initiatives?status=active"}) {
		t.Errorf("slate answered %q, want the focus and the active initiatives once for the batch", got)
	}

	// readOf is the item under key, its newest model read and that read's
	// prompt.
	readOf := func(key string) (server.ItemAnswer, server.OutputView, string) {
		t.Helper()
		shown := p.shown(t, key)
		i := slices.IndexFunc(shown.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageModelRead })
		if i < 0 || shown.Outputs[i].DispatchID == nil {
			t.Fatalf("%s holds no dispatched model read: %s", key, jsonOf(shown.Outputs))
		}
		return shown, shown.Outputs[i], stand.promptOfRun(*shown.Outputs[i].DispatchID)
	}
	shown, read, prompt := readOf("reddit/t3_bb2")
	valve := draftedNote(t, shown)
	if !strings.Contains(prompt, draftHeading) || !strings.Contains(prompt, "What would the valve change?") || !strings.Contains(prompt, currentWork) {
		t.Errorf("the confident save's model read quotes neither the draft under its heading nor the owner's current work")
	}
	if read.State != store.OutputDone || text(read.NoteDigest) != text(store.NoteDigest(&valve)) || shown.Item.Phase != "read" {
		t.Errorf("the confident save's read is %s answering %s and the save is %s, want done answering the draft, and read",
			read.State, text(read.NoteDigest), shown.Item.Phase)
	}

	out, err := runCLI(t, "review", "list", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "review-list.schema.json")
	var queue server.ReviewAnswer
	if err := json.Unmarshal([]byte(out), &queue); err != nil {
		t.Fatal(err)
	}
	var unclear note.Answer
	if len(queue.Items) != 1 || queue.Items[0].Key != "reddit/t3_cc3" || queue.Items[0].Reason != route.CauseNoteUnclear ||
		json.Unmarshal(queue.Items[0].NoteDraft, &unclear) != nil || unclear.Summary != "The ledger summary." ||
		unclear.Relation != "The ledger relation." || unclear.Idea != "The ledger idea." {
		t.Fatalf("review lists %s, want the unsure save as note-unclear with its draft's summary, relation and idea", jsonOf(queue.Items))
	}
	tab, err := runCLI(t, "tui", "--view", "review", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(tab), "tui-review.schema.json")
	var snap review.Snapshot
	if err := json.Unmarshal([]byte(tab), &snap); err != nil || len(snap.Items) != 1 || snap.Items[0].Group != "reason/"+route.CauseNoteUnclear {
		t.Errorf("the review tab lists %s, want the unsure save as note-unclear", jsonOf(snap.Items))
	}
	t.Setenv("COLUMNS", "200")
	t.Setenv("LINES", "80")
	frame, err := runCLI(t, "tui", "--once", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"The ledger summary.", "The ledger relation.", "The ledger idea.", "What would the ledger change?"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the review tab's frame lacks %q", want)
		}
	}

	own := "how would each receipt land as a row?"
	place("reddit/t3_cc3", "--note", own)
	if ran := p.run(t, reader); ran != 1 {
		t.Fatalf("placing the unsure save with a note ran %d tasks, want its model read", ran)
	}
	_, read, prompt = readOf("reddit/t3_cc3")
	if !strings.Contains(prompt, own) || strings.Contains(prompt, draftHeading) || strings.Contains(prompt, "ledger change") {
		t.Errorf("the unsure save's model read does not answer the person's note in the draft's place")
	}
	if text(read.NoteDigest) != text(store.NoteDigest(&own)) {
		t.Errorf("the unsure save's read answers %s, want the person's note", text(read.NoteDigest))
	}

	place("reddit/t3_dd4")
	if ran := p.run(t, p.noteDrafter(t, models, nil), reader); ran != 1 {
		t.Fatalf("the worker with no context reader ran %d tasks, want the draft alone", ran)
	}
	last := stand.starts()[len(stand.starts())-1]
	if !strings.HasSuffix(last.Schema, "/prompts/note/answer.schema.json") || strings.Contains(stand.prompt(last.Key), currentWork) {
		t.Errorf("the draft with no context reader carries the owner's current work")
	}
	if got := p.shown(t, "reddit/t3_dd4"); got.Item.Phase != "review" {
		t.Errorf("the save drafted below the floor is %s, want review", got.Item.Phase)
	}
	place("reddit/t3_dd4")
	if ran := p.run(t, reader); ran != 1 {
		t.Fatalf("placing the unclear save with no note ran %d tasks, want its model read of the draft", ran)
	}
	shown, read, prompt = readOf("reddit/t3_dd4")
	compost := draftedNote(t, shown)
	if !strings.Contains(prompt, draftHeading) || !strings.Contains(prompt, "What would the compost change?") ||
		text(read.NoteDigest) != text(store.NoteDigest(&compost)) {
		t.Errorf("placing the unclear save with no note did not read its draft")
	}

	if n := p.count(t, `SELECT count(*) FROM decisions WHERE note IS NOT NULL AND note != ?`, own); n != 0 {
		t.Errorf("%d decisions carry a note other than the person's, want no draft in any decision", n)
	}
	out, err = runCLI(t, "gates", "agreement", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "gate-agreement.schema.json")
	var report accuracy.Report
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatal(err)
	}
	if n := report.Note.Overall; n.Drafts != 3 || n.Kept != 2 || n.Rewritten != 1 || n.Waiting != 0 || len(report.Note.ByConfidence) != 3 {
		t.Errorf("the note band is %s, want three drafts in three bands, two kept and one rewritten", jsonOf(report.Note))
	}
	if out, err = runCLI(t, "gates", "agreement", "--config", p.config); err != nil || !strings.Contains(out, "note drafts, floor 0.70") ||
		!strings.Contains(out, "confidence 0.4-0.5") {
		t.Errorf("gates agreement printed %q, %v, want the note band by confidence", out, err)
	}
}

// draftedNote is the note that the item's newest done draft reads as.
func draftedNote(t *testing.T, shown server.ItemAnswer) string {
	t.Helper()
	i := slices.IndexFunc(shown.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageNote && o.State == store.OutputDone })
	if i < 0 {
		t.Fatalf("%s holds no done draft: %s", shown.Item.Key, jsonOf(shown.Outputs))
	}
	d, err := route.DraftOf(store.Output{StageRun: store.StageRun{Answer: shown.Outputs[i].Answer}})
	if err != nil {
		t.Fatal(err)
	}
	return d.Note()
}
