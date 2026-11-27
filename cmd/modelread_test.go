package cmd

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/lens"
	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// sentenceScanner stands in for the scanner over a suggestion. It finds
// ingestion's own command rule wherever the sentence names sudo.
type sentenceScanner struct{}

const commandRule = "ingestion.command"

func (sentenceScanner) Version() string { return "rules-test" }

func (sentenceScanner) Scan(_ context.Context, text string) ([]store.Finding, []store.Span, error) {
	at := strings.Index(text, "sudo")
	if at < 0 {
		return []store.Finding{}, []store.Span{}, nil
	}
	return []store.Finding{{Rule: commandRule, Severity: "info", Offset: at, Length: len("sudo")}},
		[]store.Span{{Rule: commandRule, Span: "sudo"}}, nil
}

// modelRead is the model read's handler that a worker opens from models, with
// contexts as its lenses' context readers and scanner over each suggestion.
func (p *pipeline) modelRead(t *testing.T, models modelConfig, contexts lens.Readers, scanner modelread.Scanner) worker.Handler {
	t.Helper()
	port := openModelPort(t.Context(), models, p.provider.URL, quiet)
	if port == nil {
		t.Fatal("the [model] table opened no port")
	}
	return modelread.NewHandler(port, p.worker, contexts, scanner, modelstage.Settings{Model: models.ModelRead.Model, Effort: models.ModelRead.Effort}, quiet)
}

// readAnswered is an answer holding to prompts/model-read/answer.schema.json.
func readAnswered(summary, suggestion string) map[string]any {
	a := map[string]any{"summary": summary, "suggestion": suggestion}
	if suggestion == "" {
		a["suggestion"] = nil
	}
	return a
}

// A person places a save on the fleet side with a note, which queues one
// model read under the fleet lens. The read sends the note and the save's
// text in one dispatch, on the model and effort that [model.model-read]
// names. Its output holds the summary and the suggestion, versioned by the
// prompt and by the lens, and the scanner's findings over the suggestion sit
// beside them, under the rules' version. The item's suggestion carries them
// as its schema publishes it. A person who places the read save again with
// another note gets that note answered, and placing it again with no note
// reads nothing and drafts no note. A save that its rule reads as is reaches
// delivery with no model read.
func TestAFleetSavesModelReadAnswersEachNoteAPersonPlacesItWith(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	sentence := "This post suggests that setup scripts stop calling sudo, because a script run as root hides what it changes."
	again := "does this bear on how we run CI?"
	stand.read = func(prompt string) map[string]any {
		if strings.Contains(prompt, again) {
			return readAnswered("A post that bears on CI.", "")
		}
		return readAnswered("A post about setup scripts.", sentence)
	}
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	note := "what would this change in how we set up machines?"
	if _, err := runCLI(t, "review", "place", "reddit/t3_bb2", "work-inbox", "--note", note, "--config", p.config); err != nil {
		t.Fatal(err)
	}
	// The ruled save's quality task started the first fill, which holds the
	// model read until it is released.
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}

	models := stand.models(t, "", "")
	models.ModelRead = stageModel{Model: "sonnet", Effort: "high"}
	if ran := p.run(t, p.modelRead(t, models, nil, sentenceScanner{}), p.lifeDeliverer(t, life)); ran != 2 {
		t.Fatalf("the worker ran %d tasks, want the fleet save's model read and the other's delivery", ran)
	}
	starts := stand.starts()
	if len(starts) != 1 || starts[0].Model != "sonnet" || starts[0].Effort != "high" || !strings.HasSuffix(starts[0].Schema, "/model-read/answer.schema.json") {
		t.Fatalf("the stand-in started %s, want one model read on sonnet at high", jsonOf(starts))
	}
	if prompt := stand.prompt(starts[0].Key); !strings.Contains(prompt, note) || !strings.Contains(prompt, "Its body") {
		t.Errorf("the model read's prompt holds neither the note nor the save's text")
	}

	fleet := prompts.ModelRead["fleet"]
	shown := p.shown(t, "reddit/t3_bb2")
	i := slices.IndexFunc(shown.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageModelRead })
	if i < 0 {
		t.Fatalf("the fleet save holds no model read: %s", jsonOf(shown.Outputs))
	}
	read := shown.Outputs[i]
	var answer modelread.Answer
	if err := json.Unmarshal(read.Answer, &answer); err != nil || answer.Summary == nil || text(answer.Suggestion) != sentence {
		t.Errorf("the model read's answer is %s, want its summary and its suggestion", read.Answer)
	}
	if read.State != store.OutputDone || read.ProducerVersion != fleet.Version || text(read.Lens) != "fleet" ||
		text(read.LensVersion) != fleet.Lens.Version || shown.Item.Phase != "read" {
		t.Errorf("the model read is %s at %s under lens %s %s, and the save is %s; want done at %s under fleet %s, and read",
			read.State, read.ProducerVersion, text(read.Lens), text(read.LensVersion), shown.Item.Phase, fleet.Version, fleet.Lens.Version)
	}
	if len(read.Findings) != 1 || read.Findings[0].Rule != commandRule || text(read.Findings[0].Span) != "sudo" ||
		read.Findings[0].Unit != nil || text(read.RulesVersion) != "rules-test" {
		t.Errorf("the model read holds findings %s under rules %s, want the scanner's one finding on the suggestion under rules-test",
			jsonOf(read.Findings), text(read.RulesVersion))
	}
	out, err := runCLI(t, "items", "show", "reddit/t3_bb2", "--config", p.config)
	if err != nil || !strings.Contains(out, "lens fleet v"+fleet.Lens.Version) || !strings.Contains(out, "suggestion "+strconv.Quote(sentence)) {
		t.Errorf("items show printed %q, %v, want the lens and the suggestion quoted", out, err)
	}
	if s := shown.Suggestion; s == nil || s.Sentence != sentence || text(s.Lens) != "fleet "+fleet.Lens.Version || len(s.Findings) != 1 ||
		s.Findings[0].Rule != commandRule || s.Findings[0].In != server.FoundInSentence || text(s.Findings[0].Span) != "sudo" {
		t.Errorf("the save's suggestion is %s, want the sentence under the fleet lens with its one finding in the sentence", jsonOf(s))
	}
	raw, err := runCLI(t, "items", "show", "reddit/t3_bb2", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(raw), "item.schema.json")

	if slices.ContainsFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageModelRead }) {
		t.Errorf("a model read's task is still open: %s", jsonOf(p.tasks(t)))
	}
	reader := p.modelRead(t, models, nil, sentenceScanner{})
	stand.set(func(d *standInDispatch) { d.hold = "queued behind other runs" })
	if _, err := runCLI(t, "review", "place", "reddit/t3_bb2", "work-inbox", "--note", again, "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if ran := p.run(t, reader); ran != 1 || len(stand.starts()) != 2 || !strings.Contains(stand.prompt(stand.starts()[1].Key), again) {
		t.Fatalf("placing the read save with another note ran %d tasks and started %s, want one read of that note", ran, jsonOf(stand.starts()))
	}
	againRead := func() server.OutputView {
		shown = p.shown(t, "reddit/t3_bb2")
		return shown.Outputs[slices.IndexFunc(shown.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageModelRead })]
	}
	if read := againRead(); read.State != store.OutputWaiting || read.DispatchID == nil || text(read.NoteDigest) != text(store.NoteDigest(&again)) {
		t.Errorf("while its run waits the read is %s on dispatch %s answering %s, want waiting on its dispatch for that note",
			read.State, text(read.DispatchID), text(read.NoteDigest))
	}
	stand.set(func(d *standInDispatch) { d.hold = "" })
	p.advance(time.Minute)
	p.run(t, reader)
	if read := againRead(); read.State != store.OutputDone || text(read.NoteDigest) != text(store.NoteDigest(&again)) || shown.Suggestion != nil ||
		len(stand.starts()) != 2 {
		t.Errorf("after its run answered the read is %s answering %s with suggestion %s after %d calls, want done answering that note with none after 2",
			read.State, text(read.NoteDigest), jsonOf(shown.Suggestion), len(stand.starts()))
	}
	if _, err := runCLI(t, "review", "place", "reddit/t3_bb2", "work-inbox", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if ran := p.run(t, reader); ran != 0 || len(stand.starts()) != 2 ||
		slices.ContainsFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageNote }) {
		t.Errorf("placing the read save with no note ran %d tasks, started %d calls or queued a note's draft, want none", ran, len(stand.starts()))
	}

	ruled := p.shown(t, "reddit/t3_aa1")
	if slices.ContainsFunc(ruled.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageModelRead }) || ruled.Item.Phase != "delivered" {
		t.Errorf("the save its rule reads as is is %s with outputs %s, want delivered with no model read", ruled.Item.Phase, jsonOf(ruled.Outputs))
	}
}
