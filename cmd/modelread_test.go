package cmd

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/extract"
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
// scanner over each suggestion.
func (p *pipeline) modelRead(t *testing.T, models modelConfig, scanner modelread.Scanner) worker.Handler {
	t.Helper()
	port := openModelPort(t.Context(), models, p.provider.URL, quiet)
	if port == nil {
		t.Fatal("the [model] table opened no port")
	}
	return modelread.NewHandler(port, p.worker, scanner, modelstage.Settings{Model: models.Read.Model, Effort: models.Read.Effort}, quiet)
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
// text in one dispatch, on the model and effort that [model.read] names. Its
// output holds the summary and the suggestion, versioned by the prompt and by
// the lens, and the scanner's findings over the suggestion sit beside them,
// under the rules' version. A save that its rule reads as is reaches delivery
// with no model read.
func TestAFleetSavesModelReadAnswersItsNoteAndScansItsSuggestion(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	sentence := "This post suggests that setup scripts stop calling sudo, because a script run as root hides what it changes."
	stand.read = func(string) map[string]any { return readAnswered("A post about setup scripts.", sentence) }
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	note := "what would this change in how we set up machines?"
	if _, err := runCLI(t, "review", "place", "reddit/t3_bb2", "work-inbox", "--note", note, "--config", p.config); err != nil {
		t.Fatal(err)
	}

	models := stand.models(t, "", "")
	models.Read = stageModel{Model: "sonnet", Effort: "high"}
	if ran := p.run(t, p.modelRead(t, models, sentenceScanner{}), p.lifeDeliverer(t, life)); ran != 2 {
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
	if err != nil || !strings.Contains(out, "lens fleet v"+fleet.Lens.Version) || !strings.Contains(out, "suggestion "+quotedUpTo(sentence, answerShown)) ||
		!strings.Contains(out, commandRule+"  info  \"sudo\"") {
		t.Errorf("items show printed %q, %v, want the lens, the suggestion quoted and its finding", out, err)
	}

	if slices.ContainsFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageModelRead }) {
		t.Errorf("a model read's task is still open: %s", jsonOf(p.tasks(t)))
	}
	ruled := p.shown(t, "reddit/t3_aa1")
	if slices.ContainsFunc(ruled.Outputs, func(o server.OutputView) bool { return o.Stage == store.StageModelRead }) || ruled.Item.Phase != "delivered" {
		t.Errorf("the save its rule reads as is is %s with outputs %s, want delivered with no model read", ruled.Item.Phase, jsonOf(ruled.Outputs))
	}
}
