package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Two saves that a person placed on reading-list before the quality gate ran
// are delivered to life, and an ordinary quality replay passes over them. A
// replay with --placed-by person asks the gate about each as a measurement.
// While it waits, and after the gate drops one with confidence, each save
// stays delivered with its placement, and nothing is withdrawn or delivered
// again. gates agreement counts the drop and the fleet guess as
// disagreements, and the other save's pass and personal guess as agreements.
func TestTheGateMeasuresAPersonsPlacementAndMovesNothing(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(map[string]map[string]any{
		"bb2": judged(qualitygate.AnswerDrop, 0.95, connectors.SideFleet, 0.9),
		"cc3": judged(qualitygate.AnswerPass, 0.85, connectors.SidePersonal, 0.75),
	})
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_bb2", "golang"), redditSave("t3_cc3", "misc"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	keys := []string{"reddit/t3_bb2", "reddit/t3_cc3"}
	for _, key := range keys {
		if _, err := runCLI(t, "review", "place", key, "reading-list", "--config", p.config); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	models := stand.models(t, "", "")
	p.run(t, p.qualityGate(t, models), p.lifeDeliverer(t, life))
	if articles, _ := life.counts(); articles != 2 || len(stand.starts()) != 0 {
		t.Fatalf("life holds %d articles and the gate ran %d times, want both saves delivered and no run", articles, len(stand.starts()))
	}

	replay := func(args ...string) server.ReplayAnswer {
		t.Helper()
		out, err := runCLI(t, append([]string{"items", "replay", "--stage", store.StageQuality, "--json", "--config", p.config}, args...)...)
		var answer server.ReplayAnswer
		if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
			t.Fatalf("items replay %v printed %q, %v", args, out, err)
		}
		holdsTo(t, []byte(out), "replay.schema.json")
		return answer
	}
	if a := replay(); a.Items != 0 {
		t.Errorf("a quality replay would run on %d saves, want none that a person placed", a.Items)
	}
	if a := replay("--placed-by", store.PlacedByPerson); a.Items != 2 || a.Queued {
		t.Errorf("a measurement replay answered %s, want an estimate over the 2 placed saves", jsonOf(a))
	}
	if a := replay("--placed-by", store.PlacedByPerson, "--yes"); a.Items != 2 || !a.Queued {
		t.Fatalf("a confirmed measurement replay answered %s, want 2 queued", jsonOf(a))
	}
	for _, key := range keys {
		if shown := p.shown(t, key); shown.Item.Phase != "delivered" || shown.Waiting != nil {
			t.Errorf("while its measurement waits %s is %s and shows waiting %s, want delivered", key, shown.Item.Phase, jsonOf(shown.Waiting))
		}
	}
	if ran := p.run(t, p.qualityGate(t, models), p.lifeDeliverer(t, life)); ran != 2 {
		t.Fatalf("the worker ran %d tasks, want the 2 measurements alone", ran)
	}

	for _, key := range keys {
		all, side, read := p.decisions(t, key)
		if !strings.HasPrefix(side.DecidedBy, "person:") || side.Verdict != connectors.SidePersonal || read.Verdict != connectors.ReadAsIs {
			t.Errorf("after its measurement %s is %s by %s and read %s, want the person's placement as it was", key, side.Verdict, side.DecidedBy, read.Verdict)
		}
		if len(all) != 3 {
			t.Errorf("%s holds %d decisions, want the placement, its read and the gate's", key, len(all))
		}
		shown := p.shown(t, key)
		if shown.Item.Phase != "delivered" || len(shown.Deliveries) != 1 || shown.Deliveries[0].Outcome != store.DeliveryDelivered {
			t.Errorf("after its measurement %s is %s with deliveries %s, want delivered once", key, shown.Item.Phase, jsonOf(shown.Deliveries))
		}
	}
	if all, _, _ := p.decisions(t, "reddit/t3_bb2"); !containsDecision(all, store.GateQuality, "drop") {
		t.Errorf("the dropped save holds %s, want the gate's drop kept", jsonOf(all))
	}
	if articles, creates := life.counts(); articles != 2 || creates != 2 || len(p.tasks(t)) != 0 {
		t.Errorf("life holds %d articles from %d creates with %d tasks open, want each save written once and nothing queued", articles, creates, len(p.tasks(t)))
	}

	out, err := runCLI(t, "gates", "agreement", "--json", "--config", p.config)
	var report accuracy.Report
	if err != nil || json.Unmarshal([]byte(out), &report) != nil {
		t.Fatalf("gates agreement printed %q, %v", out, err)
	}
	holdsTo(t, []byte(out), "gate-agreement.schema.json")
	q, s := report.Quality, report.Side
	if report.Labels.Personal != 2 || report.Unanswered != 0 || q.Overall.Agreed != 1 || q.Overall.Disagreed != 1 ||
		s.Overall.Agreed != 1 || s.Overall.Disagreed != 1 || len(q.ByConfidence) != 2 || q.ByConfidence[1].From != 0.9 {
		t.Errorf("gates agreement printed %s, want 2 personal labels, and each gate agreeing once and disagreeing once", out)
	}
	if out, err := runCLI(t, "gates", "agreement", "--config", p.config); err != nil || !strings.Contains(out, "quality gate, drop floor 0.80") {
		t.Errorf("gates agreement printed %q, %v", out, err)
	}
}

func containsDecision(all []server.DecisionView, gate, verdict string) bool {
	for _, d := range all {
		if d.Gate == gate && d.Verdict == verdict {
			return true
		}
	}
	return false
}
