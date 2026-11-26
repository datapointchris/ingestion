package cmd

import (
	"encoding/json"
	"testing"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// A person places one save before the gate runs, and the gate sends two
// others to review, where the person keeps one and drops the other. Only the
// two placed from review are labels, so gates agreement counts them and a
// measurement replay passes over the third. An arm asks the gate's prompt of
// sonnet on one label. It decides nothing, so the save keeps its phase,
// decisions and delivery, and gates agreement prints sonnet's answer beside
// the gate's on that save.
func TestOnlyAPlacementFromReviewIsALabelAndAnArmMovesNothing(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(map[string]map[string]any{
		"bb2": judged(qualitygate.AnswerUnsure, 0.6, connectors.SidePersonal, 0.6),
		"cc3": judged(qualitygate.AnswerDrop, 0.7, connectors.SideFleet, 0.9),
	})
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_bb2", "golang"), redditSave("t3_cc3", "misc"), redditSave("t3_dd4", "misc"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	if _, err := runCLI(t, "review", "place", "reddit/t3_dd4", "reading-list", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	models := stand.models(t, "", "")
	p.run(t, p.qualityGate(t, models), p.lifeDeliverer(t, life))
	for _, key := range []string{"reddit/t3_bb2", "reddit/t3_cc3"} {
		if shown := p.shown(t, key); shown.Item.Phase != "review" {
			t.Fatalf("after the gate %s is %s, want review", key, shown.Item.Phase)
		}
	}
	if _, err := runCLI(t, "review", "place", "reddit/t3_bb2", "reading-list", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if _, err := runCLI(t, "review", "drop", "reddit/t3_cc3", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, p.qualityGate(t, models), p.lifeDeliverer(t, life))

	agreement := func() accuracy.Report {
		t.Helper()
		out, err := runCLI(t, "gates", "agreement", "--json", "--config", p.config)
		var report accuracy.Report
		if err != nil || json.Unmarshal([]byte(out), &report) != nil {
			t.Fatalf("gates agreement printed %q, %v", out, err)
		}
		holdsTo(t, []byte(out), "gate-agreement.schema.json")
		return report
	}
	report := agreement()
	if l := report.Labels; l.Items != 2 || l.Personal != 1 || l.Drop != 1 || report.Unanswered != 0 ||
		report.Quality.Overall.Agreed != 1 || report.Quality.Overall.Unsure != 1 || report.Side.Overall.Agreed != 1 || len(report.Arms) != 0 {
		t.Errorf("gates agreement answered %s, want the 2 saves placed from review: the drop agreed and the unsure verdict", jsonOf(report))
	}

	replay := func(args ...string) server.ReplayAnswer {
		t.Helper()
		out, err := runCLI(t, append([]string{"items", "replay", "--json", "--config", p.config}, args...)...)
		var answer server.ReplayAnswer
		if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
			t.Fatalf("items replay %v printed %q, %v", args, out, err)
		}
		holdsTo(t, []byte(out), "replay.schema.json")
		return answer
	}
	if a := replay("--stage", store.StageQuality, "--placed-by", store.PlacedByPerson); a.Items != 0 {
		t.Errorf("a measurement replay would run on %d saves, want none: the labels hold an answer and the third save is no label", a.Items)
	}
	if a := replay("--stage", store.StageQualityArm, "--model", "sonnet"); a.Items != 2 || a.Queued {
		t.Errorf("an arm replay answered %s, want an estimate over the 2 labels", jsonOf(a))
	}
	before, _, _ := p.decisions(t, "reddit/t3_bb2")
	if a := replay("--stage", store.StageQualityArm, "--model", "sonnet", "--key", "reddit/t3_bb2", "--yes"); a.Items != 1 || !a.Queued {
		t.Fatalf("a confirmed arm replay answered %s, want 1 queued", jsonOf(a))
	}
	stand.set(func(d *standInDispatch) {
		d.answer = func(string) map[string]any { return judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.9) }
	})
	if ran := p.run(t, p.modelHandler(t, models, qualitygate.ArmCapability), p.qualityGate(t, models), p.lifeDeliverer(t, life)); ran != 1 {
		t.Fatalf("the worker ran %d tasks, want the arm's one", ran)
	}
	starts := stand.starts()
	if last := starts[len(starts)-1]; last.Model != "sonnet" || len(starts) != 3 {
		t.Errorf("the dispatches started %s, want the arm's last and on sonnet", jsonOf(starts))
	}
	after, side, _ := p.decisions(t, "reddit/t3_bb2")
	shown := p.shown(t, "reddit/t3_bb2")
	if len(after) != len(before) || side.Verdict != connectors.SidePersonal || shown.Item.Phase != "delivered" || len(shown.Deliveries) != 1 {
		t.Errorf("after the arm the save is %s with decisions %s and deliveries %s, want it delivered as the person placed it",
			shown.Item.Phase, jsonOf(after), jsonOf(shown.Deliveries))
	}
	if articles, creates := life.counts(); articles != 2 || creates != 2 {
		t.Errorf("life holds %d articles from %d creates, want each kept save written once", articles, creates)
	}

	report = agreement()
	if len(report.Arms) != 1 {
		t.Fatalf("gates agreement printed arms %s, want sonnet's", jsonOf(report.Arms))
	}
	arm := report.Arms[0]
	if arm.Model != "sonnet" || arm.Items != 1 || arm.Arm.Quality.Overall.Agreed != 1 || arm.Gate.Quality.Overall.Unsure != 1 ||
		report.Quality.Overall.Comparisons != 2 {
		t.Errorf("gates agreement answered %s, want sonnet's pass beside the gate's unsure verdict on the one save", jsonOf(report))
	}
}
