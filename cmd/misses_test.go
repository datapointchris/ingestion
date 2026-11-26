package cmd

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/misses"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// plantedSave is a self post saved from r/markersub whose every outsider
// field holds the word marker, so a projection that carried one shows it.
func plantedSave(name string) string {
	post := `{"kind":"t3","data":{"name":"` + name + `","author":"markerauthor","subreddit":"markersub","title":"Marker title words",` +
		`"selftext":"Marker body words","is_self":true,"created_utc":1562872000.0,"permalink":"/r/markersub/comments/` + name[3:] + `/marker_slug/"}}`
	return `{"child":` + post + `,"thread":[{"data":{"children":[` + post + `]}},{"data":{"children":[]}}]}`
}

// In its first week the gate answers four saves, whose titles, URLs, text,
// sightings and the gate's own domains, themes, idea and reason all hold the
// word marker. A person keeps a save the gate would drop, places a fleet guess
// on a personal destination, and marks a save the gate placed low-quality.
// The schedule's first time drafts one owner proposal over those three misses
// and queues one misses task. Its run reads none of the planted words, and
// gates proposals list prints its edit. In the second week the person places
// an unsure save, which is no miss, so the next time drafts nothing. The
// gate's prompt keeps its version throughout.
func TestAWeekOfMissesDraftsOneOwnerProposalThatReadsNoOutsiderText(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	version := prompts.Quality.Version
	stand := newStandInDispatch(t, p.provider)
	stand.answer = func(prompt string) map[string]any {
		a := judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.9)
		switch {
		case strings.Contains(prompt, "/comments/aa1/"):
			a = judged(qualitygate.AnswerDrop, 0.95, connectors.SidePersonal, 0.9)
		case strings.Contains(prompt, "/comments/bb2/"):
			a = judged(qualitygate.AnswerPass, 0.9, connectors.SideFleet, 0.95)
		case strings.Contains(prompt, "/comments/dd4/"):
			a = judged(qualitygate.AnswerUnsure, 0.5, connectors.SidePersonal, 0.5)
		}
		a["domains"], a["themes"], a["idea"], a["reason"] = []string{"markerdomain"}, []string{"marker theme words"}, "Marker idea words.", "Marker reason words."
		return a
	}
	_, rest, found := strings.Cut(prompts.Quality.Text(), "\n- themes:")
	if !found {
		t.Fatal("the quality prompt holds no themes line for the draft to edit")
	}
	line, _, _ := strings.Cut(rest, "\n")
	themes := "- themes:" + line
	stand.draft = func(string) map[string]any {
		return map[string]any{
			"pattern": "The gate passed thin posts and guessed fleet for personal ones.", "change": "Ask what an item adds before a pass.",
			"edits": []map[string]string{{"before": themes, "after": themes + " Name what the item adds."}}, "answers": []int{1, 2, 3},
		}
	}
	models := stand.models(t, "haiku", "medium")
	gate := p.qualityGate(t, models)
	drafts, err := misses.NewScheduler(p.st, misses.DefaultSchedule, quiet)
	if err != nil {
		t.Fatal(err)
	}
	proposals := func() (misses.List, string) {
		t.Helper()
		out, err := runCLI(t, "gates", "proposals", "list", "--json", "--config", p.config)
		var list misses.List
		if err != nil || json.Unmarshal([]byte(out), &list) != nil {
			t.Fatalf("gates proposals list printed %q, %v", out, err)
		}
		holdsTo(t, []byte(out), "prompt-proposals-list.schema.json")
		return list, out
	}

	p.landExports(t, map[string][]byte{"week": redditExport(plantedSave("t3_aa1"), plantedSave("t3_bb2"), plantedSave("t3_cc3"), plantedSave("t3_dd4"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	p.run(t, gate)
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, gate)
	for _, args := range [][]string{
		{"review", "place", "reddit/t3_aa1", "reading-list", "--cause", "title", "--note", "Worth keeping for its depth"},
		{"review", "place", "reddit/t3_bb2", "reading-list"},
		{"items", "mark", "reddit/t3_cc3", "low-quality"},
	} {
		if out, err := runCLI(t, append(args, "--config", p.config)...); err != nil {
			t.Fatalf("%v printed %q, %v", args, out, err)
		}
	}
	if _, err := drafts.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if list, _ := proposals(); len(list.Proposals) != 0 {
		t.Fatalf("before the schedule's first time the store holds %s, want no proposal", jsonOf(list))
	}

	p.advance(7 * 24 * time.Hour)
	if _, err := drafts.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	queued := slices.IndexFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageMisses })
	if queued < 0 || p.tasks(t)[queued].ProposalID == nil || p.tasks(t)[queued].ItemKey != nil {
		t.Fatalf("the schedule's first time queued %s, want one misses task naming its proposal", jsonOf(p.tasks(t)))
	}
	if out, err := runCLI(t, "tasks", "list", "--config", p.config); err != nil || !strings.Contains(out, "proposal "+*p.tasks(t)[queued].ProposalID) {
		t.Errorf("tasks list printed %q, %v, want the misses task's proposal", out, err)
	}
	if ran := p.run(t, p.modelHandler(t, models, misses.Capability)); ran != 1 {
		t.Fatalf("the worker ran %d misses tasks, want the one draft", ran)
	}
	starts := stand.starts()
	i := slices.IndexFunc(starts, func(s dispatchStart) bool { return strings.HasSuffix(s.Schema, "/prompts/misses/answer.schema.json") })
	if i < 0 || starts[i].Model != misses.DefaultModel || starts[i].Effort != misses.DefaultEffort {
		t.Fatalf("the stand-in started %s, want one draft on %s at %s", jsonOf(starts), misses.DefaultModel, misses.DefaultEffort)
	}
	sent := stand.prompt(starts[i].Key)
	if strings.Contains(strings.ToLower(sent), "marker") {
		t.Errorf("the draft's prompt carries a planted word from a title, URL, text, sighting or the gate's free text:\n%s", sent)
	}
	if !strings.Contains(sent, "Worth keeping for its depth") || !strings.Contains(sent, themes) {
		t.Errorf("the draft's prompt quotes neither the person's note nor the gate's prompt:\n%s", sent)
	}

	list, raw := proposals()
	if len(list.Proposals) != 1 {
		t.Fatalf("gates proposals list printed %s, want one proposal", raw)
	}
	got := list.Proposals[0]
	if got.Zone != store.Owner || got.State != "done" || got.Prompt != store.StageQuality || got.PromptVersion != version || got.MissCount != 3 ||
		got.LabeledAfter != nil || strings.Contains(strings.ToLower(raw), "marker") {
		t.Errorf("gates proposals list printed %s, want one done owner proposal on the quality prompt over 3 misses and no planted word", raw)
	}
	var projected []misses.Miss
	if err := json.Unmarshal(got.Misses, &projected); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"reddit/t3_aa1": "placement personal verdict", "reddit/t3_bb2": "placement personal side", "reddit/t3_cc3": "mark low-quality verdict"}
	for _, m := range projected {
		if w := m.LabelKind + " " + m.Label + " " + strings.Join(m.Missed, ","); want[m.ItemKey] != w {
			t.Errorf("the proposal holds miss %s, want %s", jsonOf(m), want[m.ItemKey])
		}
	}
	if out, err := runCLI(t, "gates", "proposals", "list", "--config", p.config); err != nil || !strings.Contains(out, "from 3 misses labeled through") ||
		!strings.Contains(out, "- "+themes) || !strings.Contains(out, "+ "+themes+" Name what the item adds.") {
		t.Errorf("gates proposals list printed %q, %v, want the draft's edit", out, err)
	}

	if _, err := runCLI(t, "review", "place", "reddit/t3_dd4", "reading-list", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.advance(7 * 24 * time.Hour)
	if _, err := drafts.Pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if list, raw := proposals(); len(list.Proposals) != 1 || slices.ContainsFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageMisses }) {
		t.Errorf("a week whose only label agreed left %s and tasks %s, want no new proposal or misses task", raw, jsonOf(p.tasks(t)))
	}

	p.landExports(t, map[string][]byte{"later": redditExport(redditSave("t3_ee5", "misc"))})
	p.parse(t, extract.Builtin())
	p.scanFindingNothing(t)
	p.run(t, gate)
	outputs := p.shown(t, "reddit/t3_ee5").Outputs
	if j := slices.IndexFunc(outputs, func(o server.OutputView) bool { return o.Stage == store.StageQuality }); j < 0 || outputs[j].ProducerVersion != version {
		t.Errorf("after the proposal the gate answered %s, want its prompt still at %s", jsonOf(outputs), version)
	}
}
