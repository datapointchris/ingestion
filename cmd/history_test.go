package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermstyle"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/views/history"
)

// Once the gate's family has earned hiding a share of its drops, a save that it hid lists under the
// history tab's dropped filter, with the gate as who decided and quality-drop as why. Placing it
// there by the articles choice delivers it to life's stand-in, lists it under placed, and records
// the person's disagreement, which returns the family to rung 0.
func TestTheHistoryTabListsAGateDropAndAPlacementThereOverrulesTheGate(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	life := newStandInLife(t, p.provider)
	stand := newStandInDispatch(t, p.provider)
	stand.answer = func(string) map[string]any { return judged(qualitygate.AnswerDrop, 0.95, connectors.SidePersonal, 0.4) }
	gate := p.qualityGate(t, stand.models(t, "haiku", "medium"))
	family := gatetrust.Family("reddit-exports", gatetrust.Answerer{PromptVersion: prompts.Quality.Version, Model: "haiku", Effort: "medium"})
	land := func(name string, from, n int) []string {
		t.Helper()
		saves, keys := make([]string, n), make([]string, n)
		for i := range n {
			id := fmt.Sprintf("t3_h%03d", from+i)
			saves[i], keys[i] = redditSave(id, "misc"), "reddit/"+id
		}
		if _, err := redditexport.Land(t.Context(), p.lander, redditexport.Drop{Connector: "reddit-exports", Name: name, Raw: redditExport(saves...)}); err != nil {
			t.Fatal(err)
		}
		p.parse(t, extract.Builtin())
		p.scanFindingNothing(t)
		p.run(t, gate)
		return keys
	}
	first := land("first", 0, 30)
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, gate)
	for _, key := range first {
		if _, err := runCLI(t, "items", "mark", key, gatetrust.MarkLowQuality, "--config", p.config); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runCLI(t, "gates", "trust", "promote", family, "--config", p.config); err != nil {
		t.Fatal(err)
	}
	var hidden string
	for _, key := range land("second", 100, 8) {
		if _, side, _ := p.decisions(t, key); side.Cause == route.CauseQualityDrop {
			hidden = key
			break
		}
	}
	if hidden == "" {
		t.Fatal("the promoted family hid none of eight drops")
	}

	out, err := runCLI(t, "tui", "--view", "history", "--json", "--config", p.config)
	if err != nil {
		t.Fatalf("tui --view history --json failed: %v", err)
	}
	holdsTo(t, []byte(out), "tui-history.schema.json")
	var every history.Snapshot
	if err := json.Unmarshal([]byte(out), &every); err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]string{}
	for _, it := range every.Items {
		outcomes[it.Item.Key] = it.Item.Outcome
	}
	if every.Outcome != nil || len(every.Items) != 40 || outcomes[hidden] != store.OutcomeDropped || outcomes[first[0]] != store.OutcomeWaiting {
		t.Errorf("the history snapshot under every outcome holds %d items, %s as %s and %s as %s, want both exports and all 38 saves, the hidden drop dropped and the shown one waiting",
			len(every.Items), hidden, outcomes[hidden], first[0], outcomes[first[0]])
	}

	board := historyTab{history.NewBoard(t.Context(), historyService{reviewService(tuiOptions{config: p.config})}, history.Outside{})}
	m := gotermboard.NewModel(board, gotermstyle.PaletteFor(io.Discard))
	m.Update(tea.WindowSizeMsg{Width: 180, Height: 40})
	for range 3 {
		runKey(t, m, letter('l'))
	}
	if err := board.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.Rebuild()
	shown := func() map[string]server.ItemListed {
		t.Helper()
		var out strings.Builder
		if err := board.print(&out, true); err != nil {
			t.Fatal(err)
		}
		holdsTo(t, []byte(out.String()), "tui-history.schema.json")
		var snap history.Snapshot
		if err := json.Unmarshal([]byte(out.String()), &snap); err != nil {
			t.Fatal(err)
		}
		items := map[string]server.ItemListed{}
		for _, it := range snap.Items {
			if snap.Outcome == nil || it.Item.Outcome != *snap.Outcome {
				t.Errorf("under the filter %v the tab shows %s as %s", snap.Outcome, it.Item.Key, it.Item.Outcome)
			}
			items[it.Item.Key] = it.Item
		}
		return items
	}
	dropped := shown()
	if d := dropped[hidden].Decided; d == nil || d.Decider != server.DeciderGate || d.Cause != route.CauseQualityDrop {
		t.Fatalf("under dropped the tab shows %s as %s, want the gate's quality-drop", hidden, jsonOf(dropped[hidden]))
	}
	if _, listed := dropped[first[0]]; listed {
		t.Errorf("under dropped the tab shows %s, which waits in review", first[0])
	}
	var frame strings.Builder
	if err := board.print(&frame, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"history", "outcome dropped", "l outcome: every outcome placed archived [dropped] waiting split", hidden, "quality-drop"} {
		if !strings.Contains(frame.String(), want) {
			t.Errorf("the frame lacks %q:\n%s", want, frame.String())
		}
	}
	t.Logf("the history tab under the dropped filter:\n%s", frame.String())

	if !m.MoveCursorTo(hidden) {
		t.Fatalf("the cursor does not reach %s", hidden)
	}
	runKey(t, m, letter('A'))
	p.run(t, p.lifeDeliverer(t, life))
	if life.find(func(a *standInArticle) bool { return text(a.sourceKey) == hidden }) == nil {
		t.Errorf("life holds no article under %s after the articles key placed it from history", hidden)
	}
	if n := p.count(t, `SELECT count(*) FROM gate_trust_decisions WHERE family = ? AND item_key = ? AND kind = ? AND agreement = ? AND decided_by = ?`,
		family, hidden, gatetrust.KindPlacement, gatetrust.Disagreed, route.ByPerson("person")); n != 1 {
		t.Errorf("gate trust holds %d of the person's disagreements on %s, want 1", n, hidden)
	}
	if out, err := runCLI(t, "gates", "trust", "list", "--json", "--config", p.config); err != nil || !strings.Contains(out, `"last_change":"placement"`) {
		t.Errorf("gates trust list printed %q, %v, want the family moved last by the placement", out, err)
	}
	if err := board.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, listed := shown()[hidden]; listed {
		t.Errorf("after the placement the dropped filter still shows %s", hidden)
	}
	board.ShowOutcome(store.OutcomePlaced)
	if err := board.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if placed := shown()[hidden]; placed.Decided == nil || placed.Decided.Decider != server.DeciderPerson || text(placed.Decided.Destination) != "reading-list" ||
		!slices.Contains([]string{"placed", "read", "delivered"}, placed.Phase) {
		t.Errorf("under placed the tab shows %s as %s, want the person's placement on reading-list", hidden, jsonOf(placed))
	}
}
