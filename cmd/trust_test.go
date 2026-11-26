package cmd

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/datapointchris/earnedtrust/seededhash"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// The gate passes a save that a tag placed, whose placement stands, and a
// read on it agrees and climbs nothing. It drops every other save. While its
// family stands on rung 0, each drop waits in review as gate-would-drop.
// Placing a gate-would-drop save on a destination disagrees, and returns the
// family's run to 0. Thirty straight low-quality marks earn asking, a
// repeated mark counts once, and one promote climbs the family to rung 1. Of
// the next saves the gate drops, the seeded draw over each answer shows its
// share in review, and the rest drop as quality-drop with their rows and ideas
// kept. Thirty agreeing marks on drops answered after the promote climb it to
// rung 2. Another model's drops all show, because its family starts on rung 0.
// A read on a hidden drop disagrees, which returns the promoted family
// to rung 0, where every drop shows. Placing a hidden drop on a destination
// disagrees too.
func TestAGateFamilyEarnsTrustBeforeItHidesAShareOfItsDrops(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = func(prompt string) map[string]any {
		if strings.Contains(prompt, "/comments/tg1/") {
			return judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.4)
		}
		return judged(qualitygate.AnswerDrop, 0.95, connectors.SidePersonal, 0.4)
	}
	gate := p.qualityGate(t, stand.models(t, "haiku", "medium"))
	answerer := gatetrust.Answerer{PromptVersion: prompts.Quality.Version, Model: "haiku", Effort: "medium"}
	family := gatetrust.Family("reddit-exports", answerer)
	land := func(name string, tags []string, ids ...string) []string {
		saves, keys := make([]string, 0, len(ids)), make([]string, 0, len(ids))
		for _, id := range ids {
			saves, keys = append(saves, redditSave(id, "misc")), append(keys, "reddit/"+id)
		}
		if _, err := redditexport.Land(t.Context(), p.lander, redditexport.Drop{Connector: "reddit-exports", Name: name, Tags: tags, Raw: redditExport(saves...)}); err != nil {
			t.Fatal(err)
		}
		p.parse(t, extract.Builtin())
		p.scanFindingNothing(t)
		p.run(t, gate)
		return keys
	}
	ids := func(from, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("t3_m%03d", from+i)
		}
		return out
	}
	mark := func(key, mark string) server.MarkAnswer {
		t.Helper()
		out, err := runCLI(t, "items", "mark", key, mark, "--json", "--config", p.config)
		var a server.MarkAnswer
		if err != nil || json.Unmarshal([]byte(out), &a) != nil {
			t.Fatalf("items mark %s %s printed %q, %v", key, mark, out, err)
		}
		holdsTo(t, []byte(out), "item-mark.schema.json")
		return a
	}
	answerOf := func(key string) string {
		t.Helper()
		all, _, _ := p.decisions(t, key)
		for _, d := range all {
			if d.Gate == store.GateQuality {
				return d.DecidedBy
			}
		}
		t.Fatalf("the gate has not answered %s", key)
		return ""
	}
	standingOf := func(name string) store.GateTrust {
		t.Helper()
		out, err := runCLI(t, "gates", "trust", "list", "--json", "--config", p.config)
		var a server.GateTrustAnswer
		if err != nil || json.Unmarshal([]byte(out), &a) != nil {
			t.Fatalf("gates trust list printed %q, %v", out, err)
		}
		holdsTo(t, []byte(out), "gate-trust-list.schema.json")
		for _, g := range a.Families {
			if g.Family == name {
				return g
			}
		}
		t.Fatalf("gates trust list printed %q, want the family %s", out, name)
		return store.GateTrust{}
	}
	standing := func() store.GateTrust { t.Helper(); return standingOf(family) }

	tagged := land("tagged", []string{"read-later"}, "t3_tg1")[0]
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	first := land("first", nil, ids(0, 36)...)
	if _, side, _ := p.decisions(t, tagged); side.DecidedBy != route.ByTag || side.Verdict != connectors.SidePersonal {
		t.Errorf("the tagged save is %s by %s after the gate passed it, want its tag's placement standing", side.Verdict, side.DecidedBy)
	}
	if a := mark(tagged, gatetrust.MarkRead); a.Mark.Agreement != gatetrust.Agreed || len(a.Families) != 0 {
		t.Errorf("read on a pass answered %s, want agreed with no family moved", jsonOf(a))
	}
	for _, key := range first {
		if _, side, _ := p.decisions(t, key); side.Verdict != route.VerdictReview || side.Cause != route.CauseGateWouldDrop {
			t.Fatalf("on rung 0 the gate's drop of %s is %s for %s, want review as %s", key, side.Verdict, side.Cause, route.CauseGateWouldDrop)
		}
	}
	if g := standing(); g.Rung != 0 || g.Share != 1 || g.StraightRun != 0 || g.AsksPromotion || g.Threshold != 30 {
		t.Errorf("before any mark the family stands at %s, want rung 0 showing every drop", jsonOf(g))
	}

	for _, key := range first[:5] {
		mark(key, gatetrust.MarkLowQuality)
	}
	if _, err := runCLI(t, "review", "place", first[5], "reading-list", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if g := standing(); g.StraightRun != 0 || g.LastChange != gatetrust.ChangePlacement {
		t.Errorf("after a gate-would-drop save was placed the family stands at %s, want its run back at 0", jsonOf(g))
	}
	for i, key := range first[6:] {
		if a := mark(key, gatetrust.MarkLowQuality); a.Mark.Agreement != gatetrust.Agreed || len(a.Families) != 1 ||
			a.Families[0].AsksPromotion != (i == 29) {
			t.Fatalf("low-quality mark %d answered %s, want agreed, asking only at the 30th", i+1, jsonOf(a))
		}
	}
	if a := mark(first[6], gatetrust.MarkLowQuality); a.Mark.Sequence != 1 || a.Families[0].StraightRun != 30 {
		t.Errorf("a repeated mark answered %s, want the first mark and a run of 30", jsonOf(a))
	}
	out, err := runCLI(t, "gates", "trust", "promote", family, "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "gate-trust-promotion.schema.json")
	if g := standing(); g.Rung != 1 || g.Share != 0.25 || g.AsksPromotion || text(g.PromotedBy) != "person:person" {
		t.Errorf("after the promote the family stands at %s, want rung 1 showing 25%% of drops", jsonOf(g))
	}

	next := land("second", nil, ids(100, 40)...)
	var hidden []string
	for _, key := range next {
		_, side, _ := p.decisions(t, key)
		if seededhash.Fraction(family, answerOf(key)) < 0.25 {
			if side.Verdict != route.VerdictReview || side.Cause != route.CauseGateWouldDrop {
				t.Errorf("%s falls in the draw's share, and is %s for %s, want review as %s", key, side.Verdict, side.Cause, route.CauseGateWouldDrop)
			}
			continue
		}
		hidden = append(hidden, key)
		if side.Verdict != route.VerdictDrop || side.Cause != route.CauseQualityDrop || p.phase(t, key) != "dropped" {
			t.Errorf("%s falls outside the draw's share, and is %s for %s, want dropped as %s", key, side.Verdict, side.Cause, route.CauseQualityDrop)
		}
	}
	if len(hidden) == 0 || len(hidden) == len(next) {
		t.Fatalf("the draw hid %d of %d drops, want some shown and some hidden", len(hidden), len(next))
	}
	if out, err := runCLI(t, "items", "show", hidden[0], "--config", p.config); err != nil || !strings.Contains(out, `idea "The drop idea in one line."`) {
		t.Errorf("items show on a hidden drop printed %q, %v, want its row and idea kept", out, err)
	}

	marked := slices.DeleteFunc(slices.Clone(next), func(key string) bool { return slices.Contains(hidden[:2], key) })
	for _, key := range marked[:30] {
		mark(key, gatetrust.MarkLowQuality)
	}
	if g := standing(); g.Rung != 2 || g.Share != 0.1875 {
		t.Errorf("after thirty marks on drops answered since the promote the family stands at %s, want rung 2", jsonOf(g))
	}

	trusted := gate
	gate = p.qualityGate(t, stand.models(t, "sonnet", "medium"))
	wouldHide := 0
	for _, key := range land("another model", nil, ids(300, 8)...) {
		if seededhash.Fraction(family, answerOf(key)) >= 0.1875 {
			wouldHide++
		}
		if _, side, _ := p.decisions(t, key); side.Verdict != route.VerdictReview || side.Cause != route.CauseGateWouldDrop {
			t.Errorf("another model's drop of %s is %s for %s, want review as %s", key, side.Verdict, side.Cause, route.CauseGateWouldDrop)
		}
	}
	if wouldHide == 0 {
		t.Fatal("the rung-2 family's draw would hide none of another model's drops, so the step shows nothing")
	}
	answerer.Model = "sonnet"
	if g := standingOf(gatetrust.Family("reddit-exports", answerer)); g.Rung != 0 || g.Model != "sonnet" || g.Effort != "medium" {
		t.Errorf("another model's family stands at %s, want rung 0 under sonnet at medium", jsonOf(g))
	}
	gate = trusted
	if a := mark(hidden[0], gatetrust.MarkRead); a.Mark.Agreement != gatetrust.Disagreed {
		t.Errorf("read on a hidden drop answered %s, want disagreed", jsonOf(a))
	}
	if g := standing(); g.Rung != 0 || g.Share != 1 || g.StraightRun != 0 || g.PromotedBy == nil {
		t.Errorf("after read on a drop the family stands at %s, want rung 0 showing every drop, still promoted", jsonOf(g))
	}
	if p.phase(t, hidden[0]) != "dropped" {
		t.Errorf("the marked drop is %s, want it left where it was", p.phase(t, hidden[0]))
	}
	for _, key := range marked[30:] {
		mark(key, gatetrust.MarkLowQuality)
	}
	if _, err := runCLI(t, "review", "place", hidden[1], "reading-list", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if g := standing(); g.StraightRun != 0 || g.LastChange != gatetrust.ChangePlacement {
		t.Errorf("after a hidden drop was placed the family stands at %s, want its run back at 0", jsonOf(g))
	}
	for _, key := range land("third", nil, ids(200, 8)...) {
		if _, side, _ := p.decisions(t, key); side.Verdict != route.VerdictReview || side.Cause != route.CauseGateWouldDrop {
			t.Errorf("back on rung 0 the drop of %s is %s for %s, want review as %s", key, side.Verdict, side.Cause, route.CauseGateWouldDrop)
		}
	}
}
