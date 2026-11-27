package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/ypl"
)

// sortSet is a declaration set of one ypl playlist polled once, whose videos a person sorts, and a
// destination for each verb that places on one.
func sortSet(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"connectors/ypl-saves.yml": "name: ypl-saves\nmode: poll\nsource: ypl\nzone: outside\nperson_sorts: true\nschedule: once\n" +
			"source_parameters:\n  playlist: saves\n",
		"destinations.yml": "destinations:\n" +
			"  - name: reading-list\n    side: personal\n    default_for_side: true\n" +
			"  - name: work-inbox\n    side: fleet\n    default_for_side: true\n" +
			"  - name: learning-plan\n    side: personal\n    deliverer: learning-resources\n" +
			"  - name: watch\n    side: personal\n    deliverer: ypl-watch\n" +
			"  - name: dropped\n",
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// One video waits in review for each verb, and a person sorts each by its verb's row:
//
//   - fleet relate places it on the fleet side's default, read by a model under the fleet lens,
//     which queues the note's draft and then the model read
//   - learning delivers a video that the quality gate passed to the learning plan, and sends one
//     that it would drop back to review as learning-refused, beside the gate's reason
//   - personal relate, personal summarize and technical summarize shelve it, and nothing runs
//     until it is opened. Opening a relate hands its parse to psyche here. Opening a summary
//     queues one model read under its lens, and a second opening shows the kept summary with no
//     model call
//   - watch queues one ypl-watch delivery to the playlist picked, which a spent quota holds with
//     no attempt spent, and which adds the video once however the item is sorted again
//   - a drop keeps its cause
func TestEachSortVerbRunsWhatItsRouteNames(t *testing.T) {
	stand, api := newStandInYPL(t)
	videos := map[string]string{
		"fleetvid001": route.VerbFleetRelate, "learnkeep01": route.VerbLearning, "learndrop01": route.VerbLearning,
		"relatevid01": route.VerbPersonalRelate, "summvideo01": route.VerbPersonalSummarize, "techvideo01": route.VerbTechnicalSummarize,
		"watchvid001": route.VerbWatch, "dropvideo01": "drop",
	}
	stand.playlistIDs["saves"], stand.playlistIDs["Later"], stand.playlistIDs["Other"] = "PLsaves", "PLlater", "PLother"
	n := 0
	for id := range videos {
		n++
		stand.add(standInVideo{id: id, title: "Test video " + id})
		stand.playlists["PLsaves"] = append(stand.playlists["PLsaves"], standInItem{itemID: "save-" + id, videoID: id})
	}
	stand.refuseInsert["watchvid001"] = time.Hour

	p := newPipeline(t, extract.Builtin(), afterStage(quiet))
	p.apply(t, sortSet(t))
	if out, err := runCLI(t, "connectors", "release", "ypl-saves", "--config", p.config); err != nil {
		t.Fatalf("connectors release printed %q, %v", out, err)
	}
	dispatch := newStandInDispatch(t, p.provider)
	dispatch.answer = func(prompt string) map[string]any {
		if strings.Contains(prompt, "learndrop01") {
			return judged(qualitygate.AnswerDrop, 0.95, connectors.SidePersonal, 0.9)
		}
		return judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.9)
	}
	dispatch.note = func(string) map[string]any { return drafted(0.95, "garden") }
	dispatch.read = func(string) map[string]any { return readAnswered("The summary of the video.", "") }
	models := dispatch.models(t, "", "")
	handlers := yplModelHandlers(t, p, api.URL, models)
	watch := handlers[store.DeliverCapability(route.WatchDeliverer)]
	if watch == nil {
		t.Fatalf("a worker with a [source.ypl] table holds no %s handler", route.WatchDeliverer)
	}
	p.runConnector(t, "ypl-saves")
	p.run(t, handlers[store.PollCapability(ypl.Source)])
	p.run(t, standInCaptions{})
	p.scanFindingNothing(t)
	p.run(t, p.qualityGate(t, models))
	if waiting := p.count(t, `SELECT count(*) FROM items WHERE phase = 'review'`); waiting != n {
		t.Fatalf("%d of the %d videos wait in review", waiting, n)
	}

	sort := func(key, verb string, more ...string) string {
		t.Helper()
		out, err := runCLI(t, append([]string{"review", "sort", "youtube/" + key, verb, "--config", p.config}, more...)...)
		if err != nil {
			t.Fatalf("review sort %s %s printed %q, %v", key, verb, out, err)
		}
		return out
	}
	tasks := func(key, stage string) int {
		t.Helper()
		return p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = ? AND state IN ('queued', 'leased')`, "youtube/"+key, stage)
	}

	sort("fleetvid001", route.VerbFleetRelate)
	if _, side, read := p.decisions(t, "youtube/fleetvid001"); side.Verdict != connectors.SideFleet || text(side.Destination) != "work-inbox" ||
		side.Cause != route.VerbFleetRelate || side.DecidedBy != route.ByPerson("person") || read.Verdict != connectors.ReadModel || text(read.Lens) != route.LensFleet {
		t.Errorf("fleet-relate decided %+v and read %+v, want the person's fleet side on work-inbox, read by a model under the fleet lens", side, read)
	}
	if tasks("fleetvid001", store.StageNote) != 1 {
		t.Errorf("fleet-relate queued no note's draft")
	}
	p.run(t, p.noteDrafter(t, models, nil))
	if tasks("fleetvid001", store.StageModelRead) != 1 {
		t.Errorf("fleet-relate's drafted note queued no model read")
	}

	learning := newStandInLearning(t, p.provider)
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	cfg := fileConfig{Deliver: map[string]deliverTable{deliver.LearningResources: {serviceClient: serviceClient{API: learning.URL, ClientID: learningClient}}}}
	learn := deliver.Handler(deliver.LearningResources, openDeliverers(t.Context(), cfg, p.provider.URL, quiet)[deliver.LearningResources])
	sort("learnkeep01", route.VerbLearning)
	p.run(t, learn)
	if sent := learning.sent("youtube/learnkeep01"); len(sent) != 1 || sent[0].ResourceType != "video" || sent[0].URL != "https://www.youtube.com/watch?v=learnkeep01" {
		t.Errorf("learning sent %+v, want one video resource under the item's URL", sent)
	}
	if said := sort("learndrop01", route.VerbLearning); !strings.Contains(said, route.CauseLearningRefused) {
		t.Errorf("a learning sort that the gate would drop said %q, want it back in review as %s", said, route.CauseLearningRefused)
	}
	out, err := runCLI(t, "review", "list", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "review-list.schema.json")
	var queue server.ReviewAnswer
	_ = json.Unmarshal([]byte(out), &queue)
	refused := false
	for _, it := range queue.Items {
		var gate qualitygate.Answer
		if it.Key == "youtube/learndrop01" && it.Reason == route.CauseLearningRefused && json.Unmarshal(it.QualityAnswer, &gate) == nil && gate.Reason != "" {
			refused = true
		}
	}
	if !refused || len(learning.sent("youtube/learndrop01")) != 0 || p.count(t, `SELECT count(*) FROM deliveries WHERE item_key = ?`, "youtube/learndrop01") != 0 {
		t.Errorf("the refused learning sort is not back in review as %s with the gate's reason, or it was delivered", route.CauseLearningRefused)
	}

	dispatches := len(dispatch.starts())
	for key, verb := range map[string]string{"relatevid01": route.VerbPersonalRelate, "summvideo01": route.VerbPersonalSummarize, "techvideo01": route.VerbTechnicalSummarize} {
		if said := sort(key, verb); !strings.Contains(said, "Shelved") {
			t.Errorf("review sort %s %s said %q, want it shelved", key, verb, said)
		}
	}
	p.run(t, p.modelRead(t, models, nil, sentenceScanner{}), p.noteDrafter(t, models, nil))
	if got := len(dispatch.starts()); got != dispatches+1 || p.count(t, `SELECT count(*) FROM items WHERE phase = 'shelved'`) != 3 {
		t.Errorf("after the shelf sorts the stand-in saw %d new dispatches and %d items are shelved, want fleet-relate's read alone and all three",
			got-dispatches, p.count(t, `SELECT count(*) FROM items WHERE phase = 'shelved'`))
	}
	shelf, err := runCLI(t, "shelf", "list", "--json", "--config", p.config)
	if err != nil || !strings.Contains(shelf, `"state": "waiting"`) && !strings.Contains(shelf, `"state":"waiting"`) {
		t.Fatalf("shelf list printed %q, %v", shelf, err)
	}
	holdsTo(t, []byte(shelf), "shelf-list.schema.json")

	kept := installFakePsyche(t)
	if out, err := runCLI(t, "shelf", "open", "youtube/relatevid01", "--config", p.config); err != nil {
		t.Fatalf("shelf open of a relate printed %q, %v", out, err)
	}
	if args, err := os.ReadFile(filepath.Join(kept, "args")); err != nil || !strings.HasPrefix(string(args), "relate --parse -") {
		t.Errorf("opening a relate ran psyche with %q, %v, want relate --parse -", args, err)
	}

	open := func(key string) server.ShelfOpenAnswer {
		t.Helper()
		out, err := runCLI(t, "shelf", "open", "youtube/"+key, "--json", "--config", p.config)
		var answer server.ShelfOpenAnswer
		if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
			t.Fatalf("shelf open %s printed %q, %v", key, out, err)
		}
		holdsTo(t, []byte(out), "shelf-open.schema.json")
		return answer
	}
	if first := open("summvideo01"); !first.Opened || first.Item.State != route.ShelfReading || tasks("summvideo01", store.StageModelRead) != 1 {
		t.Errorf("the first opening of a summary answered %+v, want it opened and one model read queued", first)
	}
	if first := open("techvideo01"); text(first.Item.Lens) != route.LensLearning || tasks("techvideo01", store.StageModelRead) != 1 {
		t.Errorf("opening a technical summary answered %+v, want one model read queued under the learning lens", first)
	}
	p.run(t, p.modelRead(t, models, nil, sentenceScanner{}))
	before := len(dispatch.starts())
	again := open("summvideo01")
	if again.Opened || again.Item.State != route.ShelfDone || len(again.Item.Summary) == 0 || tasks("summvideo01", store.StageModelRead) != 0 ||
		len(dispatch.starts()) != before {
		t.Errorf("the second opening answered %+v and the stand-in saw %d new dispatches, want the kept summary with no model call",
			again, len(dispatch.starts())-before)
	}
	if out, err := runCLI(t, "shelf", "open", "youtube/summvideo01", "--config", p.config); err != nil || !strings.Contains(out, "The summary of the video.") {
		t.Errorf("shelf open of a kept summary printed %q, %v", out, err)
	}

	if _, err := runCLI(t, "review", "sort", "youtube/watchvid001", route.VerbWatch, "--config", p.config); err == nil {
		t.Error("a watch that picks no playlist, where serve names no default, was not refused")
	}
	sort("watchvid001", route.VerbWatch, "--playlist", "Later")
	if n := p.count(t, `SELECT count(*) FROM deliveries WHERE item_key = ? AND destination = 'watch' AND playlist = 'Later'`, "youtube/watchvid001"); n != 1 {
		t.Fatalf("watch queued %d deliveries to the watch destination's picked playlist, want one", n)
	}
	p.run(t, watch)
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'deliver' AND attempts = 0 AND waiting_since_ts IS NOT NULL`,
		"youtube/watchvid001"); n != 1 || len(stand.inserts) != 0 {
		t.Errorf("a spent quota left %d waiting deliver tasks with no attempt spent and %d inserts, want one and none", n, len(stand.inserts))
	}
	p.advance(2 * time.Hour)
	p.run(t, watch)
	sort("watchvid001", route.VerbWatch, "--playlist", "Other")
	p.advance(2 * time.Hour)
	p.run(t, watch)
	if shown := p.item(t, "youtube/watchvid001"); len(stand.inserts) != 1 || stand.inserts[0] != "PLlater/watchvid001" ||
		len(shown.Deliveries) != 1 || shown.Deliveries[0].Outcome != store.DeliveryDelivered || text(shown.Deliveries[0].Playlist) != "Later" {
		t.Errorf("ypl saw inserts %v and the item holds deliveries %s, want the video added once to Later", stand.inserts, jsonOf(shown.Deliveries))
	}

	if out, err := runCLI(t, "review", "drop", "youtube/dropvideo01", "--cause", route.CauseChannel, "--config", p.config); err != nil {
		t.Fatalf("review drop printed %q, %v", out, err)
	}
	if _, side, _ := p.decisions(t, "youtube/dropvideo01"); side.Destination != nil || side.Cause != route.CauseChannel {
		t.Errorf("the drop holds %+v, want no destination and the cause channel", side)
	}

	t.Setenv("COLUMNS", "160")
	t.Setenv("LINES", "30")
	frame, err := runCLI(t, "tui", "--view", "shelf", "--once", "--config", p.config)
	if err != nil || !strings.Contains(frame, "personal-relate") || !strings.Contains(frame, "opened") || !strings.Contains(frame, "done") {
		t.Errorf("the shelf tab's frame lacks its rows: %v\n%s", err, frame)
	}
	snap, err := runCLI(t, "tui", "--view", "shelf", "--json", "--config", p.config)
	if err != nil || !strings.Contains(snap, "The summary of the video.") {
		t.Fatalf("the shelf tab's snapshot lacks the kept summary: %v\n%s", err, snap)
	}
	holdsTo(t, []byte(snap), "tui-shelf.schema.json")
}
