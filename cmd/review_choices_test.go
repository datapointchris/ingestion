package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/ypl"
)

// choiceSet is a declaration set of one ypl playlist polled once, whose videos a person places,
// and a destination for each choice that places on one.
func choiceSet(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"connectors/ypl-saves.yml": "name: ypl-saves\nmode: poll\nsource: ypl\nzone: outside\nperson_sorts: true\nschedule: once\n" +
			"source_parameters:\n  playlist: saves\n",
		"destinations.yml": "destinations:\n" +
			"  - name: reading-list\n    side: personal\n    default_for_side: true\n    deliverer: life-articles\n" +
			"  - name: work-inbox\n    side: fleet\n    default_for_side: true\n" +
			"  - name: learning-plan\n    side: personal\n    deliverer: learning-resources\n" +
			"  - name: watch\n    side: personal\n    deliverer: ypl-watch\n" +
			"  - name: psyche\n    side: personal\n    push_only: true\n" +
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

// One video waits in review for each choice, and a person places each by its choice:
//
//   - articles delivers it to life's articles, and learning to the learning plan, though the
//     quality gate would drop it
//   - suggestion places it on the fleet side's default, read by a model under the fleet lens,
//     which drafts the note and then reads it
//   - watch queues one ypl-watch delivery to the playlist picked, which a spent quota holds with
//     no attempt spent. Its insert lands and the gateway loses the answer, so the retry finds the
//     video in the playlist and adds it no second time. A watch naming another playlist is
//     refused, and so is a placement on the watch destination, which picks no playlist
//   - psyche places it at psyche, and psyche relate reads its parse on this machine
//   - archive keeps it on the personal side with no destination and no delivery, with what was
//     liked as its cause and the read mark
//   - a drop for low quality keeps its cause and writes the low-quality mark
//
// Each placement and the drop then label the gate's agreement, and the archive labels nothing.
func TestEachReviewChoiceReachesWhatItNames(t *testing.T) {
	stand, api := newStandInYPL(t)
	videos := []string{"articlevid1", "learndrop01", "suggestvid1", "watchvid001", "psychevid01", "archivevid1", "dropvideo01"}
	stand.playlistIDs["saves"], stand.playlistIDs["Later"], stand.playlistIDs["Other"] = "PLsaves", "PLlater", "PLother"
	for _, id := range videos {
		stand.add(standInVideo{id: id, title: "Test video " + id})
		stand.playlists["PLsaves"] = append(stand.playlists["PLsaves"], standInItem{itemID: "save-" + id, videoID: id})
	}
	stand.refuseInsert["watchvid001"] = time.Hour
	stand.loseInsert["watchvid001"] = true

	p := newPipeline(t, extract.Builtin(), afterStage(quiet))
	p.apply(t, choiceSet(t))
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
	dispatch.read = func(string) map[string]any {
		return readAnswered("The summary of the video.", "A suggestion for the work.")
	}
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
	if waiting := p.count(t, `SELECT count(*) FROM items WHERE phase = 'review'`); waiting != len(videos) {
		t.Fatalf("%d of the %d videos wait in review", waiting, len(videos))
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
	var offered []string
	for _, c := range queue.Choices {
		offered = append(offered, c.Choice)
		if c.Missing != nil {
			t.Errorf("the set declares a destination for every choice, and %s answers %q", c.Choice, *c.Missing)
		}
	}
	if !slices.Equal(offered, route.ChoiceNames()) {
		t.Errorf("review offers %v, want %v", offered, route.ChoiceNames())
	}

	place := func(key, choice string, more ...string) {
		t.Helper()
		out, err := runCLI(t, append([]string{"review", "place", "youtube/" + key, choice, "--config", p.config}, more...)...)
		if err != nil {
			t.Fatalf("review place %s %s printed %q, %v", key, choice, out, err)
		}
	}
	tasks := func(key, stage string) int {
		t.Helper()
		return p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = ? AND state IN ('queued', 'leased')`, "youtube/"+key, stage)
	}
	marks := func(key, mark string) int {
		t.Helper()
		return p.count(t, `SELECT count(*) FROM marks WHERE item_key = ? AND mark = ? AND decided_by = ?`, "youtube/"+key, mark, route.ByPerson("person"))
	}

	life := newStandInLife(t, p.provider)
	place("articlevid1", route.ChoiceArticles)
	p.run(t, p.lifeDeliverer(t, life))
	if articles, _ := life.counts(); articles != 1 || life.find(func(a *standInArticle) bool { return a.url == "https://www.youtube.com/watch?v=articlevid1" }) == nil {
		t.Errorf("life holds %d articles, want the one video under its URL", articles)
	}

	learning := newStandInLearning(t, p.provider)
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	cfg := fileConfig{Deliver: map[string]deliverTable{deliver.LearningResources: {serviceClient: serviceClient{API: learning.URL, ClientID: learningClient}}}}
	place("learndrop01", route.ChoiceLearning)
	p.run(t, deliver.Handler(deliver.LearningResources, openDeliverers(t.Context(), cfg, p.provider.URL, quiet)[deliver.LearningResources]))
	if sent := learning.sent("youtube/learndrop01"); len(sent) != 1 || sent[0].ResourceType != "video" {
		t.Errorf("learning sent %+v, want the video that the gate would drop as one video resource", sent)
	}

	place("suggestvid1", route.ChoiceSuggestion)
	if _, side, read := p.decisions(t, "youtube/suggestvid1"); side.Verdict != connectors.SideFleet || text(side.Destination) != "work-inbox" ||
		side.DecidedBy != route.ByPerson("person") || read.Verdict != connectors.ReadModel || text(read.Lens) != route.LensFleet {
		t.Errorf("suggestion decided %+v and read %+v, want the person's fleet side on work-inbox, read by a model under the fleet lens", side, read)
	}
	if tasks("suggestvid1", store.StageNote) != 1 {
		t.Fatal("suggestion queued no note's draft")
	}
	p.run(t, p.noteDrafter(t, models, nil))
	p.run(t, p.modelRead(t, models, nil, sentenceScanner{}))
	read := slices.IndexFunc(p.item(t, "youtube/suggestvid1").Outputs, func(o server.OutputView) bool {
		return o.Stage == store.StageModelRead && o.State == store.OutputDone
	})
	if read < 0 {
		t.Error("the suggestion's model read kept no answer")
	}

	if _, err := runCLI(t, "review", "place", "youtube/watchvid001", route.ChoiceWatch, "--config", p.config); err == nil {
		t.Error("a watch that picks no playlist, where serve names no default, was not refused")
	}
	if out, err := placeOnDestination(t, p, "youtube/watchvid001", "watch"); err == nil {
		t.Errorf("a placement on the watch destination printed %q, want it refused, because only the watch choice picks a playlist", out)
	}
	place("watchvid001", route.ChoiceWatch, "--playlist", "Later")
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
	p.advance(2 * time.Hour)
	p.run(t, watch)
	if out, err := runCLI(t, "review", "place", "youtube/watchvid001", route.ChoiceWatch, "--playlist", "Other", "--config", p.config); err == nil {
		t.Errorf("a watch to Other after one to Later printed %q, want it refused", out)
	}
	place("watchvid001", route.ChoiceWatch, "--playlist", "Later")
	if shown := p.item(t, "youtube/watchvid001"); len(stand.inserts) != 1 || stand.inserts[0] != "PLlater/watchvid001" ||
		len(shown.Deliveries) != 1 || shown.Deliveries[0].Outcome != store.DeliveryDelivered || text(shown.Deliveries[0].Playlist) != "Later" {
		t.Errorf("ypl saw inserts %v and the item holds deliveries %s, want the video added once to Later", stand.inserts, jsonOf(shown.Deliveries))
	}

	kept := installFakePsyche(t)
	place("psychevid01", route.ChoicePsyche)
	if args, err := os.ReadFile(filepath.Join(kept, "args")); err != nil || !strings.HasPrefix(string(args), "relate --parse -") {
		t.Errorf("the psyche choice ran psyche with %q, %v, want relate --parse -", args, err)
	}
	if _, side, read := p.decisions(t, "youtube/psychevid01"); text(side.Destination) != route.PsycheDestination || read.Verdict != connectors.ReadAsIs {
		t.Errorf("psyche decided %+v and read %+v, want the item at psyche, read as is", side, read)
	}

	if out, err := runCLI(t, "review", "archive", "youtube/archivevid1", "--liked", route.LikedChannel, "--config", p.config); err != nil {
		t.Fatalf("review archive printed %q, %v", out, err)
	}
	if _, side, read := p.decisions(t, "youtube/archivevid1"); side.Verdict != connectors.SidePersonal || side.Destination != nil ||
		side.Cause != route.LikedChannel || read.Verdict != store.VerdictArchive {
		t.Errorf("archive decided %+v and read %+v, want the personal side with no destination, liked for its channel, read as archived", side, read)
	}
	if shown := p.item(t, "youtube/archivevid1"); shown.Item.Phase != store.PhaseArchived || len(shown.Deliveries) != 0 || marks("archivevid1", gatetrust.MarkRead) != 1 {
		t.Errorf("the archived item reads phase %s with %d deliveries and %d read marks, want archived, none and one",
			shown.Item.Phase, len(shown.Deliveries), marks("archivevid1", gatetrust.MarkRead))
	}

	if out, err := runCLI(t, "review", "drop", "youtube/dropvideo01", "--cause", route.CauseLowQuality, "--config", p.config); err != nil {
		t.Fatalf("review drop printed %q, %v", out, err)
	}
	if _, side, _ := p.decisions(t, "youtube/dropvideo01"); side.Verdict != store.VerdictDrop || side.Destination != nil ||
		side.Cause != route.CauseLowQuality || marks("dropvideo01", gatetrust.MarkLowQuality) != 1 {
		t.Errorf("the drop holds %+v with %d low-quality marks, want no destination, the cause low-quality and one mark",
			side, marks("dropvideo01", gatetrust.MarkLowQuality))
	}

	out, err = runCLI(t, "gates", "agreement", "--json", "--config", p.config)
	var agreement accuracy.Report
	if err != nil || json.Unmarshal([]byte(out), &agreement) != nil || agreement.Labels.Items != len(videos)-1 || agreement.Labels.Drop != 1 {
		t.Errorf("gates agreement labeled %+v, %v, want each choice's item but the archive's, one of them a drop", agreement.Labels, err)
	}
}

// placeOnDestination places the item on destination through the place route with a destination
// in its body, as a caller that names destinations rather than choices does.
func placeOnDestination(t *testing.T, p *pipeline, key, destination string) (string, error) {
	t.Helper()
	answer, _, err := decideReview(t.Context(), p.config, "", server.RouteReviewPlace, key, "place", server.PlaceRequest{Destination: destination})
	return jsonOf(answer), err
}
