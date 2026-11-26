package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deletion"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// videoSource is the source of the scenarios' connectors, whose deleter the
// tests register, and videoMediaType the media type its arrivals carry.
const (
	videoSource    = "test-videos"
	videoMediaType = "application/x-youtube-video"
)

func init() { deletion.Register(videoSource) }

// videoSet is a declaration set of three connectors of videoSource: a capture
// that deletes each landed video from its source, a backfill polled once, and
// a channel whose tag places its videos on the fleet side. Each leaves the
// side to a person.
func videoSet(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"connectors/video-capture.yml": "name: video-capture\nmode: poll\nsource: " + videoSource + "\nschedule: \"@every 30m\"\n" +
			"zone: outside\ntags: [video]\nafter_landing: delete\nperson_sorts: true\n",
		"connectors/video-backfill.yml": "name: video-backfill\nmode: poll\nsource: " + videoSource + "\nschedule: once\n" +
			"zone: outside\nperson_sorts: true\n",
		"connectors/video-channel.yml": "name: video-channel\nmode: poll\nsource: " + videoSource + "\nschedule: \"@hourly\"\n" +
			"zone: outside\ntags: [talks]\nperson_sorts: true\n",
		"destinations.yml": "destinations:\n  - name: watch-later\n    side: personal\n    tags: []\n" +
			"  - name: work-inbox\n    side: fleet\n    tags: [talks]\n",
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

// newVideoPipeline is a gated pipeline under videoSet, whose lander queues each
// video's parse as test-captions.
func newVideoPipeline(t *testing.T) *pipeline {
	t.Helper()
	captions, err := extract.NewRegistry(extract.Extractor{
		Name: "test-captions", Version: "1", MediaType: videoMediaType,
		Extract: func(context.Context, io.Reader) ([]extract.Unit, error) {
			return nil, errors.New("a video holds no bytes")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p := newPipeline(t, captions, afterStage(quiet))
	p.apply(t, videoSet(t))
	return p
}

// video is an arrival of the video id, as a video source's poll reports it.
func video(id string, views int64) server.ReportArrival {
	uploaded := "2026-11-20T10:00:00Z"
	return server.ReportArrival{
		Key: "youtube/" + id, URL: "https://www.youtube.com/watch?v=" + id, MediaType: videoMediaType, Title: "A video",
		ExternalKey: "item-" + id, Fields: map[string]string{"channel_id": "channel-one"},
		Metrics: map[string]int64{"duration_s": 300, "views": views, "likes": 40, "comments": 3}, SourceTS: &uploaded,
	}
}

// videoPoller stands in for a video source's poll handler. It answers each
// connector's poll with what finds names for it and the cursor next names,
// fails a connector's next poll where fail names a reason, and keeps each
// task it was handed.
type videoPoller struct {
	finds  map[string][]server.ReportArrival
	next   map[string]string
	fail   map[string]string
	handed []worker.Task
}

func (*videoPoller) Capability() string { return "poll:" + videoSource }

func (v *videoPoller) Run(_ context.Context, task worker.Task) worker.Result {
	v.handed = append(v.handed, task)
	name := *task.Connector
	if reason, ok := v.fail[name]; ok {
		delete(v.fail, name)
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	var cursor *string
	if c, ok := v.next[name]; ok {
		cursor = &c
	}
	return worker.Result{Outcome: server.OutcomeDone, Arrivals: v.finds[name], Cursor: cursor}
}

// videoDeleter stands in for a video source's deleter. Its first delete of
// each item that refuse names fails, and it keeps each sighting it was handed.
type videoDeleter struct {
	refuse map[string]bool
	calls  []deletion.Sighting
}

func (d *videoDeleter) Delete(_ context.Context, s deletion.Sighting) deletion.Outcome {
	d.calls = append(d.calls, s)
	if d.refuse[s.ItemKey] {
		delete(d.refuse, s.ItemKey)
		reason := "the source answered 503"
		return deletion.Outcome{Error: &reason}
	}
	return deletion.Outcome{}
}

// captionsStandIn parses each video as one unit of text, reading no bytes, in
// place of a captions extractor.
type captionsStandIn struct{}

func (captionsStandIn) Capability() string { return "parse:test-captions" }

func (captionsStandIn) Run(_ context.Context, task worker.Task) worker.Result {
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: "test-captions", ProducerVersion: "1", State: store.OutputDone,
		Units: []store.Unit{{N: 1, Text: "A talk on " + *task.ItemKey}},
	}}}
}

// runConnector makes connector's poll due now through the CLI.
func (p *pipeline) runConnector(t *testing.T, connector string) {
	t.Helper()
	if out, err := runCLI(t, "connectors", "run", connector, "--config", p.config); err != nil {
		t.Fatalf("connectors run %s printed %q, %v", connector, out, err)
	}
}

// item is the item under key as `items show --json` prints it, held to its
// schema.
func (p *pipeline) item(t *testing.T, key string) server.ItemAnswer {
	t.Helper()
	out, err := runCLI(t, "items", "show", key, "--json", "--config", p.config)
	var shown server.ItemAnswer
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil {
		t.Fatalf("items show %s printed %q, %v", key, out, err)
	}
	holdsTo(t, []byte(out), "item.schema.json")
	return shown
}

// A capture poll lands each video once as youtube/<id> under its connector's
// declared tags and zone, with the poll's metrics on its sighting, and moves
// the connector's cursor, which the next poll is handed with the connector's
// declaration. Each landed video queues one delete from the source. A poll
// that finds the same videos again lands nothing new and queues no second
// delete. A delete that fails tries again, and the source is asked once more.
func TestAPollLandsEachVideoOnceUnderItsConnectorAndDeletesItFromTheSource(t *testing.T) {
	ctx := t.Context()
	p := newVideoPipeline(t)
	poller := &videoPoller{
		finds: map[string][]server.ReportArrival{"video-capture": {video("testvideo01", 1200), video("testvideo02", 80)}},
		next:  map[string]string{"video-capture": "page-2"},
	}
	p.runConnector(t, "video-capture")
	p.run(t, poller)

	shown := p.item(t, "youtube/testvideo01")
	if len(shown.Sightings) != 1 {
		t.Fatalf("the video holds %d sightings, want the capture's", len(shown.Sightings))
	}
	s := shown.Sightings[0]
	if s.Connector != "video-capture" || !slices.Equal(s.Tags, []string{"video"}) || s.Zone != store.Outside ||
		s.ExternalKey != "item-testvideo01" || s.Metrics["views"] != 1200 || s.Metrics["duration_s"] != 300 {
		t.Errorf("the video's sighting is %s, want the capture's tags and zone, the source's key and the poll's metrics", jsonOf(s))
	}
	if shown.Item.SourceTS == nil || !strings.HasPrefix(*shown.Item.SourceTS, "2026-11-20T10:00:00") {
		t.Errorf("the video's source time is %v, want its upload", shown.Item.SourceTS)
	}
	if cur, _, err := p.st.Cursor(ctx, "video-capture"); err != nil || cur.Position == nil || *cur.Position != "page-2" || cur.LastDeliveryTS == nil {
		t.Errorf("the capture's cursor is %s, %v, want it moved to page-2 with a delivery", jsonOf(cur), err)
	}

	p.runConnector(t, "video-capture")
	p.run(t, poller)
	last := poller.handed[len(poller.handed)-1]
	var declared connectors.Connector
	if last.Cursor == nil || *last.Cursor != "page-2" || json.Unmarshal(last.Declaration, &declared) != nil ||
		declared.Name != "video-capture" || declared.AfterLanding != connectors.AfterLandingDelete {
		t.Errorf("the second poll was handed cursor %v and declaration %s, want page-2 and the capture's", last.Cursor, last.Declaration)
	}
	if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, videoMediaType); n != 2 {
		t.Errorf("two polls of two videos left %d items, want 2", n)
	}
	deletes := func() []server.TaskView {
		var out []server.TaskView
		for _, task := range p.tasks(t) {
			if task.Stage == store.StageDelete {
				out = append(out, task)
			}
		}
		return out
	}
	if d := deletes(); len(d) != 2 || d[0].Capability != deletion.Capability(videoSource) {
		t.Fatalf("two polls of two videos queued deletes %s, want one per video", jsonOf(d))
	}

	deleter := &videoDeleter{refuse: map[string]bool{"youtube/testvideo01": true}}
	p.run(t, deletion.Handler(videoSource, deleter))
	if d := deletes(); len(d) != 1 || *d[0].ItemKey != "youtube/testvideo01" || d[0].State != store.TaskQueued || !d[0].HasError {
		t.Fatalf("after the source refused one delete the open deletes are %s, want it queued again with its error", jsonOf(d))
	}
	p.advance(time.Minute)
	p.run(t, deletion.Handler(videoSource, deleter))
	if d := deletes(); len(d) != 0 {
		t.Errorf("after the retry the open deletes are %s, want none", jsonOf(d))
	}
	if len(deleter.calls) != 3 || deleter.calls[0].Connector != "video-capture" || !strings.HasPrefix(deleter.calls[0].ExternalKey, "item-") {
		t.Errorf("the source was asked %s, want each video's capture sighting, and the refused one twice", jsonOf(deleter.calls))
	}
}

// A connector scheduled once polls at once on its first apply. A poll that
// fails is kept on its cursor and runs again after the backoff, and the first
// poll that ends done finishes the connector, which the scheduler never polls
// again. connectors list reads when it finished.
func TestAOnceConnectorPollsUntilAPollEndsDone(t *testing.T) {
	ctx := t.Context()
	p := newVideoPipeline(t)
	scheduler := connectors.NewScheduler(p.st, p.lander, quiet)
	poller := &videoPoller{
		finds: map[string][]server.ReportArrival{"video-backfill": {video("testvideo03", 5)}},
		fail:  map[string]string{"video-backfill": "the source did not answer"},
	}
	pass := func() {
		t.Helper()
		if _, err := scheduler.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		p.run(t, poller)
	}
	backfills := func() int {
		return p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND connector = ?`, store.StagePoll, "video-backfill")
	}

	pass()
	if cur, _, _ := p.st.Cursor(ctx, "video-backfill"); backfills() != 1 || cur.Failures != 1 || cur.FinishedTS != nil {
		t.Fatalf("after a failed poll the backfill made %d polls and its cursor is %s, want 1 poll and its failure kept", backfills(), jsonOf(cur))
	}
	pass()
	if backfills() != 1 {
		t.Errorf("the backfill polled again before its backoff, %d polls", backfills())
	}
	p.advance(6 * time.Minute)
	pass()
	if cur, _, _ := p.st.Cursor(ctx, "video-backfill"); backfills() != 2 || cur.Failures != 0 || cur.FinishedTS == nil {
		t.Fatalf("after its backoff the backfill made %d polls and its cursor is %s, want a second poll that finished it", backfills(), jsonOf(cur))
	}
	p.advance(3 * time.Hour)
	pass()
	if backfills() != 2 {
		t.Errorf("a finished backfill polled again, %d polls", backfills())
	}
	out, err := runCLI(t, "connectors", "list", "--config", p.config)
	if line := lineWith(out, "video-backfill"); err != nil || !strings.Contains(line, "once · finished") || strings.Contains(line, "overdue") {
		t.Errorf("connectors list printed %q, %v, want the backfill finished", line, err)
	}
}

// A video from a connector that leaves the side to a person waits in review as
// person-sorts once the quality gate passes it, with the gate's side guess
// beside it. A second sighting by a channel whose tag places on the fleet side
// runs the gates again in the write that lands it, which places the waiting
// video by that tag and leaves a video that a person placed where it is.
func TestAPersonSortsVideoWaitsWithItsGuessAndASecondSightingGatesItAgain(t *testing.T) {
	p := newVideoPipeline(t)
	if out, err := runCLI(t, "connectors", "release", "video-capture", "--config", p.config); err != nil {
		t.Fatalf("connectors release printed %q, %v", out, err)
	}
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(nil)
	poller := &videoPoller{finds: map[string][]server.ReportArrival{
		"video-capture": {video("testvideo01", 1200), video("testvideo02", 80)},
		"video-channel": {video("testvideo01", 1300), video("testvideo02", 90)},
	}}
	p.runConnector(t, "video-capture")
	p.run(t, poller)
	p.run(t, captionsStandIn{})
	p.scanFindingNothing(t)
	p.run(t, p.qualityGate(t, stand.models(t, "", "")))

	out, err := runCLI(t, "review", "list", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "review-list.schema.json")
	var queue server.ReviewAnswer
	_ = json.Unmarshal([]byte(out), &queue)
	if len(queue.Items) != 2 {
		t.Fatalf("the review queue holds %d videos, want both", len(queue.Items))
	}
	for _, it := range queue.Items {
		if it.Reason != route.CausePersonSorts || it.Guess == nil || it.Guess.Side != connectors.SidePersonal || it.Sightings[0].Metrics["views"] == 0 {
			t.Errorf("%s waits as %s with guess %v, want person-sorts beside a personal guess and its metrics", it.Key, it.Reason, it.Guess)
		}
	}
	if out, err := runCLI(t, "review", "place", "youtube/testvideo02", "watch-later", "--config", p.config); err != nil {
		t.Fatalf("review place printed %q, %v", out, err)
	}

	p.runConnector(t, "video-channel")
	p.run(t, poller)
	if _, side, _ := p.decisions(t, "youtube/testvideo01"); side.Verdict != connectors.SideFleet || side.DecidedBy != route.ByTag ||
		side.Destination == nil || *side.Destination != "work-inbox" {
		t.Errorf("after the channel's sighting the waiting video is %s by %s, want fleet on work-inbox by its tag", side.Verdict, side.DecidedBy)
	}
	if _, side, _ := p.decisions(t, "youtube/testvideo02"); side.DecidedBy != route.ByPerson("person") || *side.Destination != "watch-later" {
		t.Errorf("after the channel's sighting the placed video is on %v by %s, want it left on watch-later by the person", side.Destination, side.DecidedBy)
	}
}

// lineWith is the line of out that holds s, and "" where none does.
func lineWith(out, s string) string {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, s) {
			return line
		}
	}
	return ""
}
