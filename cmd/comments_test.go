package cmd

import (
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermstyle"

	"github.com/datapointchris/ingestion/comments"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/views/review"
	"github.com/datapointchris/ingestion/ypl"
)

// c on a video's card queues one read of its top comments, and a second c queues none. A worker
// reads the comments through ypl and asks the model port once, on the smallest model, quoting
// each comment. The store keeps the answer as the comments stage's output, whose answer column
// ships only as outsider text, and the card shows it.
func TestCOnAVideosCardReadsWhatItsCommentsSayOnce(t *testing.T) {
	const key = "youtube/cmtvid00001"
	stand, api := newStandInYPL(t)
	stand.add(standInVideo{id: "cmtvid00001", title: "Test video with comments"})
	stand.mu.Lock()
	stand.playlistIDs["capture"] = "PLtestcapture"
	stand.playlists["PLtestcapture"] = []standInItem{{itemID: "itemcmt", videoID: "cmtvid00001"}}
	stand.captions["cmtvid00001"] = captionsOf("cmtvid00001", "A test description.", "manual", 754, nil, []extract.Segment{{StartMs: 0, DurationMs: 2000, Text: "Hello."}})
	stand.comments["cmtvid00001"] = []map[string]any{
		{"id": "c1", "author": "viewer one", "author_channel_id": nil, "text": "The second half explains the pump well.", "like_count": 12, "reply_count": 1, "published_at": "2026-11-21T08:00:00Z"},
		{"id": "c2", "author": "viewer two", "author_channel_id": "UCviewertwo", "text": "The audio drops out at ten minutes.", "like_count": 3, "reply_count": 0, "published_at": "2026-11-21T09:00:00Z"},
	}
	stand.mu.Unlock()
	p := newYPLPipeline(t, map[string]string{"ypl-capture": yplCapture})
	dispatch := newStandInDispatch(t, p.provider)
	dispatch.answer = answerBy(nil)
	dispatch.comments = func(string) map[string]any {
		return map[string]any{
			"summary": "Viewers praise the explanation and flag an audio fault.", "praise": []string{"clear pump explanation"},
			"criticism": []string{"audio drops at ten minutes"}, "tone": "mixed",
		}
	}
	handlers := yplModelHandlers(t, p, api.URL, dispatch.models(t, "", ""))
	read := handlers[comments.Capability]
	if read == nil {
		t.Fatal("a worker with a [source.ypl] table and a model port holds no comments handler")
	}
	p.runConnector(t, "ypl-capture")
	p.run(t, handlers[store.PollCapability(ypl.Source)])
	p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
	if out, err := runCLI(t, "connectors", "release", "ypl-capture", "--config", p.config); err != nil {
		t.Fatalf("connectors release printed %q, %v", out, err)
	}
	p.scanFindingNothing(t)
	p.run(t, handlers[qualitygate.Capability])
	if !inReview(t, p, key, route.CausePersonSorts) {
		t.Fatal("the video does not wait in review")
	}

	board := reviewTab{review.NewBoard(t.Context(), reviewService(tuiOptions{config: p.config}), review.Outside{})}
	m := gotermboard.NewModel(board, gotermstyle.PaletteFor(io.Discard))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 60})
	if err := board.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.Rebuild()
	runKey(t, m, letter('c'))
	runKey(t, m, letter('c'))
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageComments); n != 1 {
		t.Fatalf("two presses of c queued %d reads of the comments, want one", n)
	}

	p.run(t, read)
	p.run(t, read)
	var started []dispatchStart
	for _, s := range dispatch.starts() {
		if strings.HasSuffix(s.Schema, "/prompts/comments/answer.schema.json") {
			started = append(started, s)
		}
	}
	if len(started) != 1 || started[0].Model != comments.DefaultModel || stand.commentReads == nil || len(stand.commentReads) != 1 {
		t.Fatalf("the read started %+v after %d reads of ypl's comments, want one run on %s after one read", started, len(stand.commentReads), comments.DefaultModel)
	}
	if prompt := dispatch.prompt(started[0].Key); !strings.Contains(prompt, "The audio drops out at ten minutes.") || !strings.Contains(prompt, "Test video with comments") {
		t.Errorf("the prompt quotes neither the video's title nor its comments:\n%s", prompt)
	}
	var kept string
	if err := p.st.Reader().QueryRowContext(t.Context(), `SELECT answer FROM outputs WHERE item_key = ? AND stage = ? AND state = ?`,
		key, store.StageComments, store.OutputDone).Scan(&kept); err != nil {
		t.Fatalf("the store keeps no done comments output: %v", err)
	}
	if a, err := comments.DecodeAnswer(json.RawMessage(kept)); err != nil || a.Tone != "mixed" {
		t.Errorf("the comments output keeps %s (%v), want the model's answer", kept, err)
	}
	outputs := store.Tables[slices.IndexFunc(store.Tables, func(tab store.Table) bool { return tab.Name == "outputs" })]
	if !outputs.IsText("answer") {
		t.Error("an output's answer is not outsider text, so the comments' answer would ship as a record")
	}

	runKey(t, m, letter('c'))
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageComments); n != 1 {
		t.Errorf("c after the read landed queued %d reads, want the one that landed", n)
	}
	out, err := runCLI(t, "tui", "--view", "review", "--json", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	holdsTo(t, []byte(out), "tui-review.schema.json")
	var snap review.Snapshot
	if err := json.Unmarshal([]byte(out), &snap); err != nil || len(snap.Items) != 1 || snap.Items[0].Item.Comments == nil ||
		snap.Items[0].Item.Comments.State != server.CommentsDone {
		t.Fatalf("the snapshot holds %s, %v, want the video with its comments read", out, err)
	}
	t.Setenv("COLUMNS", "200")
	t.Setenv("LINES", "80")
	frame, err := runCLI(t, "tui", "--once", "--config", p.config)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"What the comments say", "Viewers praise the explanation", "audio drops at ten minutes", "mixed"} {
		if !strings.Contains(frame, want) {
			t.Errorf("the card lacks %q:\n%s", want, frame)
		}
	}
}
