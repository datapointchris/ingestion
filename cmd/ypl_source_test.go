package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/metricsrefresh"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
	"github.com/datapointchris/ingestion/ypl"
)

// yplToken is the bearer token that the stand-in ypl takes, which the
// worker's [source.ypl] table hands its client.
const yplToken = "ypl-test-token"

// standInVideo is one video that the stand-in ypl holds, with every id and
// title made up.
type standInVideo struct {
	id, title string
	// broadcast is none, live or upcoming.
	broadcast string
	// withheld leaves the video out of a details read, as YouTube leaves out a
	// deleted or private one.
	withheld bool
	// published is when the video was published, in RFC 3339.
	published string
	// views is its view count, 1200 unless set otherwise.
	views int
	// likesHidden answers its like count as 0, as ypl answers a count that
	// its owner hides.
	likesHidden bool
}

// standInItem is one playlist item or upload.
type standInItem struct {
	itemID, videoID string
	unavailable     bool
}

// standInYPL answers ypl's Data API routes with the shapes its handlers
// write: a playlist's live items, a channel's uploads a page of 50 at a time,
// the details of videos, and a playlist item's delete. It answers a video's
// captions and audio from captions and audio. It refuses a request
// without yplToken, and a read of more video details than ypl reads at once.
// detailReads keeps the ids of each read of video details, and busyDetails
// answers that many of the next as ypl does while its Data API calls stay
// busy.
// refuseDelete answers an item's first delete with a 503 and a Retry-After,
// as a spent day's quota does. refuseDeleteForGood answers every delete of
// an item as ypl answers YouTube's refusal of the write. emptyReads answers
// that many reads of uploads with an empty page. busy answers that many of a
// video's next caption or audio reads as ypl does while its yt-dlp stays busy,
// gatewayDown that many after them as a gateway before a restarting ypl does,
// and failing that many after those as ypl answers a failed read of YouTube.
// ypl refuses each read of an unreadable video, as it refuses a members-only
// one, and every video read where it holds no yt-dlp. cutAudio sends that
// many of a video's audio answers with half the bytes that it promised.
// comments holds each video's top comments, and commentReads the id of each
// read of them. An insert into a playlist adds the video to its end, as
// YouTube does however often the playlist holds it, and inserts keeps each
// insert's playlist and video. refuseInsert answers a video's first insert
// with a 503 and a Retry-After, as a spent day's quota does. loseInsert adds
// a video's first insert, then answers 502 with no body, as a gateway that
// lost ypl's answer does.
type standInYPL struct {
	mu                  sync.Mutex
	playlistIDs         map[string]string
	playlists           map[string][]standInItem
	uploads             map[string][]standInItem
	videos              map[string]standInVideo
	refuseDelete        map[string]time.Duration
	refuseDeleteForGood map[string]bool
	emptyReads          int
	deletes             []string
	captions            map[string]map[string]any
	audio               map[string][]byte
	busy                map[string]int
	gatewayDown         map[string]int
	failing             map[string]int
	unreadable          map[string]bool
	noYTDLP             bool
	cutAudio            map[string]int
	detailReads         [][]string
	busyDetails         int
	comments            map[string][]ypl.Comment
	commentReads        []string
	inserts             []string
	refuseInsert        map[string]time.Duration
	loseInsert          map[string]bool
}

func newStandInYPL(t *testing.T) (*standInYPL, *httptest.Server) {
	t.Helper()
	s := &standInYPL{
		playlistIDs: map[string]string{}, playlists: map[string][]standInItem{}, uploads: map[string][]standInItem{},
		videos: map[string]standInVideo{}, refuseDelete: map[string]time.Duration{}, refuseDeleteForGood: map[string]bool{},
		captions: map[string]map[string]any{}, audio: map[string][]byte{}, busy: map[string]int{}, gatewayDown: map[string]int{}, failing: map[string]int{},
		unreadable: map[string]bool{}, cutAudio: map[string]int{}, comments: map[string][]ypl.Comment{}, refuseInsert: map[string]time.Duration{},
		loseInsert: map[string]bool{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/youtube/playlists/{id}/items", s.playlistItems)
	mux.HandleFunc("DELETE /api/v1/youtube/playlists/{id}/items/{item}", s.deleteItem)
	mux.HandleFunc("POST /api/v1/youtube/playlists/{id}/items", s.insertItem)
	mux.HandleFunc("GET /api/v1/youtube/channels/{id}/uploads", s.channelUploads)
	mux.HandleFunc("GET /api/v1/youtube/videos", s.videoDetails)
	mux.HandleFunc("GET /api/v1/youtube/videos/{id}/captions", s.videoCaptions)
	mux.HandleFunc("GET /api/v1/youtube/videos/{id}/audio", s.videoAudio)
	mux.HandleFunc("GET /api/v1/youtube/videos/{id}/comments", s.videoComments)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+yplToken {
			refuse(w, http.StatusUnauthorized, "unauthorized", "the request carries no token this server takes", 0)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	return s, api
}

func refuse(w http.ResponseWriter, status int, code, message string, retryAfter time.Duration) {
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": http.StatusText(status), "message": message, "code": code})
}

func answer(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

// shown is an item as ypl's liveItem writes it.
func (s *standInYPL) shown(items []standInItem) []map[string]any {
	out := []map[string]any{}
	for i, item := range items {
		out = append(out, map[string]any{
			"item_id": item.itemID, "video_id": item.videoID, "position": i, "title": s.videos[item.videoID].title,
			"channel_title": "Test channel", "unavailable": item.unavailable,
		})
	}
	return out
}

func (s *standInYPL) playlistItems(w http.ResponseWriter, r *http.Request) {
	id, ok := s.playlistIDs[r.PathValue("id")]
	if !ok {
		refuse(w, http.StatusNotFound, "not_found", "playlist not found", 0)
		return
	}
	answer(w, map[string]any{"playlist_id": id, "items": s.shown(s.playlists[id])})
}

func (s *standInYPL) deleteItem(w http.ResponseWriter, r *http.Request) {
	playlist, item := r.PathValue("id"), r.PathValue("item")
	s.deletes = append(s.deletes, playlist+"/"+item)
	if wait, ok := s.refuseDelete[item]; ok {
		delete(s.refuseDelete, item)
		refuse(w, http.StatusServiceUnavailable, "youtube_quota_spent", "the day's quota cannot afford this call", wait)
		return
	}
	if s.refuseDeleteForGood[item] {
		refuse(w, http.StatusUnprocessableEntity, "youtube_refused", "YouTube refused the call", 0)
		return
	}
	s.playlists[playlist] = slices.DeleteFunc(s.playlists[playlist], func(i standInItem) bool { return i.itemID == item })
	w.WriteHeader(http.StatusNoContent)
}

// insertItem answers ypl's playlist-item insert: a body of video_id alone,
// answered 201 with the new item's id and the playlist's YouTube id.
func (s *standInYPL) insertItem(w http.ResponseWriter, r *http.Request) {
	id, ok := s.playlistIDs[r.PathValue("id")]
	if !ok {
		refuse(w, http.StatusNotFound, "not_found", "playlist not found", 0)
		return
	}
	var body struct {
		VideoID string `json:"video_id"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || body.VideoID == "" {
		refuse(w, http.StatusUnprocessableEntity, "invalid_body", "the body names video_id alone", 0)
		return
	}
	if wait, ok := s.refuseInsert[body.VideoID]; ok {
		delete(s.refuseInsert, body.VideoID)
		refuse(w, http.StatusServiceUnavailable, "youtube_quota_spent", "the day's quota cannot afford this call", wait)
		return
	}
	item := fmt.Sprintf("item-%d", len(s.inserts)+1)
	s.inserts = append(s.inserts, id+"/"+body.VideoID)
	s.playlists[id] = append(s.playlists[id], standInItem{itemID: item, videoID: body.VideoID})
	if s.loseInsert[body.VideoID] {
		delete(s.loseInsert, body.VideoID)
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"item_id": item, "playlist_id": id, "video_id": body.VideoID})
}

func (s *standInYPL) channelUploads(w http.ResponseWriter, r *http.Request) {
	uploads := s.uploads[r.PathValue("id")]
	if s.emptyReads > 0 {
		s.emptyReads--
		uploads = nil
	}
	start, _ := strconv.Atoi(r.URL.Query().Get("page_token"))
	end := min(start+50, len(uploads))
	page := map[string]any{"data": s.shown(uploads[start:end]), "next_page_token": nil}
	if end < len(uploads) {
		page["next_page_token"] = strconv.Itoa(end)
	}
	answer(w, page)
}

func (s *standInYPL) videoDetails(w http.ResponseWriter, r *http.Request) {
	ids := strings.Split(r.URL.Query().Get("ids"), ",")
	if len(ids) > ypl.MaxVideos {
		refuse(w, http.StatusUnprocessableEntity, "too_many_videos", "ids names too many videos", 0)
		return
	}
	s.detailReads = append(s.detailReads, ids)
	if s.busyDetails > 0 {
		s.busyDetails--
		refuse(w, http.StatusServiceUnavailable, "youtube_data_api_busy", "the server's Data API calls stayed busy; send the request again", 90*time.Second)
		return
	}
	out := []map[string]any{}
	for _, id := range ids {
		v, ok := s.videos[id]
		if !ok || v.withheld {
			continue
		}
		likes := 40
		if v.likesHidden {
			likes = 0
		}
		out = append(out, map[string]any{
			"id": v.id, "title": v.title, "description": "A description.", "channel_id": "UCtestchannel", "channel_title": "Test channel",
			"published_at": v.published, "privacy": "public", "duration_seconds": 754,
			"live_broadcast_content": v.broadcast, "view_count": v.views, "like_count": likes, "comment_count": 3,
		})
	}
	answer(w, out)
}

// readable answers a read of a busy, unreachable, failing or unreadable video
// as ypl, or a gateway before it, does, and says whether the read goes on.
func (s *standInYPL) readable(w http.ResponseWriter, id string) bool {
	if s.noYTDLP {
		refuse(w, http.StatusServiceUnavailable, "youtube_reads_unavailable", "this server reads no video through yt-dlp: it has no yt-dlp", 0)
		return false
	}
	if s.busy[id] > 0 {
		s.busy[id]--
		refuse(w, http.StatusServiceUnavailable, "youtube_reads_busy", "the server's yt-dlp stayed busy with other reads; send the request again", 90*time.Second)
		return false
	}
	if s.gatewayDown[id] > 0 {
		s.gatewayDown[id]--
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html><body>502 Bad Gateway</body></html>"))
		return false
	}
	if s.failing[id] > 0 {
		s.failing[id]--
		refuse(w, http.StatusBadGateway, "youtube_read_failed", "the read of YouTube failed", 0)
		return false
	}
	if s.unreadable[id] {
		refuse(w, http.StatusUnprocessableEntity, "video_unavailable", "YouTube lets no signed-out read reach this video", 0)
		return false
	}
	return true
}

func (s *standInYPL) videoCaptions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.readable(w, id) {
		return
	}
	captions, ok := s.captions[id]
	if !ok {
		refuse(w, http.StatusNotFound, "not_found", "video not found", 0)
		return
	}
	answer(w, captions)
}

func (s *standInYPL) videoAudio(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.readable(w, id) {
		return
	}
	audio, ok := s.audio[id]
	if !ok {
		refuse(w, http.StatusNotFound, "not_found", "video not found", 0)
		return
	}
	w.Header().Set("Content-Type", "audio/wav")
	w.Header().Set("Content-Length", strconv.Itoa(len(audio)))
	if s.cutAudio[id] > 0 {
		s.cutAudio[id]--
		audio = audio[:len(audio)/2]
	}
	_, _ = w.Write(audio)
}

// videoComments answers a video's top comments through the client's own
// VideoComments, which a test holds to ypl's comments document, with its
// comments off where it holds none.
func (s *standInYPL) videoComments(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.videos[id]; !ok {
		refuse(w, http.StatusNotFound, "not_found", "video not found", 0)
		return
	}
	s.commentReads = append(s.commentReads, id)
	comments, on := s.comments[id]
	if !on {
		comments = []ypl.Comment{}
	}
	answer(w, ypl.VideoComments{VideoID: id, CommentsDisabled: !on, Comments: comments})
}

// add holds each video, available, finished, published on a day in the past
// and viewed 1200 times unless set otherwise.
func (s *standInYPL) add(videos ...standInVideo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range videos {
		if v.broadcast == "" {
			v.broadcast = "none"
		}
		if v.published == "" {
			v.published = "2026-11-20T10:00:00Z"
		}
		if v.views == 0 {
			v.views = 1200
		}
		s.videos[v.id] = v
	}
}

// yplWorker answers the poll handler and the deleter's that the ypl source
// opens, which the worker's handlers must hold.
func yplWorker(t *testing.T, p *pipeline, api string) (poll, del worker.Handler) {
	t.Helper()
	handlers := yplHandlers(t, p, api)
	poll, del = handlers[store.PollCapability(ypl.Source)], handlers[store.DeleteCapability(ypl.Source)]
	if poll == nil || del == nil {
		t.Fatalf("a worker with a [source.ypl] table holds poll handler %v and delete handler %v, want both", poll, del)
	}
	return poll, del
}

// yplHandlers opens the ypl source as worker run does, from a [source.ypl]
// table naming api and a token command. It answers each handler that the
// worker then holds, by capability.
func yplHandlers(t *testing.T, p *pipeline, api string) map[string]worker.Handler {
	t.Helper()
	return yplModelHandlers(t, p, api, modelConfig{})
}

// yplModelHandlers is yplHandlers beside each model stage's handler that
// models opens.
func yplModelHandlers(t *testing.T, p *pipeline, api string, models modelConfig) map[string]worker.Handler {
	t.Helper()
	dir := t.TempDir()
	token, config := filepath.Join(dir, "ypl-token.json"), filepath.Join(dir, "worker.toml")
	for path, body := range map[string]string{
		token:  `{"access_token":"` + yplToken + `","token_type":"Bearer","expires_in":3600}`,
		config: fmt.Sprintf("[source.ypl]\napi = %q\ntoken_command = [\"cat\", %q]\n", api, token),
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := readConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	handlers := map[string]worker.Handler{}
	port := openModelPort(t.Context(), models, p.provider.URL, quiet)
	for _, h := range workerHandlers(p.worker, nil, openSources(t.Context(), cfg, p.provider.URL, p.worker, quiet), port, nil, nil, models, quiet) {
		handlers[h.Capability()] = h
	}
	return handlers
}

// recording keeps what each run of its handler reported.
type recording struct {
	worker.Handler
	results []worker.Result
}

func (r *recording) Run(ctx context.Context, task worker.Task) worker.Result {
	result := r.Handler.Run(ctx, task)
	r.results = append(r.results, result)
	return result
}

// landedKeys are the keys of what the last poll reported, in order.
func (r *recording) landedKeys(t *testing.T) []string {
	t.Helper()
	if len(r.results) == 0 {
		t.Fatal("no poll ran")
	}
	last := r.results[len(r.results)-1]
	if last.Outcome != server.OutcomeDone {
		t.Fatalf("the poll ended %s: %v", last.Outcome, last.Error)
	}
	keys := []string{}
	for _, a := range last.Arrivals {
		keys = append(keys, a.Key)
	}
	return keys
}

// newYPLPipeline is a pipeline under a ypl connector for each name in
// declared, whose declaration body follows its name, mode, source and zone.
func newYPLPipeline(t *testing.T, declared map[string]string) *pipeline {
	t.Helper()
	p := newPipeline(t, extract.Builtin(), afterStage(quiet))
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "connectors"), 0o755); err != nil {
		t.Fatal(err)
	}
	for connector, body := range declared {
		declaration := "name: " + connector + "\nmode: poll\nsource: ypl\nzone: outside\nperson_sorts: true\n" + body
		if err := os.WriteFile(filepath.Join(dir, "connectors", connector+".yml"), []byte(declaration), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p.apply(t, dir)
	return p
}

// yplCapture and yplChannel are the bodies of a capture playlist's connector,
// which deletes each save it lands, and of a watched channel's.
const (
	yplCapture = "schedule: \"@every 30m\"\nafter_landing: delete\nsource_parameters:\n  playlist: capture\n"
	yplChannel = "schedule: \"@hourly\"\nsource_parameters:\n  channel: \"@testchannel\"\n"
)

// A capture playlist's poll lands each available video once as
// youtube/<id>, with its details as fields and metrics, and passes over one
// that ypl marks unavailable. A premiere waits in the playlist until it airs.
// Each landed video's delete calls ypl's playlist-item delete once. A delete
// that ypl refuses for the day's quota waits the Retry-After it sent. A
// delete that YouTube refuses for good fails, and a later poll that reports
// its video again asks for no other. When the playlist's title comes to
// name another playlist, the poll fails and lands nothing.
func TestAYPLCapturePollLandsEachAvailableVideoOnceAndDeletesItFromThePlaylist(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.add(standInVideo{id: "testvid0001", title: "Test video 1"}, standInVideo{id: "testvid0002", title: "Test video 2"},
		standInVideo{id: "testvid0003", title: "Test video 3"}, standInVideo{id: "testvid0004", title: "Test video 4", broadcast: "upcoming"},
		standInVideo{id: "testvid0005", title: "Test video 5"})
	stand.playlistIDs["capture"], stand.playlistIDs["PLtestcapture"] = "PLtestcapture", "PLtestcapture"
	stand.playlists["PLtestcapture"] = []standInItem{
		{itemID: "item0001", videoID: "testvid0001"},
		{itemID: "item0002", videoID: "testvid0002", unavailable: true},
		{itemID: "item0003", videoID: "testvid0003"},
		{itemID: "item0004", videoID: "testvid0004"},
		{itemID: "item0005", videoID: "testvid0005"},
	}
	stand.refuseDelete["item0003"] = time.Hour
	stand.refuseDeleteForGood["item0005"] = true
	p := newYPLPipeline(t, map[string]string{"ypl-capture": yplCapture})
	pollHandler, deleteHandler := yplWorker(t, p, api.URL)
	poll := &recording{Handler: pollHandler}
	pollNow := func() []string {
		t.Helper()
		p.runConnector(t, "ypl-capture")
		p.run(t, poll)
		return poll.landedKeys(t)
	}

	if got := pollNow(); !slices.Equal(got, []string{"youtube/testvid0001", "youtube/testvid0003", "youtube/testvid0005"}) {
		t.Fatalf("the first poll reported %v, want the three available videos that have aired, in the playlist's order", got)
	}
	shown := p.item(t, "youtube/testvid0001")
	s := shown.Sightings[0]
	if shown.Item.MediaType != ypl.MediaTypeVideo || s.ExternalKey != "item0001" || s.Fields[ypl.FieldPlaylistID] != "PLtestcapture" ||
		s.Fields[ypl.FieldChannelID] != "UCtestchannel" || s.Metrics["duration_s"] != 754 || s.Metrics["views"] != 1200 ||
		shown.Item.SourceTS == nil || !strings.HasPrefix(*shown.Item.SourceTS, "2026-11-20T10:00:00") {
		t.Errorf("the landed video is %s, want its playlist item, playlist, channel, metrics and upload time", jsonOf(shown))
	}

	p.run(t, deleteHandler)
	if !slices.Equal(stand.deletes, []string{"PLtestcapture/item0001", "PLtestcapture/item0003", "PLtestcapture/item0005"}) {
		t.Fatalf("ypl was asked to delete %v, want each landed video's item once", stand.deletes)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND state = ?`, store.StageDelete, store.TaskFailed); n != 1 {
		t.Errorf("%d deletes failed, want the one that YouTube refused for good", n)
	}
	p.advance(10 * time.Minute)
	p.run(t, deleteHandler)
	if len(stand.deletes) != 3 {
		t.Errorf("a delete refused with an hour's Retry-After ran again after ten minutes: %v", stand.deletes)
	}
	p.advance(time.Hour)
	p.run(t, deleteHandler)
	if len(stand.deletes) != 4 || stand.deletes[3] != "PLtestcapture/item0003" {
		t.Errorf("after the Retry-After ypl was asked to delete %v, want the refused item once more", stand.deletes)
	}

	if got := pollNow(); !slices.Equal(got, []string{"youtube/testvid0005"}) {
		t.Errorf("a poll of a playlist holding an unavailable video, a premiere and a refused delete reported %v, want the last", got)
	}
	stand.add(standInVideo{id: "testvid0004", title: "Test video 4"})
	if got := pollNow(); !slices.Equal(got, []string{"youtube/testvid0004", "youtube/testvid0005"}) {
		t.Errorf("the poll after the premiere aired reported %v, want it beside the refused delete's video", got)
	}
	p.run(t, deleteHandler)
	if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, ypl.MediaTypeVideo); n != 4 ||
		len(stand.deletes) != 5 || stand.deletes[4] != "PLtestcapture/item0004" {
		t.Errorf("three polls left %d videos and asked ypl to delete %v, want 4 videos, each deleted once", n, stand.deletes)
	}

	stand.mu.Lock()
	stand.playlistIDs["capture"] = "PLtestother"
	stand.playlists["PLtestother"] = []standInItem{{itemID: "itemother1", videoID: "testvid0001"}}
	stand.mu.Unlock()
	p.runConnector(t, "ypl-capture")
	p.run(t, poll, deleteHandler)
	if last := poll.results[len(poll.results)-1]; last.Outcome != server.OutcomeFailed || len(stand.deletes) != 5 {
		t.Errorf("a poll of a title that now names another playlist ended %s and asked ypl to delete %v, want it failed and no delete",
			last.Outcome, stand.deletes)
	}
}

// A channel's first poll keeps its newest 50 uploads as a baseline and lands
// nothing. The next lands each upload that the channel added, oldest first,
// and holds back one on air. A poll with nothing new lands nothing, and the
// one after the stream ends lands it. A read that answers no uploads keeps
// the window. A poll that finds every upload of the window gone lands none of
// the back catalog behind it.
func TestAYPLChannelPollLandsEachNewUploadOnceAfterItsBaseline(t *testing.T) {
	stand, api := newStandInYPL(t)
	var catalog []standInItem
	for i := 55; i >= 1; i-- {
		id := fmt.Sprintf("oldvid%05d", i)
		stand.add(standInVideo{id: id, title: "Old video " + strconv.Itoa(i)})
		catalog = append(catalog, standInItem{itemID: "up" + id, videoID: id})
	}
	stand.uploads["@testchannel"] = catalog
	p := newYPLPipeline(t, map[string]string{"ypl-channel": yplChannel})
	pollHandler, _ := yplWorker(t, p, api.URL)
	poll := &recording{Handler: pollHandler}
	pollNow := func() []string {
		t.Helper()
		p.runConnector(t, "ypl-channel")
		p.run(t, poll)
		return poll.landedKeys(t)
	}

	if got := pollNow(); len(got) != 0 {
		t.Fatalf("the baseline poll reported %v, want nothing", got)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	stand.add(standInVideo{id: "newvid00001", title: "New video 1", published: now}, standInVideo{id: "newvid00002", title: "New video 2", published: now},
		standInVideo{id: "newvid00003", title: "New video 3", broadcast: "live", published: now})
	stand.mu.Lock()
	stand.uploads["@testchannel"] = append([]standInItem{
		{itemID: "upnew3", videoID: "newvid00003"}, {itemID: "upnew2", videoID: "newvid00002"}, {itemID: "upnew1", videoID: "newvid00001"},
	}, catalog...)
	stand.mu.Unlock()
	if got := pollNow(); !slices.Equal(got, []string{"youtube/newvid00001", "youtube/newvid00002"}) {
		t.Fatalf("the second poll reported %v, want the two finished new uploads, oldest first", got)
	}
	if got := pollNow(); len(got) != 0 {
		t.Errorf("a third poll with nothing new reported %v, want nothing", got)
	}
	stand.add(standInVideo{id: "newvid00003", title: "New video 3", published: now})
	if got := pollNow(); !slices.Equal(got, []string{"youtube/newvid00003"}) {
		t.Errorf("the poll after the stream ended reported %v, want it", got)
	}

	stand.mu.Lock()
	stand.emptyReads = 1
	stand.mu.Unlock()
	if got := pollNow(); len(got) != 0 {
		t.Errorf("a poll that read no uploads reported %v, want nothing", got)
	}
	if got := pollNow(); len(got) != 0 {
		t.Errorf("the poll after a read of no uploads reported %v, want nothing", got)
	}
	stand.mu.Lock()
	stand.uploads["@testchannel"] = catalog[len(catalog)-5:]
	stand.mu.Unlock()
	if got := pollNow(); len(got) != 0 {
		t.Errorf("a poll that found the window's uploads gone reported %v, want none of the back catalog", got)
	}
	if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, ypl.MediaTypeVideo); n != 3 {
		t.Errorf("the channel's polls left %d videos, want the 3 new uploads", n)
	}
}

// A video that is saved to the capture playlist and uploaded by a watched
// channel lands as one item with a sighting of each, parses from its
// captions, and its save is deleted from the playlist. Each day's refresh
// then reads the metrics of each video that waits in review, and writes them
// onto each of its sightings with when it read them, where items show prints
// them. A read of unchanged counts still stamps them read, and leaves when
// the sighting was last seen. A like count that ypl answers as 0, as it
// answers one that the owner hides, keeps the count measured before. A video
// dropped from review is not read, and a refresh queues no delete. A refresh
// that ypl answers busy waits its Retry-After and spends no attempt.
func TestASavedAndUploadedVideoLandsOnceAndItsMetricsRefreshDailyWhileItWaitsInReview(t *testing.T) {
	const both, saved = "youtube/bothvid0001", "youtube/savedvid001"
	stand, api := newStandInYPL(t)
	stand.add(standInVideo{id: "oldvid00001", title: "Old video"})
	stand.playlistIDs["capture"] = "PLtestcapture"
	stand.uploads["@testchannel"] = []standInItem{{itemID: "upold", videoID: "oldvid00001"}}
	p := newYPLPipeline(t, map[string]string{"ypl-capture": yplCapture, "ypl-channel": yplChannel})
	handlers := yplHandlers(t, p, api.URL)
	poll, refresh := handlers[store.PollCapability(ypl.Source)], handlers[store.RefreshMetricsCapability(ypl.Source)]
	if refresh == nil {
		t.Fatal("a worker with a [source.ypl] table holds no handler of ypl's refresh of metrics")
	}
	p.runConnector(t, "ypl-channel")
	p.run(t, poll)

	stand.add(standInVideo{id: "bothvid0001", title: "Test video saved and uploaded", published: time.Now().UTC().Format(time.RFC3339)},
		standInVideo{id: "savedvid001", title: "Test video saved"})
	stand.mu.Lock()
	stand.playlists["PLtestcapture"] = []standInItem{{itemID: "itemboth", videoID: "bothvid0001"}, {itemID: "itemsaved", videoID: "savedvid001"}}
	stand.uploads["@testchannel"] = append([]standInItem{{itemID: "upboth", videoID: "bothvid0001"}}, stand.uploads["@testchannel"]...)
	for _, id := range []string{"bothvid0001", "savedvid001"} {
		stand.captions[id] = captionsOf(id, "A test description.", "manual", 754, nil, []extract.Segment{{StartMs: 0, DurationMs: 2000, Text: "Hello."}})
	}
	stand.mu.Unlock()
	p.runConnector(t, "ypl-capture")
	p.runConnector(t, "ypl-channel")
	p.run(t, poll)
	p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
	p.run(t, handlers[store.DeleteCapability(ypl.Source)])

	var sighted []string
	for _, s := range p.item(t, both).Sightings {
		sighted = append(sighted, s.Connector)
	}
	slices.Sort(sighted)
	if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, ypl.MediaTypeVideo); n != 2 ||
		!slices.Equal(sighted, []string{"ypl-capture", "ypl-channel"}) {
		t.Fatalf("the polls left %d videos, the saved and uploaded one sighted by %v, want 2 videos and a sighting by each connector", n, sighted)
	}
	if parse := p.readParse(t, both, ""); parse.Extractor != ypl.CaptionsExtractor.Name || len(parse.Units) == 0 {
		t.Errorf("the saved and uploaded video parsed by %q to %d units, want its captions", parse.Extractor, len(parse.Units))
	}
	deleted := slices.Sorted(slices.Values(stand.deletes))
	if !slices.Equal(deleted, []string{"PLtestcapture/itemboth", "PLtestcapture/itemsaved"}) || len(stand.playlists["PLtestcapture"]) != 0 {
		t.Fatalf("ypl was asked to delete %v, want each save once and the playlist left empty", deleted)
	}

	for _, connector := range []string{"ypl-capture", "ypl-channel"} {
		if out, err := runCLI(t, "connectors", "release", connector, "--config", p.config); err != nil {
			t.Fatalf("connectors release %s printed %q, %v", connector, out, err)
		}
	}
	dispatch := newStandInDispatch(t, p.provider)
	dispatch.answer = answerBy(nil)
	p.scanFindingNothing(t)
	p.run(t, p.qualityGate(t, dispatch.models(t, "", "")))
	if !inReview(t, p, both, route.CausePersonSorts) || !inReview(t, p, saved, route.CausePersonSorts) {
		t.Fatal("the two videos do not both wait in review for a person to sort them")
	}
	if out, err := runCLI(t, "review", "drop", saved, "--config", p.config); err != nil {
		t.Fatalf("review drop printed %q, %v", out, err)
	}

	refreshes, err := metricsrefresh.NewScheduler(p.st, metricsrefresh.DefaultSchedule, quiet)
	if err != nil {
		t.Fatal(err)
	}
	deletes := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageDelete)
	reads := len(stand.detailReads)
	arrived := map[string]string{}
	for _, s := range p.item(t, both).Sightings {
		arrived[s.Connector] = s.LastSeenTS
	}
	days := []struct {
		views       int
		likesHidden bool
	}{{5000, false}, {7000, false}, {7000, true}}
	for day, d := range days {
		stand.add(standInVideo{id: "bothvid0001", title: "Test video saved and uploaded", views: d.views, likesHidden: d.likesHidden},
			standInVideo{id: "savedvid001", title: "Test video saved", views: d.views})
		stand.mu.Lock()
		stand.busyDetails = 1
		stand.mu.Unlock()
		p.advance(24 * time.Hour)
		for range 2 {
			if _, err := refreshes.Pass(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageRefreshMetrics); n != day+1 {
			t.Fatalf("after %d days the passes queued %d refreshes, want one a day", day+1, n)
		}
		p.run(t, refresh)
		if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND state = ? AND attempts = 0 AND waiting_since_ts IS NOT NULL`,
			store.StageRefreshMetrics, store.TaskQueued); n != 1 {
			t.Fatalf("day %d's refresh that ypl answered busy is not waiting with no attempt spent", day+1)
		}
		p.advance(2 * time.Minute)
		before := store.Stamp(p.st.Now())
		p.run(t, refresh)
		for _, s := range p.item(t, both).Sightings {
			if s.Metrics["views"] != int64(d.views) || s.Metrics["likes"] != 40 || s.Metrics["duration_s"] != 754 {
				t.Errorf("day %d's refresh left %s's sighting with metrics %v, want %d views and its measured 40 likes", day+1, s.Connector, s.Metrics, d.views)
			}
			read := "never"
			if s.MetricsReadTS != nil {
				read = *s.MetricsReadTS
			}
			if read == "never" || read < before || s.LastSeenTS != arrived[s.Connector] {
				t.Errorf("day %d's refresh left %s's sighting read at %s and last seen at %s, want read at or after %s and last seen at its arrival %s",
					day+1, s.Connector, read, s.LastSeenTS, before, arrived[s.Connector])
			}
		}
		if s := p.item(t, saved).Sightings[0]; s.Metrics["views"] != 1200 {
			t.Errorf("day %d's refresh wrote %d views onto the video dropped from review, want its landed 1200", day+1, s.Metrics["views"])
		}
	}
	for _, ids := range stand.detailReads[reads:] {
		if !slices.Equal(ids, []string{"bothvid0001"}) {
			t.Errorf("a refresh read the details of %v, want only the video in review", ids)
		}
	}
	if n := len(stand.detailReads) - reads; n != 2*len(days) {
		t.Errorf("%d days' refreshes read video details %d times, want a busy answer and a read each day", len(days), n)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageDelete); n != deletes {
		t.Errorf("the refreshes queued %d deletes, want none", n-deletes)
	}
	if out, err := runCLI(t, "items", "show", both, "--config", p.config); err != nil || strings.Count(out, "views 7000") != 2 {
		t.Errorf("items show printed %q, %v, want each sighting's metrics at 7000 views", out, err)
	}
}
