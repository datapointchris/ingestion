package cmd

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
	"github.com/datapointchris/ingestion/ypl"
)

// landCapturedVideos is a pipeline under the ypl connector ypl-capture. Its poll
// has landed each of ids from the stand-in's capture playlist, and queued a
// captions parse of each. It answers the worker's handlers by capability.
func landCapturedVideos(t *testing.T, stand *standInYPL, api string, ids ...string) (*pipeline, map[string]worker.Handler) {
	t.Helper()
	stand.playlistIDs["capture"] = "PLtestcapture"
	for _, id := range ids {
		stand.add(standInVideo{id: id, title: "Test video " + id})
		stand.playlists["PLtestcapture"] = append(stand.playlists["PLtestcapture"], standInItem{itemID: "item" + id, videoID: id})
	}
	p := newYPLPipeline(t, map[string]string{"ypl-capture": yplCapture})
	handlers := yplHandlers(t, p, api)
	p.runConnector(t, "ypl-capture")
	if ran := p.run(t, handlers[store.PollCapability(ypl.Source)]); ran != 1 {
		t.Fatalf("%d polls ran, want 1", ran)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE capability = ?`, extract.Capability(ypl.CaptionsExtractor)); n != len(ids) {
		t.Fatalf("the poll queued %d captions parses, want %d", n, len(ids))
	}
	return p, handlers
}

// captionsOf is a captions answer as ypl's captions route writes it, with
// track null where kind is empty.
func captionsOf(id, description, kind string, duration int64, chapters []ypl.Chapter, segments []extract.Segment) map[string]any {
	var track any
	if kind != "" {
		track = map[string]string{"language": "en", "kind": kind}
	}
	if chapters == nil {
		chapters = []ypl.Chapter{}
	}
	if segments == nil {
		segments = []extract.Segment{}
	}
	return map[string]any{
		"video_id": id, "title": "Test video " + id, "description": description, "language": "en",
		"duration_seconds": duration, "track": track, "segments": segments, "chapters": chapters,
	}
}

// units are a parse's units as label: text.
func units(parse []store.Unit) []string {
	var out []string
	for _, u := range parse {
		label := ""
		if u.Label != nil {
			label = *u.Label
		}
		out = append(out, label+": "+u.Text)
	}
	return out
}

// parseOutput is the state, answer and error of the item's parse by producer,
// and an empty state where producer has written none.
func parseOutput(t *testing.T, p *pipeline, key, producer string) (state string, answer map[string]any, failure string) {
	t.Helper()
	var raw, errText *string
	err := p.st.Reader().QueryRowContext(t.Context(), `SELECT state, answer, error FROM outputs WHERE item_key = ? AND stage = ? AND producer = ?`,
		key, store.StageParse, producer).Scan(&state, &raw, &errText)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, ""
	}
	if err != nil {
		t.Fatalf("the %s parse of %s: %v", producer, key, err)
	}
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &answer); err != nil {
			t.Fatal(err)
		}
	}
	if errText != nil {
		failure = *errText
	}
	return state, answer, failure
}

// A video with a caption track parses from it: its description, then one
// unit per chapter labeled with its start and title. A video with no
// chapters parses to one unit per five-minute block that holds speech. The
// parse keeps the track it read, and queues the scan.
func TestAVideoParsesFromItsCaptionsByChapterOrByFiveMinuteBlock(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.captions["capvid00001"] = captionsOf("capvid00001", "The first test video's description.", "manual", 700,
		[]ypl.Chapter{{StartSeconds: 0, EndSeconds: 120, Title: "Opening"}, {StartSeconds: 120, EndSeconds: 600, Title: "The middle"}, {StartSeconds: 600, EndSeconds: 700, Title: "Closing"}},
		[]extract.Segment{
			{StartMs: 0, DurationMs: 4000, Text: "Hello and welcome."},
			{StartMs: 30000, DurationMs: 3000, Text: " "},
			{StartMs: 31000, DurationMs: 3000, Text: "  This is\nthe   opening. "},
			{StartMs: 125000, DurationMs: 2000, Text: "Now the middle."},
			{StartMs: 610000, DurationMs: 2000, Text: "And the close."},
			{StartMs: 650000, DurationMs: 2000, Text: "Goodbye."},
		})
	stand.captions["capvid00002"] = captionsOf("capvid00002", "The second test video's description.", "automatic", 960, nil,
		[]extract.Segment{
			{StartMs: 10000, DurationMs: 3000, Text: "First block."},
			{StartMs: 290000, DurationMs: 3000, Text: "Still the first."},
			{StartMs: 310000, DurationMs: 3000, Text: "Second block."},
			{StartMs: 905000, DurationMs: 3000, Text: "Fourth block."},
		})
	p, handlers := landCapturedVideos(t, stand, api.URL, "capvid00001", "capvid00002")
	if ran := p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)]); ran != 2 {
		t.Fatalf("%d captions parses ran, want 2", ran)
	}

	for _, c := range []struct {
		key   string
		kind  string
		units []string
	}{
		{"youtube/capvid00001", "manual", []string{
			"Description: The first test video's description.", "0:00 Opening: Hello and welcome. This is the opening.",
			"2:00 The middle: Now the middle.", "10:00 Closing: And the close. Goodbye.",
		}},
		{"youtube/capvid00002", "automatic", []string{
			"Description: The second test video's description.", "0:00: First block. Still the first.", "5:00: Second block.", "15:00: Fourth block.",
		}},
	} {
		parse := p.readParse(t, c.key, "")
		if got := units(parse.Units); parse.Extractor != ypl.CaptionsExtractor.Name || !slices.Equal(got, c.units) {
			t.Errorf("%s parsed by %s to %q, want %q", c.key, parse.Extractor, got, c.units)
		}
		if state, answer, _ := parseOutput(t, p, c.key, ypl.CaptionsExtractor.Name); state != store.OutputDone ||
			answer["track"].(map[string]any)["kind"] != c.kind {
			t.Errorf("%s's captions parse ended %s with answer %v, want done with its %s track", c.key, state, answer, c.kind)
		}
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageScan); n != 2 {
		t.Errorf("%d scans queued, want one per video", n)
	}
}

// A video with no caption track writes its captions parse deferred. That
// queues its Whisper parse and no scan, and leaves it out of review. The
// Whisper parse streams the video's audio from ypl through the transcriber,
// lays the transcript out by the video's chapters after its description,
// and queues the scan.
func TestAVideoWithNoCaptionTrackDefersItsParseToWhisper(t *testing.T) {
	stand, api := newStandInYPL(t)
	chapters := []ypl.Chapter{{StartSeconds: 0, EndSeconds: 300, Title: "Part one"}, {StartSeconds: 300, EndSeconds: 400, Title: "Part two"}}
	stand.captions["nocapvid001"] = captionsOf("nocapvid001", "A test video with no captions.", "", 400, chapters, nil)
	audio := toneWAV()
	stand.audio["nocapvid001"] = audio
	p, handlers := landCapturedVideos(t, stand, api.URL, "nocapvid001")
	const key = "youtube/nocapvid001"
	whisperCapability := extract.Capability(ypl.WhisperExtractor)

	p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
	if state, answer, _ := parseOutput(t, p, key, ypl.CaptionsExtractor.Name); state != store.OutputDeferred || answer["track"] != nil {
		t.Fatalf("the captions parse ended %s with answer %v, want deferred with no track", state, answer)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE capability = ? AND state = ?`, whisperCapability, store.TaskQueued); n != 1 {
		t.Errorf("%d Whisper parses queued, want 1", n)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageScan); n != 0 {
		t.Errorf("%d scans queued over the description alone, want none", n)
	}
	queue, err := p.st.ReviewQueue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(queue, func(w store.Waiting) bool { return w.Item.Key == key }) {
		t.Error("a video whose Whisper parse waits sits in review")
	}

	whisper := whisperHandler(t, api.URL, fakeTranscriber(t, audio, extract.TranscriptInfo{Language: "en", LanguageProbability: 0.97, DurationMs: 400000, VoicedMs: 390000},
		[]extract.Segment{{StartMs: 1000, DurationMs: 4000, Text: "Spoken words in part one."}, {StartMs: 301000, DurationMs: 5000, Text: " Spoken  words in part two."}}))
	if ran := p.run(t, whisper); ran != 1 {
		t.Fatalf("%d Whisper parses ran, want 1", ran)
	}
	parse := p.readParse(t, key, "")
	want := []string{"Description: A test video with no captions.", "0:00 Part one: Spoken words in part one.", "5:00 Part two: Spoken words in part two."}
	if got := units(parse.Units); parse.Extractor != ypl.WhisperExtractor.Name || !slices.Equal(got, want) {
		t.Errorf("the video parsed by %s to %q, want %q", parse.Extractor, got, want)
	}
	if state, answer, failure := parseOutput(t, p, key, ypl.WhisperExtractor.Name); state != store.OutputDone ||
		answer["model"] != extract.WhisperModel || answer["language"] != "en" {
		t.Errorf("the Whisper parse ended %s with answer %v and error %q, want done by %s in en", state, answer, failure, extract.WhisperModel)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageScan); n != 1 {
		t.Errorf("%d scans queued after the Whisper parse, want 1", n)
	}
}

// A video that ypl can read no part of fails its parse with ypl's refusal
// kept, and waits in review for want of a parse. So does every video while
// ypl holds no yt-dlp, though ypl answers that with a 503.
func TestAVideoThatYPLCannotReadFailsItsParseAndWaitsInReview(t *testing.T) {
	for _, c := range []struct {
		name, code string
		refuse     func(*standInYPL)
	}{
		{"a members-only video", "video_unavailable", func(s *standInYPL) { s.unreadable["refusedvid1"] = true }},
		{"a ypl with no yt-dlp", "youtube_reads_unavailable", func(s *standInYPL) { s.noYTDLP = true }},
	} {
		t.Run(c.name, func(t *testing.T) {
			stand, api := newStandInYPL(t)
			c.refuse(stand)
			p, handlers := landCapturedVideos(t, stand, api.URL, "refusedvid1")
			const key = "youtube/refusedvid1"

			p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
			if state, _, failure := parseOutput(t, p, key, ypl.CaptionsExtractor.Name); state != store.OutputFailed || !strings.Contains(failure, c.code) {
				t.Errorf("the captions parse ended %s with error %q, want failed with ypl's %s refusal", state, failure, c.code)
			}
			if !inReview(t, p, key, store.ReasonNoParse) {
				t.Error("the video whose parse failed does not wait in review as no-parse")
			}
		})
	}
}

// A parse that ypl answers busy goes back to the queue until the Retry-After
// it sent, however many times ypl answers so, and one that no answer from ypl
// reaches waits too. The waits spend none of its attempts, so a failed read of
// YouTube after them still runs again, and the video then parses.
func TestAVideoParseThatYPLAnswersBusyWaitsItsRetryAfterEachTime(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.captions["busyvideo01"] = captionsOf("busyvideo01", "A busy test video.", "manual", 60, nil,
		[]extract.Segment{{StartMs: 0, DurationMs: 3000, Text: "Words at last."}})
	stand.busy["busyvideo01"] = extract.ReadAttempts + 1
	stand.gatewayDown["busyvideo01"] = extract.ReadAttempts + 1
	stand.failing["busyvideo01"] = 1
	p, handlers := landCapturedVideos(t, stand, api.URL, "busyvideo01")
	captions := handlers[extract.Capability(ypl.CaptionsExtractor)]

	for range extract.ReadAttempts + 1 {
		if ran := p.run(t, captions); ran != 1 {
			t.Fatalf("%d captions parses ran, want 1", ran)
		}
		p.advance(30 * time.Second)
		if ran := p.run(t, captions); ran != 0 {
			t.Fatalf("a parse ran again 30 seconds after ypl asked it to wait 90")
		}
		p.advance(time.Minute)
	}
	for range extract.ReadAttempts + 1 {
		if ran := p.run(t, captions); ran != 1 {
			t.Fatalf("%d captions parses ran past a gateway that answered for ypl, want 1", ran)
		}
		p.advance(10 * time.Minute)
	}
	p.run(t, captions)
	if state, _, failure := parseOutput(t, p, "youtube/busyvideo01", ypl.CaptionsExtractor.Name); state != "" {
		t.Fatalf("a read of YouTube that failed after %d busy answers and %d unanswered reads ended the parse %s with error %q, want it queued again",
			extract.ReadAttempts+1, extract.ReadAttempts+1, state, failure)
	}
	p.advance(10 * time.Minute)
	p.run(t, captions)
	if state, _, failure := parseOutput(t, p, "youtube/busyvideo01", ypl.CaptionsExtractor.Name); state != store.OutputDone {
		t.Errorf("after %d busy answers and a failed read the parse ended %s with error %q, want done", extract.ReadAttempts+1, state, failure)
	}
}

// A parse that ypl answers busy for longer than extract.ReadWaitCeiling, as
// ypl answers a read past the time bound of the address it is reached at,
// fails with ypl's refusal kept, and the video waits in review.
func TestAVideoParseThatYPLKeepsAnsweringBusyFailsPastTheWaitCeiling(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.captions["stuckvideo1"] = captionsOf("stuckvideo1", "A test video that ypl never reads.", "manual", 60, nil, nil)
	stand.busy["stuckvideo1"] = 1 << 20
	p, handlers := landCapturedVideos(t, stand, api.URL, "stuckvideo1")
	const key = "youtube/stuckvideo1"
	captions := handlers[extract.Capability(ypl.CaptionsExtractor)]

	p.run(t, captions)
	p.advance(extract.ReadWaitCeiling - time.Hour)
	p.run(t, captions)
	if state, _, _ := parseOutput(t, p, key, ypl.CaptionsExtractor.Name); state != "" {
		t.Fatalf("the parse ended %s within the wait ceiling, want it still waiting", state)
	}
	p.advance(2 * time.Hour)
	p.run(t, captions)
	if state, _, failure := parseOutput(t, p, key, ypl.CaptionsExtractor.Name); state != store.OutputFailed || !strings.Contains(failure, "youtube_reads_busy") {
		t.Errorf("past the wait ceiling the parse ended %s with error %q, want failed with ypl's youtube_reads_busy", state, failure)
	}
	if !inReview(t, p, key, store.ReasonNoParse) {
		t.Error("the video whose parse waited past the ceiling does not wait in review as no-parse")
	}
}

// A Whisper parse whose worker dies each time, so that its leases lapse until
// its task fails, writes no output of its own. The video then waits in review
// as no-parse rather than staying deferred with nobody asked to act.
func TestAVideoWhoseWhisperParseLapsesUntilItFailsWaitsInReview(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.captions["lapsevideo1"] = captionsOf("lapsevideo1", "A test video whose Whisper parse dies.", "", 60, nil, nil)
	p, handlers := landCapturedVideos(t, stand, api.URL, "lapsevideo1")
	const key = "youtube/lapsevideo1"

	p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
	if inReview(t, p, key, store.ReasonNoParse) {
		t.Fatal("the video waits in review while its Whisper parse is queued")
	}
	for range store.LapsedClaimsFail + 1 {
		if _, err := p.worker.Claim(t.Context(), server.ClaimRequest{
			WorkerID: "dies", Capabilities: []string{extract.Capability(ypl.WhisperExtractor)}, Version: "test", Max: 1, LeaseSeconds: 60,
		}); err != nil {
			t.Fatal(err)
		}
		p.advance(2 * time.Minute)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE capability = ? AND state = ?`, extract.Capability(ypl.WhisperExtractor), store.TaskFailed); n != 1 {
		t.Fatalf("%d Whisper parses failed after their leases lapsed, want 1", n)
	}
	if !inReview(t, p, key, store.ReasonNoParse) {
		t.Error("the video whose Whisper parse failed does not wait in review as no-parse")
	}
}

// An audio stream that breaks partway sends the Whisper parse back to run
// again, however the transcriber ended on the half it read, and the parse
// then reads the whole audio.
func TestAVideoWhoseAudioStreamBreaksPartwayParsesOnTheNextRead(t *testing.T) {
	stand, api := newStandInYPL(t)
	stand.captions["cutvideo001"] = captionsOf("cutvideo001", "A test video whose audio breaks once.", "", 1, nil, nil)
	audio := toneWAV()
	stand.audio["cutvideo001"] = audio
	stand.cutAudio["cutvideo001"] = 1
	p, handlers := landCapturedVideos(t, stand, api.URL, "cutvideo001")
	const key = "youtube/cutvideo001"
	whisper := whisperHandler(t, api.URL, fakeTranscriber(t, audio, extract.TranscriptInfo{Language: "en", LanguageProbability: 0.9, DurationMs: 1000, VoicedMs: 1000},
		[]extract.Segment{{StartMs: 0, DurationMs: 1000, Text: "A whole second."}}))

	p.run(t, handlers[extract.Capability(ypl.CaptionsExtractor)])
	if ran := p.run(t, whisper); ran != 1 {
		t.Fatalf("%d Whisper parses ran, want 1", ran)
	}
	if state, _, failure := parseOutput(t, p, key, ypl.WhisperExtractor.Name); state != "" {
		t.Fatalf("a broken audio stream ended the Whisper parse %s with error %q, want it queued again", state, failure)
	}
	p.advance(10 * time.Minute)
	p.run(t, whisper)
	if state, _, failure := parseOutput(t, p, key, ypl.WhisperExtractor.Name); state != store.OutputDone {
		t.Errorf("the Whisper parse's second read ended %s with error %q, want done", state, failure)
	}
}

// whisperHandler is the worker's Whisper handler, reading through the
// stand-in ypl at api and transcribing with the program at transcriber.
func whisperHandler(t *testing.T, api, transcriber string) worker.Handler {
	t.Helper()
	h, ok := extract.SourceHandler(ypl.WhisperExtractorAt(transcriber), api,
		oauth2.NewClient(t.Context(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: yplToken})), quiet)
	if !ok {
		t.Fatal("no handler for a Whisper extractor whose program is installed")
	}
	return h
}

// inReview says whether the item under key waits in review for reason.
func inReview(t *testing.T, p *pipeline, key, reason string) bool {
	t.Helper()
	queue, err := p.st.ReviewQueue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(queue, func(w store.Waiting) bool { return w.Item.Key == key && w.Reason == reason })
}

// toneWAV is a second of a 440 Hz tone as 16 kHz mono 16-bit WAV.
func toneWAV() []byte {
	const rate = 16000
	samples := make([]byte, 2*rate)
	for i := range rate {
		binary.LittleEndian.PutUint16(samples[2*i:], uint16(int16(8000*math.Sin(2*math.Pi*440*float64(i)/rate))))
	}
	var b bytes.Buffer
	b.WriteString("RIFF")
	_ = binary.Write(&b, binary.LittleEndian, uint32(36+len(samples)))
	b.WriteString("WAVEfmt ")
	for _, field := range []any{uint32(16), uint16(1), uint16(1), uint32(rate), uint32(2 * rate), uint16(2), uint16(16)} {
		_ = binary.Write(&b, binary.LittleEndian, field)
	}
	b.WriteString("data")
	_ = binary.Write(&b, binary.LittleEndian, uint32(len(samples)))
	b.Write(samples)
	return b.Bytes()
}

// fakeTranscriber writes a program that stands in for audio-whisper. It
// fails unless the audio on its stdin is want. Otherwise it writes info, then
// each of segments, a JSON line each.
func fakeTranscriber(t *testing.T, want []byte, info extract.TranscriptInfo, segments []extract.Segment) string {
	t.Helper()
	dir := t.TempDir()
	fixture := filepath.Join(dir, "want.wav")
	if err := os.WriteFile(fixture, want, 0o600); err != nil {
		t.Fatal(err)
	}
	var lines bytes.Buffer
	encoder := json.NewEncoder(&lines)
	if err := encoder.Encode(info); err != nil {
		t.Fatal(err)
	}
	for _, s := range segments {
		if err := encoder.Encode(s); err != nil {
			t.Fatal(err)
		}
	}
	program := filepath.Join(dir, "audio-whisper")
	script := "#!/bin/sh\ncmp -s - '" + fixture + "' || { echo 'the audio on stdin is not the fixture' >&2; exit 1; }\ncat <<'EOF'\n" + lines.String() + "EOF\n"
	if err := os.WriteFile(program, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return program
}
