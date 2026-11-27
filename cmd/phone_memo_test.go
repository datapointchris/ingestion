package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/topics"
)

// m4aMemo is the opening of an M4A recording: an ftyp box naming the M4A
// brand, then a box that tail makes differ from memo to memo.
func m4aMemo(tail string) []byte {
	return []byte("\x00\x00\x00\x18ftypM4A \x00\x00\x00\x00M4A isom\x00\x00\x00\x10free" + tail)
}

// phoneClient registers the phone's service client, which holds the push
// scope alone, and answers an HTTP client under its token.
func phoneClient(t *testing.T, p *pipeline) *http.Client {
	t.Helper()
	p.provider.Register("ingestion-svc-phone", "phone-secret", server.ScopeItemsPush)
	return (&clientcredentials.Config{
		ClientID: "ingestion-svc-phone", ClientSecret: "phone-secret", TokenURL: p.provider.URL + "/token", Scopes: []string{server.ScopeItemsPush},
	}).Client(t.Context())
}

// send sends body to path under client, with idempotencyKey where it is not
// empty, and answers the status and body.
func send(t *testing.T, client *http.Client, method, url, idempotencyKey string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if idempotencyKey != "" {
		req.Header.Set(server.IdempotencyKey, idempotencyKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// pushedKey is the key of the one item that a push answered.
func pushedKey(t *testing.T, raw []byte) string {
	t.Helper()
	var answer server.PushAnswer
	if err := json.Unmarshal(raw, &answer); err != nil || len(answer.Items) != 1 {
		t.Fatalf("the push answered %s, want one item", raw)
	}
	return answer.Items[0].Key
}

// A memo's audio, posted under the phone client's token, lands one item in
// the owner's zone under memo/, with its bytes kept by their SHA-256, and its
// parse queued. A repeat under the same Idempotency-Key answers what it first
// landed and writes nothing. A body that holds no audio type the route reads
// answers 415. A JSON link from the phone still lands outside. The phone's
// token reaches only the push, and a person's token still pushes a memo.
func TestAPhonesMemoLandsAsTheOwnersAudioUnderItsHash(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	phone := phoneClient(t, p)
	memos := p.api.URL + "/api/v1/items?connector=phone"
	memo := m4aMemo("one")

	status, first := send(t, phone, http.MethodPost, memos, "memo-1", memo)
	if status != http.StatusCreated {
		t.Fatalf("the memo's push answered %d %s, want 201", status, first)
	}
	holdsTo(t, first, "push.schema.json")
	key := pushedKey(t, first)
	sum := sha256.Sum256(memo)
	if n := p.count(t, `SELECT count(*) FROM items i JOIN blobs b ON b.sha256 = i.blob_sha256 JOIN sightings s ON s.item_key = i.key
		WHERE i.key = ? AND i.key LIKE 'memo/%' AND i.zone = 'owner' AND i.media_type = ? AND b.sha256 = ? AND s.connector = 'phone'`,
		key, server.MediaTypeAudioMP4, hex.EncodeToString(sum[:])); n != 1 {
		t.Errorf("%d owner memos under %s hold the audio's SHA-256, want 1", n, key)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'parse' AND capability = ?`,
		key, extract.Capability(extract.AudioWhisper)); n != 1 {
		t.Errorf("the memo holds %d audio-whisper parse tasks, want 1", n)
	}
	rows := `SELECT (SELECT count(*) FROM items) + (SELECT count(*) FROM sightings) + (SELECT count(*) FROM blobs) + (SELECT count(*) FROM tasks)`
	landed := p.count(t, rows)
	if status, again := send(t, phone, http.MethodPost, memos, "memo-1", memo); status != http.StatusOK || string(again) != string(first) {
		t.Errorf("the repeat answered %d %s, want 200 with the first answer", status, again)
	}
	if now := p.count(t, rows); now != landed {
		t.Errorf("the repeat wrote %d rows, want none", now-landed)
	}

	for name, body := range map[string][]byte{"an Ogg recording": []byte("OggS\x00\x02\x00\x00"), "a PNG": []byte("\x89PNG\r\n\x1a\n\x00\x00")} {
		status, raw := send(t, phone, http.MethodPost, memos, "not-audio-"+name, body)
		var refused struct{ Code string }
		_ = json.Unmarshal(raw, &refused)
		if status != http.StatusUnsupportedMediaType || refused.Code != server.CodeMediaNotRead {
			t.Errorf("%s answered %d %s, want 415 %s", name, status, raw, server.CodeMediaNotRead)
		}
	}

	status, raw := send(t, phone, http.MethodPost, p.api.URL+"/api/v1/items", "link-1", []byte(`{"connector": "phone", "url": "https://youtu.be/phonevid001"}`))
	if status != http.StatusCreated || p.count(t, `SELECT count(*) FROM items WHERE key = 'youtube/phonevid001' AND zone = 'outside'`) != 1 {
		t.Errorf("the phone's link answered %d %s, want 201 and an outside video", status, raw)
	}

	if status, _ := send(t, phone, http.MethodGet, p.api.URL+"/api/v1/items", "", nil); status != http.StatusForbidden {
		t.Errorf("the phone's token listed the items with %d, want 403", status)
	}
	if status, raw := send(t, p.person, http.MethodPost, memos, "memo-2", m4aMemo("two")); status != http.StatusCreated {
		t.Errorf("a person's push of a memo answered %d %s, want 201", status, raw)
	}
}

// A memo's audio parses in the heavy slot into one unit per segment that holds
// speech, labeled by its span. Its scan queues the topics stage and no quality
// task, while an outside link's scan still queues the quality gate. The worker
// refuses a topics answer with a gap or an overlap. Two topics land two items
// with the memo as their parent, each with a parse of its units and a source
// time at its first unit. The memo then splits and routes nothing of its own.
// Each topic waits in review as person-sorts. fleet-relate queues a topic's
// note, and learning and watch refuse one.
func TestAMemoSplitsIntoOneItemPerTopic(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	memo := m4aMemo("one")
	whisper, err := extract.NewRegistry(extract.AudioWhisperAt(fakeTranscriber(t, memo,
		extract.TranscriptInfo{Language: "en", LanguageProbability: 0.98, DurationMs: 70000, VoicedMs: 17000},
		[]extract.Segment{
			{StartMs: 0, DurationMs: 4000, Text: "first words"},
			{StartMs: 4000, DurationMs: 3500, Text: " second  words "},
			{StartMs: 7500, DurationMs: 2000, Text: "  "},
			{StartMs: 9500, DurationMs: 5000, Text: "third"},
			{StartMs: 65000, DurationMs: 3000, Text: "fourth"},
		})))
	if err != nil {
		t.Fatal(err)
	}
	handlers := whisper.Handlers(p.worker, quiet)
	slotted, ok := handlers[0].(interface{ SlotGroup() (string, int) })
	if group, limit := slotted.SlotGroup(); !ok || group != extract.SlotGroupHeavy || limit != 1 {
		t.Errorf("audio-whisper runs in slot group %q of %d, want %s of 1", group, limit, extract.SlotGroupHeavy)
	}

	status, raw := send(t, p.person, http.MethodPost, p.api.URL+"/api/v1/items?connector=phone&recorded_at=2026-11-20T08:00:00Z", "memo-1", memo)
	if status != http.StatusCreated {
		t.Fatalf("the memo's push answered %d %s, want 201", status, raw)
	}
	key := pushedKey(t, raw)
	if ran := p.run(t, handlers...); ran != 1 {
		t.Fatalf("%d parses ran, want the memo's", ran)
	}
	if got, want := units(p.readParse(t, key, "").Units), []string{"00:00–00:04: first words", "00:04–00:07: second words", "00:09–00:14: third", "01:05–01:08: fourth"}; !slices.Equal(got, want) {
		t.Errorf("the memo's parse holds %q, want %q", got, want)
	}
	if ran := p.scanFindingNothing(t); ran != 1 {
		t.Fatalf("%d scans ran, want the memo's", ran)
	}
	if topicsTasks, quality := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'topics'`, key),
		p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'quality'`, key); topicsTasks != 1 || quality != 0 {
		t.Errorf("the memo's scan queued %d topics and %d quality tasks, want 1 and none", topicsTasks, quality)
	}
	if status, raw := send(t, p.person, http.MethodPost, p.api.URL+"/api/v1/items", "link-1", []byte(`{"connector": "phone", "url": "https://youtu.be/phonevid002"}`)); status != http.StatusCreated {
		t.Fatalf("the link's push answered %d %s, want 201", status, raw)
	}
	p.run(t, standInCaptions{})
	p.scanFindingNothing(t)
	if quality := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = 'youtube/phonevid002' AND stage = 'quality'`); quality != 1 {
		t.Errorf("the outside link's scan queued %d quality tasks, want 1", quality)
	}

	stand := newStandInDispatch(t, p.provider)
	topic := func(first, last int, title string) map[string]any {
		return map[string]any{"first_unit": first, "last_unit": last, "title": title, "idea": "The point of " + title + "."}
	}
	answers := []map[string]any{
		{"topics": []any{topic(1, 1, "a gap"), topic(3, 4, "after the gap")}},
		{"topics": []any{topic(1, 2, "an overlap"), topic(2, 4, "inside it")}},
		{"topics": []any{topic(1, 2, "Plan the trip"), topic(3, 4, "Fix the bike")}},
	}
	stand.answer = func(prompt string) map[string]any {
		if !strings.Contains(prompt, "[1] 00:00–00:04 first words\n[2] 00:04–00:07 second words") || !strings.Contains(prompt, "[4] 01:05–01:08 fourth") {
			t.Errorf("the prompt quotes the memo's units otherwise:\n%s", prompt)
		}
		answer := answers[0]
		answers = answers[1:]
		return answer
	}
	splitter := p.modelHandler(t, stand.models(t, "", ""), topics.Capability)
	for _, refusal := range []string{"a gap", "an overlap"} {
		p.run(t, splitter)
		var id, failure string
		if err := p.st.Reader().QueryRowContext(t.Context(), `SELECT t.id, coalesce(o.error, '') FROM tasks t JOIN outputs o ON o.item_key = t.item_key AND o.stage = t.stage
			WHERE t.item_key = ? AND t.stage = 'topics' AND t.state = 'failed' AND o.state = 'failed'`, key).Scan(&id, &failure); err != nil ||
			!strings.Contains(failure, refusal) {
			t.Fatalf("the answer with %s left no failed topics task naming it (%v, %q)", refusal, err, failure)
		}
		if _, err := runCLI(t, "tasks", "retry", id, "--config", p.config); err != nil {
			t.Fatal(err)
		}
	}
	p.run(t, splitter)

	recorded := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	for n, want := range []struct {
		title string
		units []string
		at    time.Time
	}{
		{"Plan the trip", []string{"00:00–00:04: first words", "00:04–00:07: second words"}, recorded},
		{"Fix the bike", []string{"00:09–00:14: third", "01:05–01:08: fourth"}, recorded.Add(9500 * time.Millisecond)},
	} {
		child := fmt.Sprintf("%s#%d", key, n+1)
		var title, zone, mediaType, parent, connector, source string
		if err := p.st.Reader().QueryRowContext(t.Context(), `SELECT i.title, i.zone, i.media_type, i.parent_key, s.connector, i.source_ts
			FROM items i JOIN sightings s ON s.item_key = i.key WHERE i.key = ?`, child).Scan(&title, &zone, &mediaType, &parent, &connector, &source); err != nil {
			t.Fatalf("topic %d: %v", n+1, err)
		}
		at, _ := time.Parse(time.RFC3339Nano, source)
		if title != want.title || zone != string(store.Owner) || mediaType != store.MediaTypeMemoTopic || parent != key || connector != "phone" || !at.Equal(want.at) {
			t.Errorf("topic %d is %q, %s, %s, under %s, sighted by %s at %s; want %q, the owner's topic under the memo, sighted by phone at %s",
				n+1, title, zone, mediaType, parent, connector, source, want.title, want.at)
		}
		if got := units(p.readParse(t, child, "").Units); !slices.Equal(got, want.units) {
			t.Errorf("topic %d's parse holds %q, want %q", n+1, got, want.units)
		}
	}
	if n := p.count(t, `SELECT count(*) FROM topics WHERE item_key = ?`, key); n != 2 {
		t.Errorf("the memo holds %d topics rows, want 2", n)
	}
	if got, want := decisionLines(t, p, key), []string{"side split topics gate"}; !slices.Equal(got, want) {
		t.Errorf("the memo holds decisions %q, want %q alone", got, want)
	}
	if rec, err := p.st.Item(t.Context(), key); err != nil || rec.Phase() != store.PhaseSplit {
		t.Errorf("the memo's phase is %q (%v), want %s", rec.Phase(), err, store.PhaseSplit)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage NOT IN ('parse', 'scan', 'topics')`, key) +
		p.count(t, `SELECT count(*) FROM deliveries WHERE item_key = ?`, key); n != 0 {
		t.Errorf("the memo holds %d tasks past its topics or deliveries, want none", n)
	}

	if ran := p.scanFindingNothing(t); ran != 2 {
		t.Fatalf("%d scans ran, want the two topics'", ran)
	}
	first, second := key+"#1", key+"#2"
	for _, child := range []string{first, second} {
		if !inReview(t, p, child, route.CausePersonSorts) || p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'quality'`, child) != 0 {
			t.Errorf("%s holds decisions %q, want a wait in review as person-sorts and no quality task", child, decisionLines(t, p, child))
		}
	}
	if _, err := runCLI(t, "review", "sort", first, route.VerbFleetRelate, "--config", p.config); err != nil {
		t.Fatal(err)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND stage = 'note' AND held_by = 'phone'`, first); n != 1 {
		t.Errorf("fleet-relate queued %d note tasks on the topic that the first fill holds, want 1", n)
	}
	for _, sort := range []struct {
		args    []string
		refusal string
	}{
		{[]string{route.VerbLearning}, "learning holds a book"}, {[]string{route.VerbWatch, "--playlist", "Later"}, "a watch adds a YouTube video"},
	} {
		args := append(append([]string{"review", "sort", second}, sort.args...), "--config", p.config)
		if _, err := runCLI(t, args...); err == nil || !strings.Contains(err.Error(), sort.refusal) {
			t.Errorf("%s on a topic answered %v, want a refusal saying %q", sort.args[0], err, sort.refusal)
		}
	}
	if !inReview(t, p, second, route.CausePersonSorts) {
		t.Errorf("the refused sorts moved the topic, which holds %q", decisionLines(t, p, second))
	}
}

// A topics answer covers a parse only where its runs of units start at 1,
// follow each other with no gap or overlap, and end at the last unit.
func TestATopicsAnswerCoversEachUnitOnceInOrder(t *testing.T) {
	run := func(first, last int) topics.AnswerTopic { return topics.AnswerTopic{FirstUnit: first, LastUnit: last} }
	for _, c := range []struct {
		runs  []topics.AnswerTopic
		units int
		want  string
	}{
		{[]topics.AnswerTopic{run(1, 2), run(3, 5)}, 5, ""},
		{[]topics.AnswerTopic{run(2, 5)}, 5, "a gap"},
		{[]topics.AnswerTopic{run(1, 2), run(4, 5)}, 5, "a gap"},
		{[]topics.AnswerTopic{run(1, 3), run(3, 5)}, 5, "an overlap"},
		{[]topics.AnswerTopic{run(1, 4)}, 5, "a gap"},
		{[]topics.AnswerTopic{run(1, 6)}, 5, "past the parse's last unit"},
		{[]topics.AnswerTopic{run(3, 2)}, 5, "before it starts"},
	} {
		err := topics.Answer{Topics: c.runs}.Covers(c.units)
		if (c.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%v over %d units answered %v, want %q", c.runs, c.units, err, c.want)
		}
	}
}

// decisionLines is each of the item's decisions as its gate, verdict, cause and
// who decided, by gate and in order.
func decisionLines(t *testing.T, p *pipeline, key string) []string {
	t.Helper()
	rows, err := p.st.Reader().QueryContext(t.Context(),
		`SELECT gate || ' ' || verdict || ' ' || cause || ' ' || decided_by FROM decisions WHERE item_key = ? ORDER BY gate DESC, sequence`, key)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
