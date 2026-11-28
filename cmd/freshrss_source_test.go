package cmd

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/datapointchris/ingestion/freshrss"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/publicweb"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
	"github.com/datapointchris/ingestion/ypl"
)

// The stand-in FreshRSS's one person: the login, the API password and the
// token that ClientLogin answers for them, as FreshRSS writes it.
const (
	freshRSSLogin    = "tester"
	freshRSSPassword = "test-api-password"
	freshRSSToken    = "tester/0123456789abcdef0123456789abcdef01234567"
)

// standInPageCap is the most entries that one page of the stand-in's reading
// list holds, below the poller's count, so a read follows its continuation.
const standInPageCap = 2

// standInEntry is one entry that the stand-in FreshRSS holds, with every
// title, link and text made up.
type standInEntry struct {
	// id is the entry's id, its crawl time in microseconds, as FreshRSS's is.
	id    int64
	n     int
	label string
	link  string
}

// standInFreshRSS answers FreshRSS's Google Reader API in the shapes that its
// greader.php writes: ClientLogin's SID, LSID and Auth lines for the one
// login and password it holds, and the reading list's stream/contents in
// FreshRSS's compat mode, the only one its API answers. A read's start time
// selects as FreshRSS's does: every entry whose id is at or after that
// second, in the order that r names. A page holds at most standInPageCap entries, and a full
// page names its last entry's id as its continuation, which the next read
// starts after. A read without the token answers 401 with Google-Bad-Token.
type standInFreshRSS struct {
	mu      sync.Mutex
	entries []standInEntry
	logins  int
	reads   int
}

func newStandInFreshRSS(t *testing.T) (*standInFreshRSS, *httptest.Server) {
	t.Helper()
	s := &standInFreshRSS{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/greader.php/accounts/ClientLogin", s.clientLogin)
	mux.HandleFunc("GET /api/greader.php/reader/api/0/stream/contents/user/-/state/com.google/reading-list", s.readingList)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(api.Close)
	return s, api
}

func (s *standInFreshRSS) clientLogin(w http.ResponseWriter, r *http.Request) {
	s.logins++
	if r.PostFormValue("Email") != freshRSSLogin || r.PostFormValue("Passwd") != freshRSSPassword {
		freshRSSUnauthorized(w)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	_, _ = fmt.Fprintf(w, "SID=%s\nLSID=null\nAuth=%s\n", freshRSSToken, freshRSSToken)
}

func freshRSSUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("Google-Bad-Token", "true")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = io.WriteString(w, "Unauthorized!")
}

func (s *standInFreshRSS) readingList(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "GoogleLogin auth="+freshRSSToken {
		freshRSSUnauthorized(w)
		return
	}
	s.reads++
	q := r.URL.Query()
	start, _ := strconv.ParseInt(q.Get("ot"), 10, 64) // codespell:ignore ot
	count, err := strconv.Atoi(q.Get("n"))
	if err != nil || count <= 0 {
		count = 20
	}
	count = min(count, standInPageCap)
	continuation, _ := strconv.ParseInt(q.Get("c"), 10, 64)
	oldestFirst := q.Get("r") == "o"
	list := slices.SortedFunc(slices.Values(s.entries), func(a, b standInEntry) int { return cmp.Compare(a.id, b.id) })
	if !oldestFirst {
		slices.Reverse(list)
	}
	var page []standInEntry
	for _, e := range list {
		after := continuation == 0 || oldestFirst && e.id > continuation || !oldestFirst && e.id < continuation
		if e.id >= start*1_000_000 && after && len(page) < count {
			page = append(page, e)
		}
	}
	items := []map[string]any{}
	for _, e := range page {
		items = append(items, map[string]any{
			"id":            fmt.Sprintf("tag:google.com,2005:reader/item/%016x", e.id),
			"crawlTimeMsec": strconv.FormatInt(e.id/1000, 10),
			"timestampUsec": strconv.FormatInt(e.id, 10),
			"published":     e.id/1_000_000 - 3600,
			"title":         fmt.Sprintf("Test entry %d &amp; more", e.n),
			"canonical":     []map[string]string{{"href": e.link}},
			"alternate":     []map[string]string{{"href": e.link}},
			"categories":    []string{"user/-/state/com.google/reading-list", "user/-/label/" + e.label},
			"origin":        map[string]string{"streamId": "feed/7", "htmlUrl": "https://feed.example.com/", "title": "Test feed"},
			"summary":       map[string]string{"content": fmt.Sprintf("<p>Feed content of test entry %d.</p>", e.n)},
		})
	}
	answer := map[string]any{"id": "user/-/state/com.google/reading-list", "updated": time.Now().Unix(), "items": items}
	if len(page) >= count {
		answer["continuation"] = strconv.FormatInt(page[len(page)-1].id, 10)
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_ = json.NewEncoder(w).Encode(answer)
}

// add adds an entry numbered n, crawled at crawled, whose link names
// pages' /post/<n>, under label.
func (s *standInFreshRSS) add(pages *httptest.Server, crawled time.Time, label string, n ...int) {
	for i, number := range n {
		s.addLink(crawled.Add(time.Duration(i)*time.Microsecond), label, number, fmt.Sprintf("%s/post/%d?utm_source=rss", pages.URL, number))
	}
}

// addLink adds an entry numbered n, crawled at crawled, whose link is link,
// under label.
func (s *standInFreshRSS) addLink(crawled time.Time, label string, n int, link string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, standInEntry{id: crawled.UnixMicro(), n: n, label: label, link: link})
}

// newStandInPages serves each /post/<n> as a page whose article reads
// "Page text of test entry <n>.", and each /post/<n>.pdf as standInPDF(n). It
// answers 403 for each n in refused, as a site that refuses the worker's GET
// does. It answers 503 to the first GET of each n in unavailableOnce, as a
// site that is down for a while does.
func newStandInPages(t *testing.T, refused, unavailableOnce []int) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	asked := map[int]int{}
	pages := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if number, ok := strings.CutSuffix(strings.TrimPrefix(r.URL.Path, "/post/"), ".pdf"); ok {
			n, _ := strconv.Atoi(number)
			w.Header().Set("Content-Type", extract.MediaTypePDF)
			_, _ = io.WriteString(w, standInPDF(n))
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/post/"))
		mu.Lock()
		asked[n]++
		first := asked[n] == 1
		mu.Unlock()
		switch {
		case err != nil || slices.Contains(refused, n):
			w.WriteHeader(http.StatusForbidden)
			return
		case first && slices.Contains(unavailableOnce, n):
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, standInPage(n))
	}))
	t.Cleanup(pages.Close)
	return pages
}

func standInPage(n int) string {
	return fmt.Sprintf("<!doctype html><html><head><title>Test entry %d | Test site</title></head><body><nav>Home</nav><article>\n"+
		"<p>Page text of test entry %d.</p>\n</article></body></html>", n, n)
}

// standInPDF is a document that the pipeline's stand-in for pdf-text reads as
// one page.
func standInPDF(n int) string { return fmt.Sprintf("%%PDF-1.7\nText: PDF text of test entry %d.\n", n) }

// standInPagesAlone is a transport that reaches pages and nothing else, so a
// GET of a link elsewhere fails rather than reaching the internet.
func standInPagesAlone(pages *httptest.Server) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if "http://"+r.URL.Host != pages.URL {
			return nil, errors.New("the test reaches its stand-in pages alone")
		}
		return http.DefaultTransport.RoundTrip(r)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// newStandInPoller is a poller of the stand-in FreshRSS at api whose GETs
// reach pages alone.
func newStandInPoller(api, pages *httptest.Server) freshrss.Poller {
	base := api.URL + "/api/greader.php"
	return freshrss.NewPoller(freshrss.NewClient(base, freshrss.SignedIn(base, freshRSSLogin, freshRSSPassword)),
		publicweb.NewReader(&http.Client{Timeout: 10 * time.Second, Transport: standInPagesAlone(pages)}), extract.Builtin())
}

// newFreshRSSPipeline is a pipeline under one freshrss connector, polled
// hourly, and the cli-push connector, which pushes to the destination psyche.
// Its parse of a page runs a stand-in for article-trafilatura that writes one
// unit for each line that holds a paragraph, and fetches the page of an item
// that holds no bytes. Its parse of a PDF runs a stand-in for pdf-text that
// writes one unit for each line that opens on "Text: ".
func newFreshRSSPipeline(t *testing.T) (*pipeline, *extract.Registry) {
	t.Helper()
	dir := t.TempDir()
	stand := func(name, script string) string {
		program := filepath.Join(dir, name)
		if err := os.WriteFile(program, []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return program
	}
	article := extract.Program(extract.ArticleTrafilatura.Name, extract.ArticleTrafilatura.Version, extract.MediaTypeHTML,
		stand("article-trafilatura", `sed -n 's|.*<p>\([^<]*\)</p>.*|{"text": "\1"}|p'`))
	article.FetchesPage = true
	pdf := extract.Program(extract.PDFText.Name, extract.PDFText.Version, extract.MediaTypePDF,
		stand("pdf-text", `sed -n 's|^Text: \(.*\)$|{"text": "\1"}|p'`))
	pdf.TakesFetchedPages = true
	extractors, err := extract.NewRegistry(article, pdf)
	if err != nil {
		t.Fatal(err)
	}
	p := newPipeline(t, extractors, afterStage(quiet))
	if err := os.MkdirAll(filepath.Join(dir, "set", "connectors"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, declaration := range map[string]string{
		"connectors/freshrss.yml": "name: freshrss\nmode: poll\nsource: freshrss\nzone: outside\nschedule: \"@hourly\"\n",
		"connectors/cli-push.yml": "name: cli-push\nmode: push\nsource: cli-pushes\ndue_interval: 30d\n",
		"destinations.yml":        "destinations:\n  - name: psyche\n    side: personal\n    push_only: true\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, "set", name), []byte(declaration), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p.apply(t, filepath.Join(dir, "set"))
	return p, extractors
}

// blobText is the bytes that the service keeps under sum.
func (p *pipeline) blobText(t *testing.T, sum *string) string {
	t.Helper()
	if sum == nil {
		t.Fatal("the item holds no bytes")
	}
	body, _, err := p.worker.Blob(t.Context(), *sum)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = body.Close() }()
	raw, _ := io.ReadAll(body)
	return string(raw)
}

// A freshrss connector's first poll signs in, keeps the newest entry's crawl
// time and lands nothing. The next lands each entry crawled since, oldest
// first, across the reading list's pages, under its canonical link's key. It
// keeps the page that the link names as the item's text/html bytes, which
// the article parse reads, and the sighting names the feed and its label. A
// page that answers 403 lands the entry's content from the feed instead, and
// its sighting says why. A page that answers 503 waits for the next poll,
// which lands it. A YouTube video's link lands as the video with no bytes, as
// a pushed one does, and a Reddit post's link lands nothing. An entry that
// FreshRSS commits late, crawled before the cursor, lands on the next poll. A
// poll with nothing new lands nothing, though the API answers again each
// entry in the hour behind the cursor.
func TestAFreshRSSPollLandsEachNewEntryOnceWithItsPageAfterItsBaseline(t *testing.T) {
	stand, api := newStandInFreshRSS(t)
	pages := newStandInPages(t, []int{6}, []int{8})
	now := time.Now()
	stand.add(pages, now.Add(-3*time.Hour), "Uncategorized", 1, 2)
	stand.add(pages, now.Add(-2*time.Hour), "neovim", 3)
	p, extractors := newFreshRSSPipeline(t)
	poll := &recording{Handler: polling.Handler(freshrss.Source, newStandInPoller(api, pages), p.worker, quiet)}
	pollNow := func() []string {
		t.Helper()
		p.runConnector(t, "freshrss")
		p.run(t, poll)
		return poll.landedKeys(t)
	}
	keyOf := func(n int) string {
		key, err := itemkey.FromURL(fmt.Sprintf("%s/post/%d", pages.URL, n))
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	video, _ := itemkey.FromURL("https://www.youtube.com/watch?v=testvideo01")

	if got := pollNow(); len(got) != 0 {
		t.Fatalf("the baseline poll reported %v, want nothing", got)
	}
	stand.add(pages, now.Add(-time.Hour), "neovim", 4, 5)
	stand.add(pages, now.Add(-30*time.Minute), "Uncategorized", 6)
	stand.addLink(now.Add(-25*time.Minute), "Uncategorized", 9, "https://www.youtube.com/watch?v=testvideo01&feature=rss")
	stand.add(pages, now.Add(-20*time.Minute), "Uncategorized", 8)
	stand.addLink(now.Add(-15*time.Minute), "Uncategorized", 10, "https://www.reddit.com/r/test/comments/abc123/test_post/")
	if got := pollNow(); !slices.Equal(got, []string{keyOf(4), keyOf(5), keyOf(6), video}) {
		t.Fatalf("the poll after six new entries reported %v, want the four that land now, oldest first", got)
	}
	shown := p.item(t, keyOf(4))
	s := shown.Sightings[0]
	if shown.Item.MediaType != extract.MediaTypeHTML || shown.Item.Zone != store.Outside ||
		*shown.Item.URL != fmt.Sprintf("%s/post/4", pages.URL) || shown.Item.Title == nil || *shown.Item.Title != "Test entry 4 & more" ||
		shown.Item.SourceTS == nil || s.Fields[freshrss.FieldFeedStreamID] != "feed/7" || s.Fields[freshrss.FieldFeedTitle] != "Test feed" ||
		s.Fields[freshrss.FieldFeedLabel] != "neovim" || s.Fields[freshrss.FieldBody] != freshrss.BodyPage ||
		!strings.HasPrefix(s.ExternalKey, "tag:google.com,2005:reader/item/") {
		t.Errorf("the landed entry is %s, want its page's media type, canonical link, title, time, feed and label", jsonOf(shown))
	}
	if got := p.blobText(t, shown.Item.BlobSHA256); got != standInPage(4) {
		t.Errorf("the entry's bytes are %q, want its page", got)
	}
	refused := p.item(t, keyOf(6))
	if f := refused.Sightings[0].Fields; f[freshrss.FieldBody] != freshrss.BodyFeed || f[freshrss.FieldPageRefused] != "HTTP 403" ||
		!strings.Contains(p.blobText(t, refused.Item.BlobSHA256), "<p>Feed content of test entry 6.</p>") {
		t.Errorf("the entry whose page refused the GET is %s, want the feed's content and why", jsonOf(refused))
	}
	if v := p.item(t, video); v.Item.MediaType != ypl.MediaTypeVideo || v.Item.BlobSHA256 != nil ||
		v.Sightings[0].Fields[freshrss.FieldFeedStreamID] != "feed/7" {
		t.Errorf("the entry that links a video is %s, want the video with no bytes, sighted from the feed", jsonOf(v))
	}

	stand.add(pages, now.Add(-35*time.Minute), "neovim", 7)
	if got := pollNow(); !slices.Equal(got, []string{keyOf(7), keyOf(8)}) {
		t.Fatalf("the poll after a late entry and a page that answered 503 once reported %v, want both", got)
	}
	if f := p.item(t, keyOf(8)).Sightings[0].Fields; f[freshrss.FieldBody] != freshrss.BodyPage {
		t.Errorf("the entry whose page answered 503 once landed %s, want its page", f[freshrss.FieldBody])
	}

	if n := p.parse(t, extractors); n != 5 {
		t.Fatalf("ran %d parses, want one for each new page", n)
	}
	for n, want := range map[int]string{4: "Page text of test entry 4.", 6: "Feed content of test entry 6."} {
		if parse := p.readParse(t, keyOf(n), ""); len(parse.Units) != 1 || parse.Units[0].Text != want ||
			parse.Extractor != extract.ArticleTrafilatura.Name {
			t.Errorf("entry %d parsed as %s, want the one unit %q", n, jsonOf(parse), want)
		}
	}

	if got := pollNow(); len(got) != 0 {
		t.Errorf("a poll with nothing new reported %v, want nothing", got)
	}
	if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, extract.MediaTypeHTML); n != 5 {
		t.Errorf("the polls left %d pages, want the 5 new entries that link one", n)
	}
	if stand.logins != 1 {
		t.Errorf("the worker signed in %d times over four polls, want once", stand.logins)
	}
}

// A poll whose keep fails partway, as the blob route refuses while serve
// rolls, reports the entries before the failure and moves the cursor past
// those alone. The next poll lands the rest.
func TestAFreshRSSPollWhoseKeepFailsPartwayReportsTheEntriesBeforeIt(t *testing.T) {
	stand, api := newStandInFreshRSS(t)
	pages := newStandInPages(t, nil, nil)
	now := time.Now()
	stand.add(pages, now.Add(-2*time.Hour), "Uncategorized", 1)
	poller := newStandInPoller(api, pages)
	keeps := 0
	keep := func(context.Context, []byte, string) (string, error) {
		keeps++
		if keeps == 2 {
			return "", errors.New("keep a page: the service answered 503")
		}
		return fmt.Sprintf("%064x", keeps), nil
	}
	keysOf := func(o polling.Outcome) []string {
		keys := []string{}
		for _, a := range o.Arrivals {
			keys = append(keys, a.Key)
		}
		return keys
	}
	keyOf := func(n int) string {
		key, _ := itemkey.FromURL(fmt.Sprintf("%s/post/%d", pages.URL, n))
		return key
	}

	baseline := poller.Poll(t.Context(), polling.Poll{Keep: keep})
	stand.add(pages, now.Add(-time.Hour), "Uncategorized", 2, 3, 4)
	stopped := poller.Poll(t.Context(), polling.Poll{Cursor: baseline.Cursor, Keep: keep})
	if stopped.Error != nil || stopped.Cursor == nil || len(stopped.Warnings) != 1 || !slices.Equal(keysOf(stopped), []string{keyOf(2)}) {
		t.Fatalf("the poll whose second keep failed ended %s, want entry 2 reported, the cursor moved and a warning", jsonOf(stopped))
	}
	resumed := poller.Poll(t.Context(), polling.Poll{Cursor: stopped.Cursor, Keep: keep})
	if resumed.Error != nil || !slices.Equal(keysOf(resumed), []string{keyOf(3), keyOf(4)}) {
		t.Errorf("the next poll ended %s, want entries 3 and 4", jsonOf(resumed))
	}
}

// A worker opened from its [source.freshrss] table signs in as the table's
// login with the password in INGESTION_SOURCE_FRESHRSS_PASSWORD. Its GET of a
// page at an address that is not public, here the stand-in's loopback, never
// connects, so the entry lands its content from the feed.
func TestAFreshRSSWorkerSignsInFromItsTableAndLandsTheFeedsContentForAPrivatePage(t *testing.T) {
	stand, api := newStandInFreshRSS(t)
	pages := newStandInPages(t, nil, nil)
	stand.add(pages, time.Now().Add(-2*time.Hour), "Uncategorized", 1)
	p, _ := newFreshRSSPipeline(t)
	t.Setenv(sourcePasswordEnv(freshrss.Source), freshRSSPassword)
	config := filepath.Join(t.TempDir(), "worker.toml")
	table := fmt.Sprintf("[source.freshrss]\napi = %q\nlogin = %q\n", api.URL+"/api/greader.php", freshRSSLogin)
	if err := os.WriteFile(config, []byte(table), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := readConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	var handler worker.Handler
	for _, h := range openSources(t.Context(), cfg, p.provider.URL, p.worker, quiet).handlers {
		if h.Capability() == store.PollCapability(freshrss.Source) {
			handler = h
		}
	}
	if handler == nil {
		t.Fatal("a worker with a [source.freshrss] table and its password holds no poll:freshrss handler")
	}
	poll := &recording{Handler: handler}

	p.runConnector(t, "freshrss")
	p.run(t, poll)
	if got := poll.landedKeys(t); len(got) != 0 || stand.logins != 1 || stand.reads != 1 {
		t.Fatalf("the baseline poll reported %v after %d sign-ins and %d reads, want nothing after one of each", got, stand.logins, stand.reads)
	}
	stand.add(pages, time.Now(), "Uncategorized", 2)
	p.runConnector(t, "freshrss")
	p.run(t, poll)
	key, _ := itemkey.FromURL(pages.URL + "/post/2")
	if got := poll.landedKeys(t); !slices.Equal(got, []string{key}) {
		t.Fatalf("the poll after a new entry reported %v, want it", got)
	}
	shown := p.item(t, key)
	if f := shown.Sightings[0].Fields; f[freshrss.FieldBody] != freshrss.BodyFeed || f[freshrss.FieldPageRefused] != "an address that is not public" ||
		!strings.Contains(p.blobText(t, shown.Item.BlobSHA256), "<p>Feed content of test entry 2.</p>") {
		t.Errorf("the entry whose page is at a private address is %s, want the feed's content and why", jsonOf(shown))
	}
	if last := poll.results[len(poll.results)-1]; last.Outcome != server.OutcomeDone {
		t.Errorf("the poll ended %s: %v", last.Outcome, last.Error)
	}
}
