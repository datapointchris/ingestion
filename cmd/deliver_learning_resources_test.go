package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// learningClient is the service client that a worker writes to the stand-in
// learning as, with learningSecret.
const (
	learningClient = "learning-svc-ingestion"
	learningSecret = "learning-secret"
)

// standInLearning answers learning's resource create as its release
// publishes it: POST /api/v1/resources takes a ResourceCreate and answers a
// ResourceCreated or a CreateConflictResponse. It admits a service token whose
// scp holds learning.resources.write. A key already held answers first: 200
// where the body matches the one that made or adopted its resource, and 409
// source_key otherwise or where that resource is deleted. A title holding a
// NUL answers 500, as the store's text refuses one. A chapter is then
// created where its part_of names a live book, and refused 422 otherwise. A
// path that one live resource holds exactly under no key is adopted. A path
// held under another key, inside a held directory, by two resources, or by
// deleted ones alone answers 409 file_path naming its holders.
type standInLearning struct {
	*httptest.Server
	mu        sync.Mutex
	resources []*standInResource
	// creates holds each create's body in the order it came.
	creates []standInCreate
	// failing counts the creates of each source_key that answer 500 first.
	failing map[string]int
}

type standInCreate struct {
	Title            string `json:"title"`
	ResourceType     string `json:"resource_type"`
	BookCopy         string `json:"book_copy"`
	FilePath         string `json:"file_path"`
	SourceKey        string `json:"source_key"`
	PartOf           int64  `json:"part_of"`
	PDFPageStart     int    `json:"pdf_page_start"`
	PDFPageEnd       int    `json:"pdf_page_end"`
	PrintedPageStart string `json:"printed_page_start"`
	PrintedPageEnd   string `json:"printed_page_end"`
}

type standInResource struct {
	id        int64
	filePath  string
	sourceKey *string
	deleted   bool
	// made is the body that made the resource under its key or adopted it.
	made *standInCreate
}

func newStandInLearning(t *testing.T, provider *authtest.Provider) *standInLearning {
	t.Helper()
	provider.Register(learningClient, learningSecret, deliver.LearningResourcesScopes...)
	verifier := provider.Verifier(t, "learning-cli-", "learning-svc-")
	l := &standInLearning{failing: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/resources", func(w http.ResponseWriter, r *http.Request) {
		token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		caller, err := verifier.Verify(r.Context(), token)
		if !bearer || err != nil || !caller.Service || !slices.Contains(caller.Scopes, "learning.resources.write") {
			writeLearning(w, http.StatusForbidden, map[string]any{"error": "Forbidden", "message": "this token reaches no resource create"})
			return
		}
		var body standInCreate
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Title == "" || body.SourceKey == "" {
			writeLearning(w, http.StatusBadRequest, map[string]any{"error": "Bad Request", "message": "title is required"})
			return
		}
		if strings.ContainsRune(body.Title, 0) {
			writeLearning(w, http.StatusInternalServerError, map[string]any{
				"error": "Internal Server Error", "message": `creating resource: ERROR: invalid byte sequence for encoding "UTF8": 0x00 (SQLSTATE 22021)`,
			})
			return
		}
		chapter := body.ResourceType == "chapter"
		if !chapter && body.ResourceType != "book" {
			writeLearning(w, http.StatusUnprocessableEntity, map[string]any{
				"error": "Unprocessable Entity", "message": "resource_type names no resource type", "field": "resource_type", "known": []string{"book", "chapter"},
			})
			return
		}
		if chapter == (body.FilePath != "") || chapter == (body.PartOf == 0) {
			writeLearning(w, http.StatusBadRequest, map[string]any{"error": "Bad Request", "message": "a book takes a file_path, and a chapter a part_of"})
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.creates = append(l.creates, body)
		if l.failing[body.SourceKey] > 0 {
			l.failing[body.SourceKey]--
			writeLearning(w, http.StatusInternalServerError, map[string]any{"error": "Internal Server Error", "message": "connection reset"})
			return
		}
		if i := slices.IndexFunc(l.resources, func(res *standInResource) bool { return text(res.sourceKey) == body.SourceKey }); i >= 0 {
			held := l.resources[i]
			if held.deleted || *held.made != body {
				l.conflict(w, "source_key", body, []*standInResource{held})
				return
			}
			l.created(w, http.StatusOK, held, false)
			return
		}
		if chapter {
			if !slices.ContainsFunc(l.resources, func(res *standInResource) bool {
				return res.id == body.PartOf && !res.deleted && (res.made == nil || res.made.ResourceType == "book")
			}) {
				writeLearning(w, http.StatusUnprocessableEntity, map[string]any{
					"error": "Unprocessable Entity", "message": fmt.Sprintf("%d names no live book", body.PartOf), "field": "part_of",
				})
				return
			}
			res := &standInResource{id: int64(len(l.resources) + 1), sourceKey: &body.SourceKey, made: &body}
			l.resources = append(l.resources, res)
			l.created(w, http.StatusCreated, res, false)
			return
		}
		var live, deleted []*standInResource
		for _, res := range l.resources {
			if res.filePath == body.FilePath || strings.HasPrefix(body.FilePath, res.filePath+"/") {
				if res.deleted {
					deleted = append(deleted, res)
				} else {
					live = append(live, res)
				}
			}
		}
		switch {
		case len(live) == 0 && len(deleted) == 0:
			res := &standInResource{id: int64(len(l.resources) + 1), filePath: body.FilePath, sourceKey: &body.SourceKey, made: &body}
			l.resources = append(l.resources, res)
			l.created(w, http.StatusCreated, res, false)
		case len(live) == 1 && live[0].filePath == body.FilePath && live[0].sourceKey == nil:
			live[0].sourceKey, live[0].made = &body.SourceKey, &body
			l.created(w, http.StatusOK, live[0], true)
		case len(live) > 0:
			l.conflict(w, "file_path", body, live)
		default:
			l.conflict(w, "file_path", body, deleted)
		}
	})
	l.Server = httptest.NewServer(mux)
	t.Cleanup(l.Close)
	return l
}

// hold makes a resource by hand at filePath under no key, deleted where
// deleted is set, and answers its id.
func (l *standInLearning) hold(filePath string, deleted bool) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	res := &standInResource{id: int64(len(l.resources) + 1), filePath: filePath, deleted: deleted}
	l.resources = append(l.resources, res)
	return fmt.Sprint(res.id)
}

// sent is each create's body for key, in the order they came.
func (l *standInLearning) sent(key string) []standInCreate {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []standInCreate
	for _, c := range l.creates {
		if c.SourceKey == key {
			out = append(out, c)
		}
	}
	return out
}

func (l *standInLearning) created(w http.ResponseWriter, status int, res *standInResource, adopted bool) {
	writeLearning(w, status, map[string]any{
		"id": res.id, "title": "a title", "resource_type_id": 1, "book_copy": "pdf", "file_path": res.filePath,
		"source_key": res.sourceKey, "adopted": adopted,
	})
}

func (l *standInLearning) conflict(w http.ResponseWriter, conflict string, body standInCreate, holders []*standInResource) {
	listed := make([]map[string]any, len(holders))
	for i, h := range holders {
		listed[i] = map[string]any{"resource_id": h.id, "file_path": h.filePath, "source_key": h.sourceKey, "deleted": h.deleted}
	}
	answer := map[string]any{
		"error": "Conflict", "message": fmt.Sprintf("resource %d holds it", holders[0].id), "conflict": conflict,
		"source_key": body.SourceKey, "resource_id": holders[0].id, "deleted": holders[0].deleted,
	}
	if conflict == "file_path" {
		answer["file_path"], answer["holders"] = body.FilePath, listed
	}
	writeLearning(w, http.StatusConflict, answer)
}

func writeLearning(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// standInBookExtractors reads a PDF and a markdown file as one unit each,
// standing in for pdf-text, which only the worker image installs.
func standInBookExtractors(t *testing.T) *extract.Registry {
	t.Helper()
	program := filepath.Join(t.TempDir(), "one-unit")
	if err := os.WriteFile(program, []byte("#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '{\"text\":\"a page\"}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	extractors, err := extract.NewRegistry(
		extract.Program("pdf-text", "1", "application/pdf", program), extract.Program("markdown", "1", "text/markdown", program))
	if err != nil {
		t.Fatal(err)
	}
	return extractors
}

// The books-folder connector tags each file below the books root book, which
// places it at learning, whose deliverer is learning-resources. Each PDF is
// created in a stand-in learning under the item's key, as a book whose pdf
// copy is the file's path below the root. A new book is delivered. One whose
// path learning holds by hand under no key is adopted, delivered-to-existing.
// One inside a directory that a resource holds is refused naming it, and so is
// one whose path only a deleted resource holds. One that learning answers 500
// stays pending. Its file then moves, and its retry still sends the first
// request's title and path. A markdown file in the folder is no PDF, so it
// waits not-taken and learning hears nothing of it.
func TestABookPlacedAtLearningIsCreatedThereUnderItsKeyAtItsPath(t *testing.T) {
	ctx := t.Context()
	books := t.TempDir()
	files := map[string]string{
		"new.pdf": "a new book", "shelf/by-hand.pdf": "a book saved by hand", "series/part-1.pdf": "a part of a series",
		"gone.pdf": "a deleted book", "flaky.pdf": "a book learning fails once", "notes.md": "# notes",
	}
	keys := map[string]string{}
	for name, body := range files {
		path := filepath.Join(books, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(body))
		key, err := itemkey.FromFileSHA256(hex.EncodeToString(sum[:]))
		if err != nil {
			t.Fatal(err)
		}
		keys[name] = key
	}
	extractors := standInBookExtractors(t)
	p := newPipelineOver(t, extractors, afterStage(quiet), map[string]string{"books": books})
	declarations := lifeSet(t, withLife+"  - name: learning\n    side: personal\n    tags: [book]\n    deliverer: learning-resources\n")
	p.apply(t, declarations)
	learning := newStandInLearning(t, p.provider)
	byHand, series, gone := learning.hold("shelf/by-hand.pdf", false), learning.hold("series", false), learning.hold("gone.pdf", true)
	learning.failing[keys["flaky.pdf"]] = 1

	cfg := fileConfig{Deliver: map[string]deliverTable{deliver.LearningResources: {
		serviceClient: serviceClient{API: learning.URL, ClientID: learningClient}, Root: "books",
	}}}
	cfg.Serve.Homes = map[string]string{"books": books}
	if err := setDelivererRoots(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	handlers := workerHandlers(p.worker, openDeliverers(ctx, cfg, p.provider.URL, quiet), openedSources{}, nil, nil, nil, modelConfig{}, quiet)
	i := slices.IndexFunc(handlers, func(h worker.Handler) bool {
		return h.Capability() == store.DeliverCapability(deliver.LearningResources)
	})
	if i < 0 {
		t.Fatalf("the worker holds no %s handler", deliver.LearningResources)
	}
	learningResources := handlers[i]
	set, err := connectors.Read(declarations, nil)
	if err != nil {
		t.Fatal(err)
	}
	folder, _ := set.Connector("books-folder")
	scanFolder := func() {
		t.Helper()
		if _, err := connectors.ScanFolder(ctx, p.lander, folder, time.Now().Add(time.Minute), time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	scanFolder()
	p.parse(t, extractors)
	p.scanFindingNothing(t)
	if ran := p.run(t, learningResources); ran != 5 {
		t.Fatalf("the worker ran %d deliveries, want one for each of the 5 PDFs placed at learning", ran)
	}

	want := map[string]struct {
		server.DeliveryView
		quotes string
	}{
		"new.pdf":           {DeliveryView: server.DeliveryView{Outcome: store.DeliveryDelivered, DestinationID: ptr("4")}},
		"shelf/by-hand.pdf": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryDeliveredToExisting, DestinationID: &byHand}},
		"series/part-1.pdf": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryRefused, DestinationID: &series}, quotes: "holds the directory series"},
		"gone.pdf":          {DeliveryView: server.DeliveryView{Outcome: store.DeliveryRefused, DestinationID: &gone}, quotes: "deleted"},
		"flaky.pdf":         {DeliveryView: server.DeliveryView{Outcome: store.DeliveryPending}, quotes: "learning answered 500"},
	}
	for name, w := range want {
		d := p.shown(t, keys[name]).Deliveries
		if len(d) != 1 || d[0].Destination != "learning" || d[0].Outcome != w.Outcome || text(d[0].DestinationID) != text(w.DestinationID) ||
			!strings.Contains(text(d[0].LastError), w.quotes) || (w.quotes == "") != (d[0].LastError == nil) {
			t.Errorf("%s holds deliveries %s, want one to learning %s as %s, quoting %q", name, jsonOf(d), w.Outcome, text(w.DestinationID), w.quotes)
		}
		stem := strings.TrimSuffix(filepath.Base(name), ".pdf")
		if sent := learning.sent(keys[name]); len(sent) != 1 ||
			sent[0] != (standInCreate{Title: stem, ResourceType: "book", BookCopy: "pdf", FilePath: name, SourceKey: keys[name]}) {
			t.Errorf("learning was sent %+v for %s, want one create of book %q at %s", sent, name, stem, name)
		}
	}
	if shown := p.shown(t, keys["notes.md"]); len(shown.Deliveries) != 0 || shown.Undelivered == nil ||
		shown.Undelivered.Reason != connectors.UndeliveredNotTaken {
		t.Errorf("notes.md holds deliveries %s and undelivered %s, want none and not-taken", jsonOf(shown.Deliveries), jsonOf(shown.Undelivered))
	}
	if sent := learning.sent(keys["notes.md"]); len(sent) != 0 {
		t.Errorf("learning was sent %+v for notes.md, want nothing", sent)
	}

	if err := os.MkdirAll(filepath.Join(books, "moved"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(books, "flaky.pdf"), filepath.Join(books, "moved", "renamed.pdf")); err != nil {
		t.Fatal(err)
	}
	scanFolder()
	p.advance(time.Hour)
	if ran := p.run(t, learningResources); ran != 1 {
		t.Fatalf("after flaky.pdf moved and its backoff passed, the worker ran %d deliveries, want its retry", ran)
	}
	if sent := learning.sent(keys["flaky.pdf"]); len(sent) != 2 || sent[1] != sent[0] || sent[1].FilePath != "flaky.pdf" || sent[1].Title != "flaky" {
		t.Errorf("learning was sent %+v for flaky.pdf, want its retry to repeat the first request at flaky.pdf", sent)
	}
	if d := p.shown(t, keys["flaky.pdf"]).Deliveries; len(d) != 1 || d[0].Outcome != store.DeliveryDelivered || text(d[0].DestinationID) != "5" {
		t.Errorf("flaky.pdf holds deliveries %s after its retry, want delivered as 5", jsonOf(d))
	}
}

// Each chapter of a book placed at learning is created there as a chapter part
// of its book's resource, with its PDF and printed pages, once the book's
// delivery has settled, and a book's chapters arrive in their order. A book
// that learning fails first holds its chapters back, and the write that
// delivers it queues them. A refused book delivers no chapter. A book that was
// delivered while nothing queued chapters has its chapters queued when serve
// starts. A chapter that learning fails first sends its first body again. A
// replay of every delivery then queues nothing.
func TestEachChapterEntersLearningPartOfItsBookOnceTheBookIsThere(t *testing.T) {
	ctx := t.Context()
	dir, books := t.TempDir(), t.TempDir()
	script := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\ncat >/dev/null\n"+body), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	// Eleven one-page chapters, so their order as numbers differs from their
	// keys' order as text. Chapter n is PDF page n+1, printed as page n. The
	// index's outline title is padded with NULs, as some PDF writers pad one.
	const chapters = 11
	units := []string{`'{"label":"i","text":"Contents"}'`}
	outline := []string{`{"level":1,"title":"Contents","page":1}`}
	for n := 1; n <= chapters; n++ {
		units = append(units, fmt.Sprintf(`'{"label":"%d","text":"Page %d"}'`, n, n))
		outline = append(outline, fmt.Sprintf(`{"level":1,"title":"Chapter %d","page":%d}`, n, n+1))
	}
	units = append(units, fmt.Sprintf(`'{"label":"%d","text":"Index"}'`, chapters+1))
	outline = append(outline, fmt.Sprintf(`{"level":1,"title":"Index\u0000\u0000","page":%d}`, chapters+2))
	pdfText := extract.Program(extract.PDFText.Name, extract.PDFText.Version, extract.MediaTypePDF,
		script("pdf-text", `printf '%s\n' `+strings.Join(units, " ")+"\n"))
	pdfText.Positional = true
	extractors, err := extract.NewRegistry(pdfText)
	if err != nil {
		t.Fatal(err)
	}
	pdfStructure := script("pdf-structure", fmt.Sprintf(`printf '%%s\n' '{"pages":%d,"outline":[%s],"page_labels":[],`+
		`"font_shares":{"sampled_pages":2,"monospace":0,"tex_or_math":0}}'`+"\n", chapters+2, strings.Join(outline, ",")))
	keys := map[string]string{}
	for name, body := range map[string]string{
		"early.pdf": "a book delivered before chapters were", "flaky.pdf": "a book learning fails once", "series/part-1.pdf": "a part of a series",
	} {
		path := filepath.Join(books, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(body))
		if keys[name], err = itemkey.FromFileSHA256(hex.EncodeToString(sum[:])); err != nil {
			t.Fatal(err)
		}
	}
	early, flaky, refused := keys["early.pdf"], keys["flaky.pdf"], keys["series/part-1.pdf"]

	// Until chaptersQueue is set, a delivery's end queues no chapter, as in a
	// release whose deliverer wrote none.
	var chaptersQueue atomic.Bool
	after := afterStage(quiet)
	p := newPipelineOver(t, extractors, func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.Stage == store.StageDeliver && !chaptersQueue.Load() {
			return nil
		}
		return after(ctx, tx, task)
	}, map[string]string{"books": books})
	declarations := lifeSet(t, withLife+"  - name: learning\n    side: personal\n    tags: [book]\n    deliverer: learning-resources\n")
	p.apply(t, declarations)
	learning := newStandInLearning(t, p.provider)
	learning.hold("series", false)
	learning.failing[flaky], learning.failing[flaky+"#2"] = 1, 1
	cfg := fileConfig{Deliver: map[string]deliverTable{deliver.LearningResources: {
		serviceClient: serviceClient{API: learning.URL, ClientID: learningClient}, Root: "books",
	}}}
	cfg.Serve.Homes = map[string]string{"books": books}
	if err := setDelivererRoots(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	handlers := workerHandlers(p.worker, openDeliverers(ctx, cfg, p.provider.URL, quiet), openedSources{}, nil, nil, nil, modelConfig{}, quiet)
	learningResources := handlers[slices.IndexFunc(handlers, func(h worker.Handler) bool {
		return h.Capability() == store.DeliverCapability(deliver.LearningResources)
	})]
	set, err := connectors.Read(declarations, nil)
	if err != nil {
		t.Fatal(err)
	}
	folder, _ := set.Connector("books-folder")
	if _, err := connectors.ScanFolder(ctx, p.lander, folder, time.Now().Add(time.Minute), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	p.parse(t, extractors)
	if n := p.run(t, sections.NewHandler(pdfStructure, p.worker, quiet)); n != 3 {
		t.Fatalf("ran %d structure tasks, want one for each book", n)
	}
	p.scanFindingNothing(t)

	if ran := p.run(t, learningResources); ran != 3 {
		t.Fatalf("the worker ran %d deliveries, want the three books' and no chapter's", ran)
	}
	for key, reason := range map[string]string{flaky + "#1": connectors.UndeliveredBookUnsettled, refused + "#1": connectors.UndeliveredBookRefused} {
		if u := p.shown(t, key).Undelivered; u == nil || u.Reason != reason {
			t.Errorf("%s waits undelivered %s, want %s", key, jsonOf(u), reason)
		}
	}

	chaptersQueue.Store(true)
	queueUndeliveredChapters(ctx, p.st, quiet)
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND state = ? AND item_key LIKE ?`,
		store.StageDeliver, store.TaskQueued, early+"#%"); n != chapters {
		t.Fatalf("serve's start queued %d of the delivered book's chapters, want all %d", n, chapters)
	}
	p.advance(time.Hour)
	if ran := p.run(t, learningResources); ran != 1+2*chapters {
		t.Fatalf("after the backoff, the worker ran %d deliveries, want the book's retry and both books' %d chapters", ran, chapters)
	}
	p.advance(time.Hour)
	if ran := p.run(t, learningResources); ran != 1 {
		t.Fatalf("after its backoff, the worker ran %d deliveries, want the second chapter's retry", ran)
	}
	for _, key := range []string{early, flaky} {
		book, err := strconv.ParseInt(text(p.shown(t, key).Deliveries[0].DestinationID), 10, 64)
		if err != nil {
			t.Fatalf("the book's delivery names learning's resource %v", err)
		}
		for n := 1; n <= chapters; n++ {
			chapter := fmt.Sprintf("%s#%d", key, n)
			want := standInCreate{
				Title: fmt.Sprintf("Chapter %d", n), ResourceType: "chapter", SourceKey: chapter, PartOf: book,
				PDFPageStart: n + 1, PDFPageEnd: n + 1, PrintedPageStart: fmt.Sprint(n), PrintedPageEnd: fmt.Sprint(n),
			}
			attempts := 1
			if chapter == flaky+"#2" {
				attempts = 2
			}
			sent := learning.sent(chapter)
			if len(sent) != attempts || slices.ContainsFunc(sent, func(c standInCreate) bool { return c != want }) {
				t.Errorf("learning was sent %+v for %s, want each attempt to create %+v", sent, chapter, want)
			}
			if d := p.shown(t, chapter).Deliveries; len(d) != 1 || d[0].Outcome != store.DeliveryDelivered {
				t.Errorf("%s holds deliveries %s, want one delivered", chapter, jsonOf(d))
			}
		}
		if order := learning.firstCreates(key + "#"); !slices.IsSortedFunc(order, func(a, b standInCreate) int { return a.PDFPageStart - b.PDFPageStart }) {
			t.Errorf("learning first heard of %s's chapters in the order %v, want the book's order", key, titles(order))
		}
	}
	if sent := learning.sent(refused + "#1"); len(sent) != 0 {
		t.Errorf("learning was sent %+v for a chapter of the refused book, want nothing", sent)
	}
	if queued := p.replayDeliveries(t); queued != 0 {
		t.Errorf("a replay of every delivery queued %d, want none, as each is settled or waits on a refused book", queued)
	}
}

// firstCreates is the first create of each source_key starting prefix, in the
// order they came.
func (l *standInLearning) firstCreates(prefix string) []standInCreate {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []standInCreate
	seen := map[string]bool{}
	for _, c := range l.creates {
		if strings.HasPrefix(c.SourceKey, prefix) && !seen[c.SourceKey] {
			seen[c.SourceKey] = true
			out = append(out, c)
		}
	}
	return out
}

func titles(creates []standInCreate) []string {
	out := make([]string, len(creates))
	for i, c := range creates {
		out[i] = c.Title
	}
	return out
}
