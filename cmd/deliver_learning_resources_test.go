package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
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
// source_key otherwise or where that resource is deleted. A path that one live
// resource holds exactly under no key is adopted. A path held under another
// key, inside a held directory, by two resources, or by deleted ones alone
// answers 409 file_path naming its holders.
type standInLearning struct {
	*httptest.Server
	mu        sync.Mutex
	resources []*standInResource
	// creates holds each create's body in the order it came.
	creates []standInCreate
	// failing counts the creates of each file_path that answer 500 first.
	failing map[string]int
}

type standInCreate struct {
	Title        string `json:"title"`
	ResourceType string `json:"resource_type"`
	BookCopy     string `json:"book_copy"`
	FilePath     string `json:"file_path"`
	SourceKey    string `json:"source_key"`
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
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Title == "" || body.FilePath == "" || body.SourceKey == "" {
			writeLearning(w, http.StatusBadRequest, map[string]any{"error": "Bad Request", "message": "title is required"})
			return
		}
		if body.ResourceType != "book" {
			writeLearning(w, http.StatusUnprocessableEntity, map[string]any{
				"error": "Unprocessable Entity", "message": "resource_type names no resource type", "field": "resource_type", "known": []string{"book"},
			})
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.creates = append(l.creates, body)
		if l.failing[body.FilePath] > 0 {
			l.failing[body.FilePath]--
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
	learning.failing["flaky.pdf"] = 1

	cfg := fileConfig{Deliver: map[string]deliverTable{deliver.LearningResources: {
		serviceClient: serviceClient{API: learning.URL, ClientID: learningClient}, Root: "books",
	}}}
	cfg.Serve.Homes = map[string]string{"books": books}
	if err := setDelivererRoots(cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv(deliverSecretEnv(deliver.LearningResources), learningSecret)
	handlers := workerHandlers(p.worker, openDeliverers(ctx, cfg, p.provider.URL, quiet), nil, nil, modelConfig{}, quiet)
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
		shown.Undelivered.Reason != connectors.UndeliveredNotTaken || !strings.Contains(shown.Undelivered.Message, "not a PDF") {
		t.Errorf("notes.md holds deliveries %s and undelivered %s, want none and not-taken as no PDF", jsonOf(shown.Deliveries), jsonOf(shown.Undelivered))
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
