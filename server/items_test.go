package server_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/goapiauth/authtest"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// A worker writes bytes under their SHA-256 and reads them back, from the
// volume or from a book's home, and bytes sent under another SHA-256 are
// refused. A person pages through the items, shows one by its key or its URL,
// and reads the unpaired report, each answer holding to its schema. A worker's
// token reaches no item.
func TestAWorkerMovesBytesThroughTheBlobRoutesAndAPersonReadsItems(t *testing.T) {
	ctx := t.Context()
	dir, books := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(books, "a.pdf"), []byte("%PDF a book"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, filepath.Join(dir, "ingestion.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	blobs, err := landing.OpenBlobs(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	lander, err := landing.New(landing.Config{Store: st, Blobs: blobs, Homes: map[string]string{"books": books}})
	if err != nil {
		t.Fatal(err)
	}
	link := "https://example.com/post?utm_source=rss"
	page, _ := itemkey.FromURL(link)
	if _, err := lander.Land(ctx, landing.Arrival{
		Connector: "freshrss", Zone: store.Outside, Key: page, MediaType: "text/html", URL: link, Body: strings.NewReader("<p>page</p>"),
	}); err != nil {
		t.Fatal(err)
	}
	book, err := lander.Land(ctx, landing.Arrival{
		Connector: "books-folder", Zone: store.Outside, MediaType: "application/pdf", Home: &landing.Home{Root: "books", Path: "a.pdf"},
	})
	if err != nil {
		t.Fatal(err)
	}

	provider := authtest.NewProvider(t)
	provider.Register("ingestion-svc-worker", "worker-secret", server.ScopeTasksClaim)
	gate := readyGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
		server.BlobRoutes(lander, log), server.ItemRoutes(st, log)))
	t.Cleanup(api.Close)
	service := (&clientcredentials.Config{
		ClientID: "ingestion-svc-worker", ClientSecret: "worker-secret", TokenURL: provider.URL + "/token",
		Scopes: []string{server.ScopeTasksClaim},
	}).Client(ctx)
	w := worker.NewClient(api.URL, service)

	text := []byte("the page's text")
	sum, _ := landing.Hash(bytes.NewReader(text))
	first, err := w.PutBlob(ctx, sum.SHA256, "text/plain; charset=utf-8", bytes.NewReader(text))
	if err != nil || first.MD5 != sum.MD5 || first.SizeBytes != int64(len(text)) {
		t.Fatalf("a worker's write answered %+v, %v", first, err)
	}
	if again, err := w.PutBlob(ctx, sum.SHA256, "text/plain", bytes.NewReader(text)); err != nil || again != first {
		t.Errorf("the same bytes written again answered %+v, %v; want the row the first write made", again, err)
	}
	var refused *worker.StatusError
	if _, err := w.PutBlob(ctx, sum.SHA256, "text/plain", strings.NewReader("other bytes")); !errors.As(err, &refused) || refused.Code != server.CodeDigestMismatch {
		t.Errorf("bytes under another SHA-256 answered %v, want %s", err, server.CodeDigestMismatch)
	}
	for want, name := range map[string]string{string(text): sum.SHA256, "%PDF a book": book.SHA256} {
		body, mediaType, err := w.Blob(ctx, name)
		if err != nil {
			t.Fatalf("read blob %s: %v", name, err)
		}
		got, _ := io.ReadAll(body)
		_ = body.Close()
		if string(got) != want || mediaType == "" {
			t.Errorf("blob %s read %d bytes as %q, want %d", name, len(got), mediaType, len(want))
		}
	}

	person := authtest.Bearer(provider.Person(t, "ingestion-cli-desk", "person"))
	get := func(client *http.Client, route string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, api.URL+route, http.NoBody)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	schemas := compileAnswerSchemas(t)
	var keys []string
	route := "/api/v1/items?limit=1"
	for range 3 {
		status, raw := get(person, route)
		if status != http.StatusOK {
			t.Fatalf("GET %s answered %d: %s", route, status, raw)
		}
		validate(t, schemas, "items-list", raw)
		var listed server.ItemsAnswer
		_ = json.Unmarshal(raw, &listed)
		for _, it := range listed.Items {
			keys = append(keys, it.Key)
		}
		if listed.Next == nil {
			break
		}
		route = "/api/v1/items?limit=1&after=" + url.QueryEscape(*listed.Next)
	}
	if len(keys) != 2 || keys[0] != book.Key || keys[1] != page {
		t.Errorf("the pages listed %v, want the book then the page", keys)
	}
	for _, named := range []string{page, link} {
		status, raw := get(person, "/api/v1/items/"+url.PathEscape(named))
		if status != http.StatusOK {
			t.Fatalf("show %s answered %d: %s", named, status, raw)
		}
		validate(t, schemas, "item", raw)
		var shown server.ItemAnswer
		_ = json.Unmarshal(raw, &shown)
		if shown.Item.Key != page || shown.Item.Phase != "landed" || len(shown.Sightings) != 1 {
			t.Errorf("show %s answered %s %s with %d sightings", named, shown.Item.Key, shown.Item.Phase, len(shown.Sightings))
		}
	}
	status, raw := get(person, "/api/v1/blobs/unpaired")
	validate(t, schemas, "blobs-unpaired", raw)
	var unpaired server.UnpairedAnswer
	_ = json.Unmarshal(raw, &unpaired)
	if status != http.StatusOK || unpaired.Files != 2 || unpaired.Rows != 3 || len(unpaired.Unrowed)+len(unpaired.Missing) != 0 {
		t.Errorf("the unpaired report answered %d: %+v; want 2 files, 3 rows and nothing unpaired", status, unpaired)
	}
	if status, _ := get(service, "/api/v1/items"); status != http.StatusForbidden {
		t.Errorf("a worker's token listing items answered %d, want 403", status)
	}
}

// compileAnswerSchemas is every file in schemas/, by its name without
// .schema.json, each able to reach the others through its $id.
func compileAnswerSchemas(t *testing.T) map[string]*jsonschema.Schema {
	t.Helper()
	files, err := filepath.Glob(mustAbs(t, "../schemas/*.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	ids := map[string]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		id := doc.(map[string]any)["$id"].(string)
		if err := c.AddResource(id, doc); err != nil {
			t.Fatal(err)
		}
		ids[strings.TrimSuffix(filepath.Base(f), ".schema.json")] = id
	}
	out := map[string]*jsonschema.Schema{}
	for name, id := range ids {
		if out[name], err = c.Compile(id); err != nil {
			t.Fatalf("compile %s: %v", name, err)
		}
	}
	return out
}

func validate(t *testing.T, schemas map[string]*jsonschema.Schema, name string, raw []byte) {
	t.Helper()
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("%s answered no JSON: %v", name, err)
	}
	if err := schemas[name].Validate(instance); err != nil {
		t.Errorf("the answer does not hold to %s.schema.json: %v", name, err)
	}
}
