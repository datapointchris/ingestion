package redditexport_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/store"
)

// RedditExportsEnv names a directory of stored Reddit exports, one JSON file
// each, for the test that lands them. Without it the test skips.
const RedditExportsEnv = "INGESTION_REDDIT_EXPORTS_DIR"

func post(name string) string {
	return `{"kind":"t3","data":{"name":"` + name + `","subreddit":"golang","title":"A post","permalink":"/r/golang/comments/` + name[3:] + `/a_post/"}}`
}

func thread(name string) string {
	return `[{"data":{"children":[` + post(name) + `]}},{"data":{"children":[]}}]`
}

func newLander(t *testing.T) (*landing.Lander, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(t.Context(), filepath.Join(dir, "ingestion.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	blobs, err := landing.OpenBlobs(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	l, err := landing.New(landing.Config{Store: st, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	return l, st
}

// Each bookmarklet mode splits into one save per post it carries, each with
// its own thread or the reason it has none, so a top export and a thread
// export land as a saved one does.
func TestEveryExportKindSplitsIntoOneSavePerPostWithItsThread(t *testing.T) {
	exports := map[string]string{
		redditexport.KindSaved: `{"kind":"reddit-saved","exported_at":"2026-10-30T19:18:39Z","items":[` +
			`{"child":` + post("t3_aa1") + `,"thread":` + thread("t3_aa1") + `},` +
			`{"child":{"kind":"t1","data":{"name":"t1_cc3","subreddit":"golang","link_title":"A post","permalink":"/r/golang/comments/aa1/a_post/cc3/"}},"thread":null,"thread_error":"403"}]}`,
		redditexport.KindTop: `{"kind":"reddit-top","exported_at":"2026-10-30T19:18:39Z","period":"month","subreddits":[{"name":"golang","limit":3,` +
			`"listing":{"data":{"children":[` + post("t3_bb1") + `,` + post("t3_bb2") + `,` + post("t3_bb3") + `]}},` +
			`"threads":{"t3_bb1":` + thread("t3_bb1") + `,"t3_bb2":{"error":"429"}}}]}`,
		redditexport.KindThread: `{"kind":"reddit-thread","exported_at":"2026-10-30T19:18:39Z","fullname":"t3_dd1","listing":` + thread("t3_dd1") + `}`,
	}
	want := map[string][]string{
		redditexport.KindSaved:  {"reddit/t3_aa1", "reddit/t1_cc3"},
		redditexport.KindTop:    {"reddit/t3_bb1", "reddit/t3_bb2", "reddit/t3_bb3"},
		redditexport.KindThread: {"reddit/t3_dd1"},
	}
	for kind, raw := range exports {
		e, err := redditexport.Parse([]byte(raw))
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		saves, err := e.Saves()
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		var keys []string
		for _, s := range saves {
			key, err := s.Key()
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
			var body struct {
				Child       json.RawMessage `json:"child"`
				Thread      json.RawMessage `json:"thread"`
				ThreadError string          `json:"thread_error"`
			}
			if err := json.Unmarshal(s.Bytes, &body); err != nil || len(body.Child) == 0 {
				t.Errorf("%s's save %s is not a child and its thread: %v", kind, key, err)
			}
			if refused := body.ThreadError != ""; refused == (len(body.Thread) > 0 && string(body.Thread) != "null") {
				t.Errorf("%s's save %s holds thread %q and thread error %q, want one", kind, key, body.Thread, body.ThreadError)
			}
		}
		if !slices.Equal(keys, want[kind]) {
			t.Errorf("%s split into %v, want %v", kind, keys, want[kind])
		}
	}
	if _, err := redditexport.Parse([]byte(`{"kind":"pocket","exported_at":"2026-10-30T19:18:39Z"}`)); err == nil {
		t.Error("a file of another kind parsed as a Reddit export")
	}
}

// A save that two exports carry lands once, as a child of the first export and
// with the first export's bytes. Each export is an item of its own, and every
// blob row's bytes are on disk.
func TestASaveInTwoExportsLandsOnceUnderTheFirst(t *testing.T) {
	ctx := t.Context()
	l, st := newLander(t)
	export := func(thread string) []byte {
		return []byte(`{"kind":"reddit-saved","exported_at":"2026-10-30T19:18:39Z","items":[{"child":` + post("t3_aa1") + `,"thread":` + thread + `}]}`)
	}
	first, err := redditexport.Land(ctx, l, redditexport.Drop{Connector: "reddit-exports", Name: "first", Raw: export(thread("t3_aa1"))})
	if err != nil || len(first.Saves) != 1 || !first.Saves[0].ItemAdded {
		t.Fatalf("the first export landed %+v, %v", first, err)
	}
	second, err := redditexport.Land(ctx, l, redditexport.Drop{Connector: "reddit-exports", Name: "second", Raw: export("null")})
	if err != nil || len(second.Saves) != 1 || second.Saves[0].ItemAdded || second.Export.Key == first.Export.Key {
		t.Fatalf("the second export landed %+v, %v; want its own item and no new save", second, err)
	}
	r, err := st.Item(ctx, "reddit/t3_aa1")
	if err != nil {
		t.Fatal(err)
	}
	if *r.Item.ParentKey != first.Export.Key || *r.Item.BlobSHA256 != first.Saves[0].SHA256 {
		t.Errorf("the save's parent is %s and its bytes %s; want the first export %s and its bytes %s",
			*r.Item.ParentKey, *r.Item.BlobSHA256, first.Export.Key, first.Saves[0].SHA256)
	}
	u, err := l.Unpaired(ctx)
	if err != nil || u.Files != 3 || u.Rows != 3 || len(u.Unrowed)+len(u.Missing)+len(u.Leftovers) != 0 {
		t.Errorf("the volume reads %+v, %v; want the two exports and one save, each a file under its row", u, err)
	}
}

// The stored Reddit exports land as one item and one blob per distinct save
// under reddit/<fullname>, each save a child of the export that carried it
// first, and every blob row's bytes are on disk. The exports are outsider text
// kept out of this public repo, so the test reads them only from
// RedditExportsEnv, and it reports counts and a failure's class alone.
func TestTheStoredRedditExportsLandOneItemAndOneBlobPerSave(t *testing.T) {
	dir := os.Getenv(RedditExportsEnv)
	if dir == "" {
		t.Skip(RedditExportsEnv + " names no directory of stored Reddit exports")
	}
	ctx := t.Context()
	l, st := newLander(t)
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s holds no JSON files: %v", dir, err)
	}
	saves, added := 0, 0
	keys := map[string]bool{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		landed, err := redditexport.Land(ctx, l, redditexport.Drop{
			Connector: "reddit-exports", Name: strings.TrimSuffix(filepath.Base(path), ".json"), Raw: raw,
		})
		if err != nil {
			t.Fatalf("%s failed after %d saves: key mismatch %t, missing parent %t",
				filepath.Base(path), len(landed.Saves), errors.Is(err, landing.ErrKeyMismatch), errors.Is(err, store.ErrNoItem))
		}
		for _, s := range landed.Saves {
			saves++
			keys[s.Key] = true
			if s.ItemAdded {
				added++
			}
		}
	}
	count := func(query string) int {
		var n int
		if err := st.Reader().QueryRowContext(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	items := count(`SELECT count(*) FROM items WHERE key LIKE 'reddit/%'`)
	withBytes := count(`SELECT count(*) FROM items JOIN blobs ON blobs.sha256 = items.blob_sha256 WHERE key LIKE 'reddit/%'`)
	children := count(`SELECT count(*) FROM items WHERE key LIKE 'reddit/%' AND parent_key LIKE 'file/%'`)
	if items != len(keys) || added != items || withBytes != items || children != items {
		t.Errorf("%d saves in %d exports made %d reddit items (%d added), %d with a blob row and %d with a parent; want %d each",
			saves, len(files), items, added, withBytes, children, len(keys))
	}
	u, err := l.Unpaired(ctx)
	if err != nil || u.Files != u.Rows || len(u.Unrowed)+len(u.Missing)+len(u.Leftovers) != 0 {
		t.Errorf("the volume and the rows disagree: %d files, %d rows, %d unrowed, %d missing, %d leftovers, %v",
			u.Files, u.Rows, len(u.Unrowed), len(u.Missing), len(u.Leftovers), err)
	}
	t.Logf("%d exports, %d saves, %d reddit items, %d blobs in all (%d files), %d sightings",
		len(files), saves, items, u.Rows, u.Files, count(`SELECT count(*) FROM sightings`))
}
