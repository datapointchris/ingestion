package landing_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/store"
)

// RedditExportsEnv names a directory of stored Reddit exports, one JSON file
// each, for the test that lands them. Without it the test skips.
const RedditExportsEnv = "INGESTION_REDDIT_EXPORTS_DIR"

func newLander(t *testing.T, homes map[string]string) (*landing.Lander, *store.Store, *landing.Blobs) {
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
	l, err := landing.New(landing.Config{Store: st, Blobs: blobs, Homes: homes})
	if err != nil {
		t.Fatal(err)
	}
	return l, st, blobs
}

// One page from a feed and from the phone is one item with two sightings and
// one blob. A landing that fails after its bytes are written leaves bytes with
// no row, never a row with no bytes, and the unpaired report names both kinds
// of disagreement.
func TestOneArtifactFromTwoConnectorsMakesOneBlobOneItemAndTwoSightings(t *testing.T) {
	ctx := t.Context()
	l, st, blobs := newLander(t, nil)
	key, err := itemkey.FromURL("https://example.com/a?utm_source=feed")
	if err != nil {
		t.Fatal(err)
	}
	for _, connector := range []string{"freshrss", "phone"} {
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: connector, Zone: store.Outside, Key: key, MediaType: "text/html",
			URL: "https://EXAMPLE.com/a?utm_source=" + connector, Body: strings.NewReader("<p>one page</p>"),
		})
		if err != nil || !landed.SightingAdded {
			t.Fatalf("%s's arrival: %+v, %v", connector, landed, err)
		}
	}
	records, err := st.Item(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(records.Sightings) != 2 || records.Blob == nil || *records.Item.URL != "https://example.com/a" {
		t.Errorf("the item holds %d sightings, blob %v and URL %v; want 2, one blob and the canonical URL",
			len(records.Sightings), records.Blob, *records.Item.URL)
	}
	u, err := l.Unpaired(ctx)
	if err != nil || u.Files != 1 || u.Rows != 1 || len(u.Unrowed)+len(u.Missing) != 0 {
		t.Fatalf("after two arrivals the volume reads %+v, %v; want one file under one row", u, err)
	}

	memo, _ := itemkey.FromMemo("ingestion-cli-desk", "k1")
	_, err = l.Land(ctx, landing.Arrival{
		Connector: "phone", Zone: store.Owner, Key: memo, MediaType: "text/plain",
		Parent: "url/ffffffffffffffff", Body: strings.NewReader("typed text"),
	})
	if !errors.Is(err, store.ErrNoItem) {
		t.Fatalf("an arrival naming a missing parent answered %v, want ErrNoItem", err)
	}
	if err := os.Remove(blobs.Path(*records.Item.BlobSHA256)); err != nil {
		t.Fatal(err)
	}
	u, err = l.Unpaired(ctx)
	if err != nil {
		t.Fatal(err)
	}
	typed, _ := landing.Hash(strings.NewReader("typed text"))
	if len(u.Unrowed) != 1 || u.Unrowed[0].SHA256 != typed.SHA256 {
		t.Errorf("the report names %+v as bytes with no row, want the failed landing's", u.Unrowed)
	}
	if len(u.Missing) != 1 || u.Missing[0].SHA256 != *records.Item.BlobSHA256 {
		t.Errorf("the report names %+v as rows with no bytes, want the page's", u.Missing)
	}
}

// A shared link lands outside, typed text lands owner, and a child takes the
// stricter of its own zone and its parent's. An owner item that an outside
// connector sights again becomes outside.
func TestTheZoneRecordsWhoWroteTheContentAndAChildTakesTheStricter(t *testing.T) {
	ctx := t.Context()
	l, st, _ := newLander(t, nil)
	link, _ := itemkey.FromURL("https://example.com/shared")
	typed, _ := itemkey.FromMemo("ingestion-svc-phone", "m1")
	linkChild, _ := itemkey.Child(link, 1)
	typedChild, _ := itemkey.Child(typed, 1)
	arrivals := []landing.Arrival{
		{Connector: "phone", Zone: store.Outside, Key: link, MediaType: "text/uri-list", URL: "https://example.com/shared"},
		{Connector: "phone", Zone: store.Owner, Key: typed, MediaType: "text/plain", Body: strings.NewReader("my note")},
		{Connector: "phone", Zone: store.Owner, Key: linkChild, MediaType: "text/plain", Parent: link, Body: strings.NewReader("a")},
		{Connector: "phone", Zone: store.Owner, Key: typedChild, MediaType: "text/plain", Parent: typed, Body: strings.NewReader("b")},
	}
	for _, a := range arrivals {
		if _, err := l.Land(ctx, a); err != nil {
			t.Fatalf("land %s: %v", a.Key, err)
		}
	}
	want := map[string]store.Zone{link: store.Outside, typed: store.Owner, linkChild: store.Outside, typedChild: store.Owner}
	for key, zone := range want {
		if r, err := st.Item(ctx, key); err != nil || r.Item.Zone != zone {
			t.Errorf("%s landed %q, %v; want %q", key, r.Item.Zone, err, zone)
		}
	}
	if _, err := l.Land(ctx, landing.Arrival{Connector: "freshrss", Zone: store.Outside, Key: typed, MediaType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if r, _ := st.Item(ctx, typed); r.Item.Zone != store.Outside {
		t.Errorf("an owner item sighted outside reads %q, want outside", r.Item.Zone)
	}
}

// A book in a copy of the books folder lands by reference: its bytes are
// hashed where they lie, its home is books:<path>, and the volume gains
// nothing. A rerun writes nothing, and a path that leaves the root is refused.
func TestABookLandsByReferenceAndARerunLandsNothing(t *testing.T) {
	ctx := t.Context()
	books := t.TempDir()
	pdf := []byte("%PDF-1.7 a book")
	if err := os.MkdirAll(filepath.Join(books, "Some Author"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(books, "Some Author", "A Book.pdf"), pdf, 0o600); err != nil {
		t.Fatal(err)
	}
	l, st, _ := newLander(t, map[string]string{"books": books})
	book := landing.Arrival{
		Connector: "books-folder", Zone: store.Outside, MediaType: "application/pdf",
		Home: &landing.Home{Root: "books", Path: "Some Author/A Book.pdf"},
	}
	first, err := l.Land(ctx, book)
	if err != nil || !first.ItemAdded || !first.HomeAdded || !strings.HasPrefix(first.Key, "file/") {
		t.Fatalf("the book landed %+v, %v", first, err)
	}
	records, _ := st.Item(ctx, first.Key)
	if len(records.Homes) != 1 || records.Homes[0].Root+":"+records.Homes[0].Path != "books:Some Author/A Book.pdf" || records.Blob == nil {
		t.Errorf("the book's homes are %+v and its blob %v, want books:Some Author/A Book.pdf beside its MD5", records.Homes, records.Blob)
	}
	u, err := l.Unpaired(ctx)
	if err != nil || u.Files != 0 || len(u.Missing) != 0 {
		t.Errorf("the volume reads %+v, %v; want no file and no row missing its bytes", u, err)
	}
	f, _, err := l.Open(ctx, first.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	served, _ := io.ReadAll(f)
	_ = f.Close()
	if !bytes.Equal(served, pdf) {
		t.Errorf("the blob served %d bytes from its home, want the book's %d", len(served), len(pdf))
	}

	again, err := l.Land(ctx, book)
	if err != nil || again.Changed || again.Key != first.Key {
		t.Errorf("a rerun landed %+v, %v; want nothing written under %s", again, err, first.Key)
	}
	if after, _ := st.Item(ctx, first.Key); after.Item.UpdatedTS != records.Item.UpdatedTS || after.Sightings[0].LastSeenTS != records.Sightings[0].LastSeenTS {
		t.Error("a rerun rewrote the item or its sighting")
	}
	for _, path := range []string{"../outside.pdf", "/etc/passwd", "Some Author/../A Book.pdf"} {
		escape := book
		escape.Home = &landing.Home{Root: "books", Path: path}
		if _, err := l.Land(ctx, escape); err == nil {
			t.Errorf("a home at %q landed, want it refused", path)
		}
	}
}

// The stored Reddit exports land as one item and one blob per distinct save
// under reddit/<fullname>, each save a child of the export that carried it
// first, and every blob row's bytes are on disk. The exports are outsider text
// in a public repo, so the test reads them only from RedditExportsEnv and
// reports counts alone.
func TestTheRealRedditExportsLandOneBlobPerSave(t *testing.T) {
	dir := os.Getenv(RedditExportsEnv)
	if dir == "" {
		t.Skip(RedditExportsEnv + " names no directory of stored Reddit exports")
	}
	ctx := t.Context()
	l, st, blobs := newLander(t, nil)
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s holds no JSON files: %v", dir, err)
	}
	saves, added := 0, 0
	fullnames := map[string]bool{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		export, err := redditexport.Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", filepath.Base(path), err)
		}
		exported, err := l.Land(ctx, landing.Arrival{
			Connector: "reddit-exports", Zone: store.Outside, MediaType: redditexport.MediaTypeExport,
			ExternalKey: strings.TrimSuffix(filepath.Base(path), ".json"), Body: bytes.NewReader(raw),
		})
		if err != nil {
			t.Fatalf("%s: %v", filepath.Base(path), err)
		}
		split, err := export.Saves()
		if err != nil {
			t.Fatalf("%s: %v", filepath.Base(path), err)
		}
		for _, s := range split {
			key, err := s.Key()
			if err != nil {
				t.Fatalf("%s: a save's key: %v", filepath.Base(path), err)
			}
			landed, err := l.Land(ctx, landing.Arrival{
				Connector: "reddit-exports", Zone: store.Outside, Key: key, MediaType: redditexport.MediaTypeSave,
				ExternalKey: s.Fullname, CursorPosition: exported.Key, Title: s.Title, URL: s.URL,
				Parent: exported.Key, Body: bytes.NewReader(s.Bytes),
			})
			if err != nil {
				t.Fatalf("land %s: %v", key, err)
			}
			saves++
			fullnames[s.Fullname] = true
			if landed.ItemAdded {
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
	if items != len(fullnames) || added != items || withBytes != items || children != items {
		t.Errorf("%d saves in %d exports made %d reddit items (%d added), %d with a blob row and %d with a parent; want %d each",
			saves, len(files), items, added, withBytes, children, len(fullnames))
	}
	for key := range fullnames {
		r, err := st.Item(ctx, "reddit/"+key)
		if err != nil || r.Item.BlobSHA256 == nil || !blobs.Has(*r.Item.BlobSHA256) {
			t.Errorf("reddit/%s holds no bytes on disk: %v", key, err)
		}
	}
	u, err := l.Unpaired(ctx)
	if err != nil || len(u.Unrowed)+len(u.Missing)+len(u.Leftovers) != 0 {
		t.Errorf("the volume and the rows disagree: %d unrowed, %d missing, %d leftovers, %v",
			len(u.Unrowed), len(u.Missing), len(u.Leftovers), err)
	}
	t.Logf("%d exports, %d saves, %d reddit items, %d blobs in all (%d files), %d sightings",
		len(files), saves, items, u.Rows, u.Files, count(`SELECT count(*) FROM sightings`))
}
