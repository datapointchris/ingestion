package landing_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

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
// one blob, whether an arrival names the key or leaves Land to derive it from
// the URL. A key that the URL does not derive is refused. A landing that fails
// after its bytes are written leaves bytes with no row, never a row with no
// bytes, and the unpaired report names both kinds of disagreement.
func TestOneArtifactFromTwoConnectorsMakesOneBlobOneItemAndTwoSightings(t *testing.T) {
	ctx := t.Context()
	l, st, blobs := newLander(t, nil)
	key, err := itemkey.FromURL("https://example.com/a?utm_source=feed")
	if err != nil {
		t.Fatal(err)
	}
	for connector, named := range map[string]string{"freshrss": "", "phone": key} {
		landed, err := l.Land(ctx, landing.Arrival{
			Connector: connector, Zone: store.Outside, Key: named, MediaType: "text/html",
			URL: "https://EXAMPLE.com/a?utm_source=" + connector, Body: strings.NewReader("<p>one page</p>"),
		})
		if err != nil || !landed.SightingAdded || landed.Key != key {
			t.Fatalf("%s's arrival: %+v, %v; want a sighting of %s", connector, landed, err, key)
		}
	}
	_, err = l.Land(ctx, landing.Arrival{
		Connector: "phone", Zone: store.Outside, Key: "youtube/dQw4w9WgXcQ", MediaType: "text/html", URL: "https://example.com/other",
	})
	if !errors.Is(err, landing.ErrKeyMismatch) {
		t.Errorf("a youtube key with an unrelated URL answered %v, want ErrKeyMismatch", err)
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
// connector sights again becomes outside, and so does its child.
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
	for _, key := range []string{typed, typedChild} {
		if r, _ := st.Item(ctx, key); r.Item.Zone != store.Outside {
			t.Errorf("after its owner item was sighted outside, %s reads %q, want outside", key, r.Item.Zone)
		}
	}
}

// A book in a copy of the books folder lands by reference: its bytes are
// hashed where they lie, its home is books:<path>, and the volume gains
// nothing. A rerun writes nothing, and a path that leaves the root is refused.
// A book rewritten in place at the same size is no longer served as the blob.
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
	b, err := l.Open(ctx, first.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	served, _ := io.ReadAll(b)
	_ = b.Close()
	if !bytes.Equal(served, pdf) || b.Home == nil {
		t.Errorf("the blob served %d bytes from home %v, want the book's %d from its home", len(served), b.Home, len(pdf))
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
	named := book
	named.Key = "file/0123456789abcdef"
	if _, err := l.Land(ctx, named); !errors.Is(err, landing.ErrKeyMismatch) {
		t.Errorf("a home under a key other than its bytes' answered %v, want ErrKeyMismatch", err)
	}

	path := filepath.Join(books, "Some Author", "A Book.pdf")
	if err := os.WriteFile(path, []byte("%PDF-1.7 a fake"), 0o600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Minute)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Open(ctx, first.SHA256); !errors.Is(err, landing.ErrBytesMissing) {
		t.Errorf("a book rewritten at the same size opened with %v, want ErrBytesMissing", err)
	}
	if u, err := l.Unpaired(ctx); err != nil || len(u.Missing) != 1 {
		t.Errorf("after the rewrite the report names %d rows with no bytes, %v; want the book's", len(u.Missing), err)
	}
}
