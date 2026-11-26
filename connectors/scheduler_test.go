package connectors_test

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/store"
)

// A folder connector over a temporary folder lands each new file once when it
// comes due, and a rerun lands nothing. A file that leaves the folder keeps
// its item, and its home reads gone. A poll connector's due time makes one
// task, named for its source, and a pass while it is in flight makes no other.
// A folder whose root is gone, as an unmounted share is, keeps the error on its
// cursor and its last good run, and comes due again in 5 minutes.
func TestTheSchedulerLandsEachNewFileOnceMakesOnePollTaskAndKeepsAFailedScan(t *testing.T) {
	ctx := t.Context()
	var mu sync.Mutex
	now := time.Date(2026, 11, 24, 10, 7, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	dir, books := t.TempDir(), t.TempDir()
	write := func(name, body string) {
		file := filepath.Join(books, name)
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		settled := time.Now().Add(-time.Minute)
		if err := os.Chtimes(file, settled, settled); err != nil {
			t.Fatal(err)
		}
	}
	write("a.epub", "an epub")
	write("b.md", "# notes")
	st, err := store.Open(ctx, filepath.Join(dir, "ingestion.db"), store.WithClock(clock))
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
	set := connectors.Set{Connectors: []connectors.Connector{
		{Name: "books-folder", Mode: connectors.ModeFolder, Source: "local-books", Schedule: "*/15 * * * *", Freshness: "30m", Zone: store.Outside, Root: "books"},
		{Name: "feed-reader", Mode: connectors.ModePoll, Source: "rss-feeds", Schedule: "@hourly", Zone: store.Outside},
	}}
	if _, err := connectors.Apply(ctx, st, set, "0123456", nil, "test", []string{"books"}, nil); err != nil {
		t.Fatal(err)
	}
	scheduler := connectors.NewScheduler(st, lander, slog.New(slog.DiscardHandler))
	pass := func() (items int, polls []store.Task) {
		t.Helper()
		if _, err := scheduler.Pass(ctx); err != nil {
			t.Fatal(err)
		}
		counts, err := st.ConnectorCounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tasks, err := st.Tasks(ctx, store.TaskQuery{})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range tasks {
			if task.Stage == store.StagePoll {
				polls = append(polls, task)
			}
		}
		return counts["books-folder"].Items, polls
	}

	if items, polls := pass(); items != 0 || len(polls) != 0 {
		t.Fatalf("a pass before either connector was due landed %d items and made %d polls", items, len(polls))
	}
	advance(time.Hour)
	items, polls := pass()
	if items != 2 || len(polls) != 1 {
		t.Fatalf("the first due pass landed %d items and made %d polls, want 2 and 1", items, len(polls))
	}
	if p := polls[0]; p.Capability != "poll:rss-feeds" || p.Connector == nil || *p.Connector != "feed-reader" || p.ItemKey != nil {
		t.Errorf("the poll task is %+v, want capability poll:rss-feeds naming feed-reader and no item", p)
	}
	cur, _, err := st.Cursor(ctx, "books-folder")
	if err != nil || cur.LastDeliveryTS == nil || cur.DueAt != store.Stamp(time.Date(2026, 11, 24, 11, 15, 0, 0, time.UTC)) {
		t.Errorf("after its scan books-folder's cursor is %+v, %v, want a delivery and due at 11:15", cur, err)
	}
	delivered, _ := time.Parse(store.StampLayout, *cur.LastDeliveryTS)
	if want := store.Stamp(delivered.Add(30 * time.Minute)); cur.StaleAt == nil || *cur.StaleAt != want {
		t.Errorf("books-folder's stale time is %v, want its delivery plus its 30m freshness, %s", cur.StaleAt, want)
	}
	// An earlier release leaves a stale time from before the last delivery,
	// and a pass before the folder is due again writes the right one.
	want := *cur.StaleAt
	if err := st.Update(ctx, func(tx *store.Tx) error {
		old := store.Stamp(delivered.Add(-time.Hour))
		cur.StaleAt = &old
		return tx.PutCursor(ctx, cur)
	}); err != nil {
		t.Fatal(err)
	}
	pass()
	if cur, _, err := st.Cursor(ctx, "books-folder"); err != nil || cur.StaleAt == nil || *cur.StaleAt != want {
		t.Errorf("after a pass books-folder's stale time is %v, %v, want %s again", cur.StaleAt, err, want)
	}
	advance(time.Hour)
	if items, polls := pass(); items != 2 || len(polls) != 1 {
		t.Errorf("a rerun over the same files landed to %d items and %d polls, want 2 and the one in flight", items, len(polls))
	}
	// The rerun moved its due time on and delivered nothing, so only its stale
	// time makes it overdue, since that time.
	cur, _, err = st.Cursor(ctx, "books-folder")
	if since, overdue := connectors.OverdueSince(cur, clock()); err != nil || !overdue || since != want {
		t.Errorf("an hour past its last delivery books-folder reads overdue %t since %q, %v; want overdue since %s", overdue, since, err, want)
	}
	write("c.txt", "a new file")
	advance(15 * time.Minute)
	if items, _ := pass(); items != 3 {
		t.Errorf("a pass after a new file landed to %d items, want 3", items)
	}
	// A file that leaves the folder leaves its item, and its home reads gone.
	// A folder emptied whole, as a share that failed to mount reads, stamps
	// nothing gone.
	live := func(name string) bool {
		t.Helper()
		sum := sha256.Sum256([]byte(name))
		homes, err := st.LiveHomesAt(ctx, "books", hex.EncodeToString(sum[:]))
		if err != nil {
			t.Fatal(err)
		}
		return len(homes) == 1
	}
	if err := os.Remove(filepath.Join(books, "b.md")); err != nil {
		t.Fatal(err)
	}
	advance(15 * time.Minute)
	if items, _ := pass(); items != 3 || live("b.md") || !live("a.epub") {
		t.Errorf("after b.md left the folder there are %d items, b.md live %t and a.epub live %t; want 3, false and true",
			items, live("b.md"), live("a.epub"))
	}
	for _, name := range []string{"a.epub", "c.txt"} {
		if err := os.Remove(filepath.Join(books, name)); err != nil {
			t.Fatal(err)
		}
	}
	advance(15 * time.Minute)
	if pass(); !live("a.epub") || !live("c.txt") {
		t.Error("a scan of an empty folder stamped its files' homes gone")
	}

	good, _, err := st.Cursor(ctx, "books-folder")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(books); err != nil {
		t.Fatal(err)
	}
	advance(15 * time.Minute)
	if _, err := scheduler.Pass(ctx); err == nil {
		t.Error("a pass over a missing folder answered no error")
	}
	cur, _, err = st.Cursor(ctx, "books-folder")
	if err != nil || cur.LastError == nil || cur.Failures != 1 || *cur.LastRunTS != *good.LastRunTS || cur.DueAt != store.Stamp(clock().Add(5*time.Minute)) {
		t.Errorf("after a failed scan books-folder's cursor is %+v, %v; want its error, 1 failure, the last run kept and due in 5 minutes", cur, err)
	}
}
