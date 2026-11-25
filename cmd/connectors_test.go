package cmd

import (
	"io"
	"io/fs"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/goapiauth/authtest"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// connectors apply sends the example set from a git checkout, and connectors
// list prints its commit on every connector. A checkout behind the set in
// force is refused until --replacing names it. A directory holding no
// declarations exits 2. A set naming an unknown mode, zone or destination, or
// a root the service does not hold, is refused whole, naming each invalid
// declaration, exits 3, and leaves the set in force. A Reddit export dropped
// into reddit-exports lands as one item and a child per save. Dropped twice,
// it lands once, and a copy under another name adds no item.
func TestConnectorsApplyTheExampleSetAndAnInvalidSetChangesNothing(t *testing.T) {
	ctx := t.Context()
	dir, books := t.TempDir(), t.TempDir()
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
	provider := authtest.NewProvider(t)
	gate := verifiedGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	log := slog.New(slog.DiscardHandler)
	api := httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
		server.ConnectorRoutes(server.ConnectorConfig{
			Store: st, Lander: lander, Scheduler: connectors.NewScheduler(st, lander, log), Roots: []string{"books"}, Log: log,
		})))
	t.Cleanup(api.Close)
	tokenFile := filepath.Join(dir, "token.json")
	config := filepath.Join(dir, "config.toml")
	for path, body := range map[string]string{
		tokenFile: `{"access_token":"` + provider.Person(t, "ingestion-cli-test", "person") + `","token_type":"Bearer","expires_in":300}`,
		config: `api = "` + api.URL + `"` + "\n" + `issuer = "` + provider.URL + `"` + "\n" +
			`client_id = "ingestion-cli-test"` + "\n" + `token_command = ["cat", "` + tokenFile + `"]` + "\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	library := filepath.Join(dir, "library")
	if err := os.CopyFS(library, os.DirFS(filepath.Join("..", "examples", "declarations"))); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	gitIn, err := gitAt(ctx, library)
	if err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		out, err := gitIn(append([]string{"-c", "user.name=test", "-c", "user.email=test@example.com"}, args...)...).Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-q", "-m", "declarations")
	commit := git("rev-parse", "--short=7", "HEAD")

	out, err := runCLI(t, "connectors", "apply", library, "--config", config)
	if want := "Applied " + commit + ": 4 connectors, 2 routing rules, 3 destinations.\n"; err != nil || out != want {
		t.Fatalf("connectors apply printed %q, %v; want %q", out, err, want)
	}
	listed := func() {
		t.Helper()
		out, err := runCLI(t, "connectors", "list", "--config", config)
		if err != nil || strings.Count(out, " "+commit+" ") != 4 {
			t.Errorf("connectors list printed %q, %v; want commit %s on each of 4 connectors", out, err, commit)
		}
	}
	listed()

	git("commit", "-q", "--allow-empty", "-m", "a later commit")
	later := git("rev-parse", "--short=7", "HEAD")
	if out, err := runCLI(t, "connectors", "apply", library, "--config", config); err != nil || !strings.HasPrefix(out, "Applied "+later) {
		t.Fatalf("applying a later commit printed %q, %v", out, err)
	}
	git("checkout", "-q", "HEAD~1")
	_, err = runCLI(t, "connectors", "apply", library, "--config", config)
	if code := exitCode(io.Discard, err); code != 3 || !strings.Contains(err.Error(), "--replacing "+later) {
		t.Errorf("an apply from a checkout behind the set in force exited %d: %v; want 3 naming --replacing %s", code, err, later)
	}
	if out, err := runCLI(t, "connectors", "apply", library, "--replacing", later, "--config", config); err != nil || !strings.HasPrefix(out, "Applied "+commit) {
		t.Errorf("a rollback with --replacing printed %q, %v; want %s applied", out, err, commit)
	}
	listed()
	_, err = runCLI(t, "connectors", "apply", filepath.Join(library, "connectors"), "--config", config)
	if code := exitCode(io.Discard, err); code != 2 {
		t.Errorf("a directory holding no declarations exited %d, want 2: %v", code, err)
	}

	invalid := map[string]string{
		"connectors/stream.yml": "name: stream\nmode: stream\nsource: a-feed\nschedule: '@daily'\nzone: public\n",
		"routing-rules.yml":     "rules:\n  - name: to-nowhere\n    match: {tag: book}\n    destination: nowhere\n",
	}
	for _, refused := range []struct {
		files map[string]string
		names []string
	}{
		{invalid, []string{"connector stream", "routing rule to-nowhere"}},
		{map[string]string{"connectors/music.yml": "name: music\nmode: folder\nsource: local-music\nschedule: '@daily'\nzone: outside\nroot: music\n"}, []string{"connector music"}},
	} {
		edited := filepath.Join(t.TempDir(), "library")
		if err := os.CopyFS(edited, os.DirFS(filepath.Join("..", "examples", "declarations"))); err != nil {
			t.Fatal(err)
		}
		for name, body := range refused.files {
			if err := os.WriteFile(filepath.Join(edited, filepath.FromSlash(name)), []byte(body), fs.FileMode(0o600)); err != nil {
				t.Fatal(err)
			}
		}
		_, err := runCLI(t, "connectors", "apply", edited, "--replacing", commit, "--config", config)
		if code := exitCode(io.Discard, err); code != 3 {
			t.Errorf("an invalid set exited %d, want 3: %v", code, err)
		}
		for _, name := range refused.names {
			if err == nil || !strings.Contains(err.Error(), "\n  "+name+"\n") {
				t.Errorf("the refusal %v does not name %s", err, name)
			}
		}
	}
	listed()

	export, copied := filepath.Join(dir, "export.json"), filepath.Join(dir, "export (1).json")
	for _, file := range []string{export, copied} {
		if err := os.WriteFile(file, redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang")), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, drop := range []struct{ file, want string }{
		{export, "Dropped export.json into reddit-exports: it holds 3 items, 3 of them new.\n"},
		{export, "export.json was already dropped into reddit-exports, so nothing landed: it holds 3 items.\n"},
		{copied, "Dropped export (1).json into reddit-exports: it holds 3 items, 0 of them new.\n"},
	} {
		if out, err := runCLI(t, "items", "drop", drop.file, "--connector", "reddit-exports", "--config", config); err != nil || out != drop.want {
			t.Errorf("items drop printed %q, %v; want %q", out, err, drop.want)
		}
	}
	notes := filepath.Join(dir, "notes.json")
	if err := os.WriteFile(notes, []byte(`{"saves": []}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = runCLI(t, "items", "drop", notes, "--connector", "reddit-exports", "--config", config)
	var items int
	if scanErr := st.Reader().QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&items); scanErr != nil {
		t.Fatal(scanErr)
	}
	if err == nil || !strings.Contains(err.Error(), "not a Reddit export") || items != 3 {
		t.Errorf("dropping a file that is no Reddit export answered %v and left %d items, want it refused and the 3 items alone", err, items)
	}
}
