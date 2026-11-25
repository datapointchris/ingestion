package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/goapiauth"
	"github.com/datapointchris/goapiauth/authtest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// items list --keys prints the newest keys alone up to its limit, items show
// reaches a Reddit post from a link the service resolves, a key that is not
// one exits 2, and blobs unpaired names a stray blob and exits 3.
func TestItemsListAndShowReadTheServiceAndUnpairedExits3OnAStrayBlob(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "ingestion.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	blobs, err := landing.OpenBlobs(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	lander, err := landing.New(landing.Config{Store: st, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	var landed []string
	for _, link := range []string{"https://example.com/a", "https://www.reddit.com/r/golang/comments/aq7ggd/a_post/", "https://example.com/b"} {
		l, err := lander.Land(ctx, landing.Arrival{Connector: "phone", Zone: store.Outside, MediaType: "text/uri-list", URL: link})
		if err != nil {
			t.Fatal(err)
		}
		landed = append([]string{l.Key}, landed...)
	}

	provider := authtest.NewProvider(t)
	gate := verifiedGate{provider.Verifier(t, "ingestion-cli-", "ingestion-svc-")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	api := httptest.NewServer(server.Handler(server.Config{Store: st, Gate: gate, Issuer: provider.URL, Version: "test", Log: log},
		server.BlobRoutes(lander, log), server.ItemRoutes(st, log)))
	t.Cleanup(api.Close)
	tokenFile := filepath.Join(dir, "token.json")
	token := `{"access_token":"` + provider.Person(t, "ingestion-cli-test", "person") + `","token_type":"Bearer","expires_in":300}`
	config := filepath.Join(dir, "config.toml")
	for path, body := range map[string]string{
		tokenFile: token,
		config: `api = "` + api.URL + `"` + "\n" + `issuer = "` + provider.URL + `"` + "\n" +
			`client_id = "ingestion-cli-test"` + "\n" + `token_command = ["cat", "` + tokenFile + `"]` + "\n",
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	out, err := runCLI(t, "items", "list", "--keys", "--limit", "2", "--config", config)
	if err != nil || out != strings.Join(landed[:2], "\n")+"\n" {
		t.Errorf("items list --keys printed %q, %v; want %v one per line", out, err, landed[:2])
	}
	out, err = runCLI(t, "items", "show", "https://old.reddit.com/r/golang/comments/aq7ggd/a_post/?utm_source=share", "--config", config)
	if err != nil || !strings.HasPrefix(out, "reddit/t3_aq7ggd   outside   landed\n") {
		t.Errorf("items show of a Reddit link printed %q, %v", out, err)
	}
	if _, err := runCLI(t, "items", "show", "reddit/aq7ggd", "--config", config); exitCode(io.Discard, err) != 2 {
		t.Errorf("items show of a key that is not one answered %v, want exit 2", err)
	}
	stray, err := blobs.Write(strings.NewReader("a stray"), "")
	if err != nil {
		t.Fatal(err)
	}
	out, err = runCLI(t, "blobs", "unpaired", "--json", "--config", config)
	var answer server.UnpairedAnswer
	if code := exitCode(io.Discard, err); code != 3 || json.Unmarshal([]byte(out), &answer) != nil {
		t.Fatalf("blobs unpaired over a stray blob exited %d and printed %q, want 3 and its answer", code, out)
	}
	if len(answer.Unrowed) != 1 || *answer.Unrowed[0].SHA256 != stray.SHA256 || len(answer.Missing) != 0 {
		t.Errorf("blobs unpaired named %+v with no row and %+v with no bytes, want the stray alone", answer.Unrowed, answer.Missing)
	}
}

// runCLI runs the command line args, with every flag at its default first,
// and answers what it printed to stdout. Each command takes this test's
// context, because cobra keeps the first one a subcommand was given.
func runCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var reset func(*cobra.Command)
	reset = func(c *cobra.Command) {
		c.SetContext(t.Context())
		c.Flags().VisitAll(func(f *pflag.Flag) {
			_ = f.Value.Set(f.DefValue)
			f.Changed = false
		})
		for _, child := range c.Commands() {
			reset(child)
		}
	}
	reset(rootCmd)
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(io.Discard)
	rootCmd.SetArgs(args)
	t.Cleanup(func() { rootCmd.SetOut(nil); rootCmd.SetErr(nil); rootCmd.SetArgs(nil) })
	err := rootCmd.ExecuteContext(t.Context())
	return out.String(), err
}

// verifiedGate is a verifier already holding its provider's keys.
type verifiedGate struct{ *goapiauth.Verifier }

func (verifiedGate) Ready() error { return nil }
