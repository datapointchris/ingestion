package cmd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var connectorsCmd = &cobra.Command{
	Use:   "connectors",
	Short: "Apply, read and run the connectors that bring each source's items in",
	Long: "A connector is a declaration kept in git: its mode, its source, its schedule or\n" +
		"due interval, its default tags and zone. `connectors apply` sends a directory\n" +
		"of them to the service whole, with the routing rules and the destinations.\n" +
		"The service's scheduler then owns when each one is due.",
}

var connectorsApplyCmd = &cobra.Command{
	Use:   "apply <dir>",
	Short: "Validate a directory of declarations and apply it to the service whole",
	Long: "Reads connectors/*.yml, routing-rules.yml and destinations.yml below dir,\n" +
		"validates the set whole, and applies it as the set in force. A set holding any\n" +
		"invalid declaration names each one and applies none, so the set in force\n" +
		"stays as it was. The service validates it again against its own settings,\n" +
		"such as the roots a folder may read.\n" +
		"\n" +
		"The service keeps the set with the commit it came from: dir's HEAD, marked\n" +
		"+dirty with the set's digest where dir holds uncommitted changes, or named\n" +
		"uncommitted by its digest outside a git work tree. `connectors list` prints it.\n" +
		"Applying the commit in force again changes nothing.",
	Example: "  ingestion connectors apply ./declarations",
	Args:    cobra.ExactArgs(1),
	RunE:    runConnectorsApply,
}

var connectorsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List each applied connector, its counts, and when it is next due",
	Long: "Lists each connector in the set in force: its mode, the commit applied, its\n" +
		"last delivery, its items, those waiting at a model stage, in review and\n" +
		"failed, and when the scheduler next makes it due. A connector whose due time\n" +
		"passed with no delivery reads overdue.",
	Example: "  ingestion connectors list\n" +
		"  ingestion connectors list --json",
	Args: cobra.NoArgs,
	RunE: runConnectorsList,
}

var connectorsShowCmd = &cobra.Command{
	Use:   "show <name>",
	Short: "Show one connector, and the item its first fill ran end to end",
	Long: "Shows one connector's declaration, counts and due time, and its first fill.\n" +
		"A connector's first fill runs its first item through the model stages and\n" +
		"holds every later one there until a person releases it, so the first item's\n" +
		"tokens can be read before the rest are spent. Every held item still lands,\n" +
		"parses and scans.",
	Example: "  ingestion connectors show reddit-exports",
	Args:    cobra.ExactArgs(1),
	RunE:    runConnectorsShow,
}

var connectorsRunCmd = &cobra.Command{
	Use:   "run <name>",
	Short: "Run a poll or folder connector now, rather than at its next due time",
	Long: "A poll connector's run is a task that a worker holding the source's poll\n" +
		"capability claims. Where one is already queued or leased, run answers that one.\n" +
		"A folder connector is made due now, and the service scans its folder next.\n" +
		"A drop or push connector runs as its items arrive, so it has no run.",
	Example: "  ingestion connectors run books-folder",
	Args:    cobra.ExactArgs(1),
	RunE:    runConnectorsRun,
}

var connectorsReleaseCmd = &cobra.Command{
	Use:   "release <name>",
	Short: "Release a connector's first fill, queuing every item it held",
	Long: "Lets every item that the connector's first fill holds at a model stage go on,\n" +
		"and stops holding any later one. Releasing a fill already released changes\n" +
		"nothing.",
	Example: "  ingestion connectors release reddit-exports",
	Args:    cobra.ExactArgs(1),
	RunE:    runConnectorsRelease,
}

func init() {
	goclikit.AsNamespace(connectorsCmd)
	connectorsListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	connectorsShowCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{connectorsApplyCmd, connectorsListCmd, connectorsShowCmd, connectorsRunCmd, connectorsReleaseCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	connectorsCmd.AddCommand(connectorsApplyCmd, connectorsListCmd, connectorsShowCmd, connectorsRunCmd, connectorsReleaseCmd)
	rootCmd.AddCommand(connectorsCmd)
}

func runConnectorsApply(cmd *cobra.Command, args []string) error {
	api, _ := cmd.Flags().GetString("api")
	dir := args[0]
	set, err := connectors.Read(dir, nil)
	if err != nil {
		return err
	}
	commit, err := appliedCommit(cmd.Context(), dir, set)
	if err != nil {
		return err
	}
	body, err := json.Marshal(server.DeclarationsRequest{Commit: commit, Set: set})
	if err != nil {
		return err
	}
	var answer server.DeclarationsAnswer
	_, _, err = callJSON(cmd.Context(), configPath(cmd), api, call{
		Method: http.MethodPut, Route: "/api/v1/declarations", Header: http.Header{"Content-Type": {"application/json"}}, Body: body,
	}, &answer)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeInvalidDeclarations {
		var named struct {
			Invalid []connectors.Invalid `json:"invalid"`
		}
		if json.Unmarshal([]byte(refused.Body), &named) == nil && len(named.Invalid) > 0 {
			return &connectors.InvalidError{Invalid: named.Invalid}
		}
	}
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if answer.Unchanged {
		_, err = fmt.Fprintf(w, "%s is already in force, so nothing changed.\n", shortCommit(answer.Commit))
		return err
	}
	_, err = fmt.Fprintf(w, "Applied %s: %d connectors, %d routing rules, %d destinations.\n",
		shortCommit(answer.Commit), answer.Connectors, answer.RoutingRules, answer.Destinations)
	return err
}

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}`)

// appliedCommit names where set came from: dir's HEAD where dir holds no
// uncommitted change, <HEAD>+dirty.<digest> where it holds one, and
// uncommitted.<digest> outside a git work tree or before its first commit. The
// digest is the first 12 hex of the SHA-256 of the set, so two different edits
// never share a name.
func appliedCommit(ctx context.Context, dir string, set connectors.Set) (string, error) {
	encoded, err := json.Marshal(set)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	digest := hex.EncodeToString(sum[:])[:12]
	git, err := gitAt(ctx, dir)
	if err != nil {
		return "", err
	}
	if git("rev-parse", "--verify", "--quiet", "HEAD").Run() != nil {
		return "uncommitted." + digest, nil
	}
	head, err := git("rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("read the commit %s is at: %w", dir, err)
	}
	status, err := git("status", "--porcelain", "--untracked-files=all", "--", ".").Output()
	if err != nil {
		return "", fmt.Errorf("read whether %s holds uncommitted changes: %w", dir, err)
	}
	commit := strings.TrimSpace(string(head))
	if len(strings.TrimSpace(string(status))) > 0 {
		commit += "+dirty." + digest
	}
	return commit, nil
}

// gitAt answers a builder of git commands that find their repository from dir.
// Each runs without the variables that would point git at another one, such
// as the GIT_DIR a hook exports, as git's own submodule script does.
func gitAt(ctx context.Context, dir string) (func(args ...string) *exec.Cmd, error) {
	local, err := exec.CommandContext(ctx, "git", "rev-parse", "--local-env-vars").Output()
	if err != nil {
		return nil, fmt.Errorf("ask git which variables locate a repository: %w", err)
	}
	drop := map[string]bool{}
	for _, name := range strings.Fields(string(local)) {
		drop[name] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); !drop[name] {
			env = append(env, kv)
		}
	}
	return func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
		cmd.Env = env
		return cmd
	}, nil
}

// shortCommit is commit as a person reads it: a full SHA's first 7, with any
// mark after it.
func shortCommit(commit string) string {
	if fullSHA.MatchString(commit) {
		rest := commit[40:]
		if mark, _, found := strings.Cut(rest, "."); found {
			rest = mark
		}
		return commit[:7] + rest
	}
	return commit
}

func runConnectorsList(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.ConnectorsAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, "/api/v1/connectors", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	writeConnectors(w, answer.Connectors, time.Now())
	return nil
}

func writeConnectors(w io.Writer, list []server.ConnectorView, now time.Time) {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	_, _ = fmt.Fprintln(table, "CONNECTOR\tMODE\tAPPLIED\tLAST DELIVERY\tITEMS\tWAITING\tREVIEW\tFAILED\tDUE\t")
	for _, c := range list {
		last := "—"
		if c.LastDeliveryTS != nil {
			last = shortTime(*c.LastDeliveryTS)
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t\n", c.Name, c.Mode, shortCommit(c.AppliedCommit), last,
			c.Items, c.Waiting, c.Review, c.Failed, dueText(c, now))
	}
	if len(list) == 0 {
		_, _ = fmt.Fprintln(table, "(no declarations applied)\t")
	}
	_ = table.Flush()
}

// dueText is when c is due as a person reads it: its schedule or interval,
// then the next due time, or how long it is overdue.
func dueText(c server.ConnectorView, now time.Time) string {
	cadence := ""
	switch {
	case c.Schedule != nil:
		cadence = *c.Schedule
	case c.DueInterval != nil:
		cadence = "every " + *c.DueInterval
	}
	if c.Overdue {
		return cadence + " · overdue since " + shortTime(c.DueAt)
	}
	at, err := time.Parse(store.StampLayout, c.DueAt)
	if err == nil && !at.After(now) {
		return cadence + " · due now"
	}
	return cadence + " · next " + shortTime(c.DueAt)
}

// shortTime is a stored instant in this machine's zone, to the minute.
func shortTime(stamp string) string {
	at, err := time.Parse(store.StampLayout, stamp)
	if err != nil {
		return stamp
	}
	return at.Local().Format("2006-01-02 15:04 MST")
}

func runConnectorsShow(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var c server.ConnectorView
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, "/api/v1/connectors/"+url.PathEscape(args[0]), &c)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	writeConnector(w, c, time.Now())
	return nil
}

func writeConnector(w io.Writer, c server.ConnectorView, now time.Time) {
	_, _ = fmt.Fprintf(w, "%s   %s   applied %s   zone %s   due %s\n", c.Name, c.Mode, shortCommit(c.AppliedCommit), c.Zone, dueText(c, now))
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(label, format string, args ...any) {
		_, _ = fmt.Fprintf(table, "  %s\t"+format+"\n", append([]any{label}, args...)...)
	}
	row("source", "%s", c.Source)
	tags := "none"
	if len(c.Tags) > 0 {
		tags = strings.Join(c.Tags, ",")
	}
	row("tags", "%s", tags)
	row("items", "%d, %d waiting at a model stage, %d in review, %d failed", c.Items, c.Waiting, c.Review, c.Failed)
	last := func(stamp *string) string {
		if stamp == nil {
			return "—"
		}
		return localTime(*stamp)
	}
	row("last run", "%s", last(c.LastRunTS))
	row("last delivery", "%s", last(c.LastDeliveryTS))
	if fill := c.FirstFill; fill != nil {
		switch fill.State {
		case store.FirstFillNone:
			row("first fill", "not begun: the first item to reach a model stage runs, and the rest wait for a release")
		case store.FirstFillHeld:
			row("first fill", "held after 1 item at the model stages, %d waiting behind it   item %s", fill.Held, orDash(fill.ItemKey))
		case store.FirstFillReleased:
			row("first fill", "released   item %s", orDash(fill.ItemKey))
		}
		stages := make([]string, 0, len(fill.Tokens))
		for stage := range fill.Tokens {
			stages = append(stages, stage)
		}
		sort.Strings(stages)
		for i, stage := range stages {
			t := fill.Tokens[stage]
			label := ""
			if i == 0 {
				label = "its tokens"
			}
			row(label, "%s   uncached input %d, cache write %d, cache read %d, output %d", stage, t.UncachedInput, t.CacheWrite, t.CacheRead, t.Output)
		}
		if fill.State == store.FirstFillHeld {
			row("release", "ingestion connectors release %s", c.Name)
		}
	}
	_ = table.Flush()
}

func runConnectorsRun(cmd *cobra.Command, args []string) error {
	api, _ := cmd.Flags().GetString("api")
	var answer server.RunAnswer
	status, _, err := callJSON(cmd.Context(), configPath(cmd), api, call{
		Method: http.MethodPost, Route: "/api/v1/connectors/" + url.PathEscape(args[0]) + "/runs",
		Header: http.Header{server.IdempotencyKey: {"run-" + randomKey()}},
	}, &answer)
	if err != nil {
		return refusedAsUsage(err)
	}
	w := cmd.OutOrStdout()
	switch {
	case answer.TaskID != nil && status == http.StatusCreated:
		_, err = fmt.Fprintf(w, "Queued poll task %s for %s.\n", *answer.TaskID, answer.Connector)
	case answer.TaskID != nil:
		_, err = fmt.Fprintf(w, "A poll of %s is already in flight as task %s.\n", answer.Connector, *answer.TaskID)
	default:
		_, err = fmt.Fprintf(w, "%s is due now, and the service scans its folder next.\n", answer.Connector)
	}
	return err
}

func runConnectorsRelease(cmd *cobra.Command, args []string) error {
	api, _ := cmd.Flags().GetString("api")
	var answer server.ReleaseAnswer
	_, _, err := callJSON(cmd.Context(), configPath(cmd), api, call{
		Method: http.MethodPost, Route: "/api/v1/connectors/" + url.PathEscape(args[0]) + "/release",
	}, &answer)
	if err != nil {
		return refusedAsUsage(err)
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Released %s's first fill, and queued the %d tasks it held.\n", answer.Connector, answer.Released)
	return err
}

// refusedAsUsage makes a refusal naming no such connector, or a mode that has
// no such verb, a usage error, since the command line named it.
func refusedAsUsage(err error) error {
	var refused *refusal
	if errors.As(err, &refused) && (refused.Code == server.CodeConnectorNotFound || refused.Code == server.CodeConnectorMode) {
		return fmt.Errorf("%w: %s", goclikit.ErrUsage, refused.Message)
	}
	return err
}

// randomKey is a fresh Idempotency-Key, carried by every try of one command.
func randomKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
