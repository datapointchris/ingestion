package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goclilogin"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// errNoAnswer is a command whose server gave no answer, which exits 4.
var errNoAnswer = errors.New("the server gave no answer")

const (
	answerAttempts = 3
	// answerDeadline bounds a command's every try together, so a server that
	// takes the connection and never answers holds it no longer.
	answerDeadline = 30 * time.Second
)

var tasksCmd = &cobra.Command{
	Use:   "tasks",
	Short: "Read the work the service holds in flight",
}

var tasksListCmd = &cobra.Command{
	Use:   "list",
	Short: "List queued, leased and failed tasks, and the live workers",
	Long: "Lists the service's tasks, the most recently changed first: every task but\n" +
		"the done ones, or the states --state names. A lapsed lease reads as leased\n" +
		"(lapsed), and the next claim takes the task. Each worker heard from in the\n" +
		"last " + shortDuration(server.WorkersHeardWithin) + " follows, with its capabilities and its last heartbeat, and\n" +
		"--all-workers lists every worker the service holds.\n" +
		"\n" +
		"A failed task's error is outsider text, so the list leaves it out.",
	Example: "  # work in flight\n" +
		"  ingestion tasks list\n" +
		"\n" +
		"  # failed tasks, as JSON\n" +
		"  ingestion tasks list --state failed --json",
	Args: cobra.NoArgs,
	RunE: runTasksList,
}

func init() {
	goclikit.AsNamespace(tasksCmd)
	tasksListCmd.Flags().StringSlice("state", nil, "Keep tasks in these states: queued, leased, done, failed")
	tasksListCmd.Flags().Int("limit", 100, "List at most this many tasks")
	tasksListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	tasksListCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	tasksListCmd.Flags().Bool("all-workers", false, "List every worker the service holds, not only those heard from lately")
	addConfigFlag(tasksListCmd)
	tasksCmd.AddCommand(tasksListCmd)
	rootCmd.AddCommand(tasksCmd)
}

func runTasksList(cmd *cobra.Command, _ []string) error {
	states, _ := cmd.Flags().GetStringSlice("state")
	limit, _ := cmd.Flags().GetInt("limit")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	allWorkers, _ := cmd.Flags().GetBool("all-workers")
	q := url.Values{}
	for _, s := range states {
		switch s {
		case store.TaskQueued, store.TaskLeased, store.TaskDone, store.TaskFailed:
		default:
			return fmt.Errorf("%w: --state is queued, leased, done or failed, not %q", goclikit.ErrUsage, s)
		}
		q.Add("state", s)
	}
	if limit < 1 || limit > server.MaxListed {
		return fmt.Errorf("%w: --limit is from 1 to %d, not %d", goclikit.ErrUsage, server.MaxListed, limit)
	}
	q.Set("limit", strconv.Itoa(limit))
	if allWorkers {
		q.Set("workers", "all")
	}
	var answer server.TasksAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, "/api/v1/tasks?"+q.Encode(), &answer)
	if err != nil {
		return err
	}
	if answer.WorkersHidden > 0 {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%d workers not heard from in the last %s are left out; --all-workers lists them.\n",
			answer.WorkersHidden, shortDuration(server.WorkersHeardWithin))
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	writeTasks(w, answer)
	return nil
}

func writeTasks(w io.Writer, answer server.TasksAnswer) {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "TASK\tITEM\tSTAGE\tCAPABILITY\tSTATE\tATTEMPTS\tLEASE\tUPDATED")
	for _, t := range answer.Tasks {
		state := t.State
		if t.Lapsed {
			state += " (lapsed)"
		}
		if t.HasError {
			state += " (error kept)"
		}
		lease := "—"
		if t.State == store.TaskLeased && t.LeaseHolder != nil && t.LeaseExpiresTS != nil {
			lease = *t.LeaseHolder + " until " + localTime(*t.LeaseExpiresTS)
		}
		if t.State == store.TaskQueued && t.NotBeforeTS != nil {
			lease = "retries at " + localTime(*t.NotBeforeTS)
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			t.ID, t.ItemKey, t.Stage, t.Capability, state, t.Attempts, lease, localTime(t.UpdatedTS))
	}
	if len(answer.Tasks) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	_, _ = fmt.Fprintln(table)
	_, _ = fmt.Fprintln(table, "WORKER\tVERSION\tCAPABILITIES\tSTARTED\tLAST HEARTBEAT")
	for _, wk := range answer.Workers {
		capabilities := strings.Join(wk.Capabilities, ",")
		if capabilities == "" {
			capabilities = "—"
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", wk.ID, wk.Version, capabilities, localTime(wk.StartedTS), localTime(wk.HeartbeatTS))
	}
	if len(answer.Workers) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	_ = table.Flush()
}

// localTime is a stored instant in this machine's zone.
func localTime(stamp string) string {
	at, err := time.Parse(store.StampLayout, stamp)
	if err != nil {
		return stamp
	}
	return at.Local().Format("2006-01-02 15:04:05 MST")
}

// getJSON reads route from the server the config file at path names, or api
// where it is set, into into, and answers the body as it came. It tries a
// bounded few times within one deadline, again on no answer and on 429, 502,
// 503 or 504. A server that gives no answer is errNoAnswer, naming the server
// and what chose it.
func getJSON(ctx context.Context, path, api, route string, into any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, answerDeadline)
	defer cancel()
	d, err := loadDeployment(ctx, path, api)
	var unreachable *url.Error
	if errors.As(err, &unreachable) {
		return nil, fmt.Errorf("%w: %s (--api): %v", errNoAnswer, api, err)
	}
	if err != nil {
		return nil, err
	}
	source, err := tokenSource(ctx, d)
	if err != nil {
		return nil, err
	}
	// Taken before the first request, so a token that cannot be had fails as
	// itself rather than as a server that gave no answer.
	token, err := source.Token()
	if err != nil {
		return nil, err
	}
	client := oauth2.NewClient(ctx, oauth2.ReuseTokenSource(token, source))
	base := strings.TrimRight(d.API.Value, "/")
	named := base + " " + chosenBy(d.API, d.Path)
	var lastErr error
	for attempt := range answerAttempts {
		if attempt > 0 {
			wait := time.Duration(attempt)*time.Second + rand.N(500*time.Millisecond)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %s, within %s: %v", errNoAnswer, named, answerDeadline, lastErr)
			case <-time.After(wait):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+route, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		switch resp.StatusCode {
		case http.StatusOK:
			return raw, json.Unmarshal(raw, into)
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			lastErr = fmt.Errorf("it answered %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
			continue
		}
		return nil, fmt.Errorf("%s refused GET %s with %d: %s", named, route, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return nil, fmt.Errorf("%w: %s, after %d tries: %v", errNoAnswer, named, answerAttempts, lastErr)
}

// chosenBy says what named the server: the --api flag, or the config file at
// path.
func chosenBy(api goclilogin.Setting, path string) string {
	switch api.Source {
	case goclilogin.SourceCommandLine:
		return "(--api)"
	case goclilogin.SourceFile:
		return "(api in " + path + ")"
	}
	return "(" + string(api.Source) + ")"
}

// shortDuration is d as a person writes it, such as 10m rather than 10m0s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
