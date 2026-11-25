package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	answerTimeout  = 15 * time.Second
)

var tasksCmd = &cobra.Command{
	Use:   "tasks",
	Short: "Read the work the service holds in flight",
}

var tasksListCmd = &cobra.Command{
	Use:   "list",
	Short: "List queued, leased and failed tasks, and every worker",
	Long: "Lists the service's tasks, the most recently changed first: every task but\n" +
		"the done ones, or the states --state names. A lapsed lease reads as leased\n" +
		"(lapsed), and the next claim takes the task. Each worker follows, with its\n" +
		"capabilities and its last heartbeat.\n" +
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
	tasksCmd.AddCommand(tasksListCmd)
	rootCmd.AddCommand(tasksCmd)
}

func runTasksList(cmd *cobra.Command, _ []string) error {
	states, _ := cmd.Flags().GetStringSlice("state")
	limit, _ := cmd.Flags().GetInt("limit")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	q := url.Values{}
	for _, s := range states {
		q.Add("state", s)
	}
	q.Set("limit", strconv.Itoa(limit))
	var answer server.TasksAnswer
	raw, err := getJSON(cmd.Context(), api, "/api/v1/tasks?"+q.Encode(), &answer)
	if err != nil {
		return err
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

// getJSON reads path from the configured server into into, and answers the
// body as it came. A server that gives no answer after a bounded few tries is
// errNoAnswer, naming the server.
func getJSON(ctx context.Context, api, path string, into any) ([]byte, error) {
	d, err := loadDeployment(ctx, goclilogin.ConfigPath(product), api)
	if err != nil {
		return nil, err
	}
	source, err := tokenSource(ctx, d)
	if err != nil {
		return nil, err
	}
	client := oauth2.NewClient(ctx, source)
	client.Timeout = answerTimeout
	base := strings.TrimRight(d.API.Value, "/")
	var lastErr error
	for attempt := range answerAttempts {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			var retrieve *oauth2.RetrieveError
			if errors.As(err, &retrieve) || errors.Is(err, errNeedsLogin) {
				return nil, err
			}
			lastErr = err
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("%s answered %d: %s", base, resp.StatusCode, strings.TrimSpace(string(raw)))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s refused GET %s with %d: %s", base, path, resp.StatusCode, strings.TrimSpace(string(raw)))
		}
		return raw, json.Unmarshal(raw, into)
	}
	return nil, fmt.Errorf("%w: %s, after %d tries: %v", errNoAnswer, base, answerAttempts, lastErr)
}
