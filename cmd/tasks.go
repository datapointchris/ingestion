package cmd

import (
	"bytes"
	"cmp"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
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
	Example: "  # work in flight, and the live workers\n" +
		"  ingestion tasks list\n" +
		"\n" +
		"  # the failed tasks, to retry one\n" +
		"  ingestion tasks list --state failed",
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

var tasksRetryCmd = &cobra.Command{
	Use:   "retry <task>",
	Short: "Queue a failed task again",
	Long: "Queues a failed task again, with its attempts and backoff cleared, and prints\n" +
		"it. A model stage's task whose run failed then dispatches a new run, under a\n" +
		"key that names the failed one. A task that is queued or leased is printed as\n" +
		"it stands, so its run is never sent twice. A task that ended done is refused,\n" +
		"and `items replay` runs its stage again. A failed prompt-proposal task is\n" +
		"refused once a later proposal reads its misses. tasks list --state failed\n" +
		"lists the failed tasks, and items show prints why each failed.",
	Example: "  ingestion tasks list --state failed\n" +
		"  ingestion tasks retry 3f9a1c0d7e2b48a65c1e09d3b7f2a614",
	Args: cobra.ExactArgs(1),
	RunE: runTasksRetry,
}

func init() {
	goclikit.AsNamespace(tasksCmd)
	tasksListCmd.Flags().StringSlice("state", nil, "Keep tasks in these states: queued, leased, done, failed")
	tasksListCmd.Flags().Int("limit", 100, "List at most this many tasks")
	tasksListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	tasksListCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	tasksListCmd.Flags().Bool("all-workers", false, "List every worker the service holds, not only those heard from lately")
	addConfigFlag(tasksListCmd)
	tasksRetryCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	tasksRetryCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	addConfigFlag(tasksRetryCmd)
	tasksCmd.AddCommand(tasksListCmd, tasksRetryCmd)
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
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteTasks, "/api/v1/tasks?"+q.Encode(), &answer)
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

func runTasksRetry(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	id := strings.TrimSpace(args[0])
	var answer server.TaskRetryAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteTaskRetries,
		"/api/v1/tasks/"+url.PathEscape(id)+"/retries", []byte("{}"), &answer)
	if err := argumentRefusal(err, server.CodeTaskNotFound, server.CodeTaskDone, server.CodeTaskSuperseded); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	t := answer.Task
	_, err = fmt.Fprintf(w, "Task %s, %s on %s, is %s.\n", t.ID, t.Stage, taskSubject(t), t.State)
	return err
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
		if t.HeldBy != nil {
			state += " (held by " + *t.HeldBy + "'s first fill)"
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			t.ID, taskSubject(t), t.Stage, t.Capability, state, t.Attempts, lease, localTime(t.UpdatedTS))
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

// taskSubject is what a task works on: its item, its connector, the prompt
// proposal that it drafts, or the source whose metrics it reads again.
func taskSubject(t server.TaskView) string {
	switch {
	case t.ItemKey != nil:
		return *t.ItemKey
	case t.Connector != nil:
		return "connector " + *t.Connector
	case t.ProposalID != nil:
		return "proposal " + *t.ProposalID
	case t.Source != nil:
		return "source " + *t.Source
	}
	return "—"
}

// localTime is a stored instant in this machine's zone.
func localTime(stamp string) string {
	at, err := time.Parse(store.StampLayout, stamp)
	if err != nil {
		return stamp
	}
	return at.Local().Format("2006-01-02 15:04:05 MST")
}

// getJSON reads target, a path that route's pattern matches, from the server
// the config file at path names, or api where it is set, into into, and
// answers the body as it came.
func getJSON(ctx context.Context, path, api string, route server.Route, target string, into any) ([]byte, error) {
	return sendJSON(ctx, path, api, route, target, nil, into)
}

// sendJSON sends body as JSON, or nothing where it is nil, to target by
// route, as callJSON does, and answers the body as it came.
func sendJSON(ctx context.Context, path, api string, route server.Route, target string, body []byte, into any) ([]byte, error) {
	c := call{Route: route, Target: target, Body: body}
	if body != nil {
		c.Header = http.Header{"Content-Type": {"application/json"}}
	}
	_, raw, err := callJSON(ctx, path, api, c, into)
	return raw, err
}

// call is one request to the server: a row of its route table, target, a
// path that the row's pattern matches, and any header and body. Deadline bounds
// its every try together, and answerDeadline does where it is zero.
type call struct {
	Route    server.Route
	Target   string
	Header   http.Header
	Body     []byte
	Deadline time.Duration
}

// callJSON sends c on the server the config file at path names, or api where
// it is set, and reads a 2xx answer into into. It answers the status and the
// body as they came. Within one deadline it sends again on no answer and on
// 429, 502, 503 or 504, as the route's repeat kind allows. A keyed route
// carries one Idempotency-Key across every try, a fresh one where c's header
// names none. A once route goes again only where its first copy never reached
// the server. A server that gives no answer is errNoAnswer, naming the server
// and what chose it.
func callJSON(ctx context.Context, path, api string, c call, into any) (int, []byte, error) {
	route, target, body := c.Route, c.Target, c.Body
	deadline := cmp.Or(c.Deadline, answerDeadline)
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	d, err := loadDeployment(ctx, path, api)
	var unreachable *url.Error
	if errors.As(err, &unreachable) {
		return 0, nil, fmt.Errorf("%w: %s (--api): %v", errNoAnswer, api, err)
	}
	if err != nil {
		return 0, nil, err
	}
	source, err := tokenSource(ctx, d)
	if err != nil {
		return 0, nil, err
	}
	// Taken before the first request, so a token that cannot be had fails as
	// itself rather than as a server that gave no answer.
	token, err := source.Token()
	if err != nil {
		return 0, nil, err
	}
	client := oauth2.NewClient(ctx, oauth2.ReuseTokenSource(token, source))
	base := strings.TrimRight(d.API.Value, "/")
	named := base + " " + chosenBy(d.API, d.Path)
	method := route.Method()
	header := c.Header.Clone()
	if header == nil {
		header = http.Header{}
	}
	if route.Repeat == server.RepeatKeyed && header.Get(server.IdempotencyKey) == "" {
		header.Set(server.IdempotencyKey, randomKey())
	}
	var lastErr error
	tries := 0
	for tries < answerAttempts {
		if tries > 0 {
			wait := time.Duration(tries)*time.Second + rand.N(500*time.Millisecond)
			select {
			case <-ctx.Done():
				return 0, nil, fmt.Errorf("%w: %s, within %s: %v", errNoAnswer, named, deadline, lastErr)
			case <-time.After(wait):
			}
		}
		tries++
		var sent io.Reader
		if body != nil {
			sent = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+target, sent)
		if err != nil {
			return 0, nil, err
		}
		req.Header = header.Clone()
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if route.Repeat == server.RepeatOnce && !neverReached(err) {
				break
			}
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			lastErr = err
			if route.Repeat == server.RepeatOnce {
				break
			}
			continue
		}
		switch resp.StatusCode {
		case http.StatusOK, http.StatusCreated, http.StatusAccepted:
			return resp.StatusCode, raw, json.Unmarshal(raw, into)
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			if route.Repeat != server.RepeatOnce {
				lastErr = fmt.Errorf("it answered %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
				continue
			}
		}
		return resp.StatusCode, raw, refusalOf(named, method, target, resp.StatusCode, raw)
	}
	return 0, nil, fmt.Errorf("%w: %s, after %d tries: %v", errNoAnswer, named, tries, lastErr)
}

// randomKey is a fresh Idempotency-Key, carried by every try of one call.
func randomKey() string {
	b := make([]byte, 16)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

// neverReached says whether err is a request that failed while dialing, so
// no byte of it reached the server.
func neverReached(err error) bool {
	var op *net.OpError
	return errors.As(err, &op) && op.Op == "dial"
}

// refusal is the server refusing a call. Code and Message are what its body
// names, where the body is the server's refusal.
type refusal struct {
	Server, Method, Route string
	Status                int
	Code, Message         string
	Body                  string
}

func (r *refusal) Error() string {
	return fmt.Sprintf("%s refused %s %s with %d: %s", r.Server, r.Method, r.Route, r.Status, r.Body)
}

func refusalOf(server, method, route string, status int, raw []byte) *refusal {
	r := &refusal{Server: server, Method: method, Route: route, Status: status, Body: strings.TrimSpace(string(raw))}
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &body) == nil {
		r.Code, r.Message = body.Code, body.Message
	}
	return r
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
