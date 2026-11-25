package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var itemsCmd = &cobra.Command{
	Use:   "items",
	Short: "Read the items the service holds",
}

var itemsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List items, newest first, with each one's zone, phase and connectors",
	Long: "Lists the service's items, newest first, up to --limit. Each row shows the\n" +
		"item's key, its zone, the phase its records reach, the connectors that\n" +
		"sighted it, when it landed, and its title.\n" +
		"\n" +
		"A title is outsider text where the zone is outside. --keys prints the keys\n" +
		"alone, one per line, so a session can work through items without reading one.",
	Example: "  # the newest items\n" +
		"  ingestion items list\n" +
		"\n" +
		"  # every key, for a script to read\n" +
		"  ingestion items list --keys --limit 5000",
	Args: cobra.NoArgs,
	RunE: runItemsList,
}

var itemsShowCmd = &cobra.Command{
	Use:   "show <key or url>",
	Short: "Show one item and every record it holds",
	Long: "Shows one item: its zone and phase, each sighting, where its bytes are kept,\n" +
		"each stage's output, each gate's decision and each delivery. A URL resolves\n" +
		"to the key of the artifact it names, so a Reddit or YouTube link reaches its\n" +
		"post or video.",
	Example: "  ingestion items show reddit/t3_aq7ggd\n" +
		"  ingestion items show https://www.reddit.com/r/golang/comments/aq7ggd/\n" +
		"  ingestion items show url/0123456789abcdef --json",
	Args: cobra.ExactArgs(1),
	RunE: runItemsShow,
}

var itemsDropCmd = &cobra.Command{
	Use:   "drop <file>",
	Short: "Drop a file into a drop connector, which lands it",
	Long: "Sends the file to the drop connector --connector names. It lands as one item\n" +
		"under its bytes' key, or goes through the handler registered for the\n" +
		"connector's format, which can land each item the file holds as its child.\n" +
		"Each item takes the connector's default tags and zone.\n" +
		"\n" +
		"The drop is keyed by the connector, the file's name, its media type and its\n" +
		"bytes, so dropping the same file again answers what the first drop landed,\n" +
		"and lands nothing new. A copy under another name lands as one more sighting\n" +
		"of the same item. The media type is read from the file's extension.",
	Example: "  ingestion items drop ~/Downloads/reddit-saved.json --connector reddit-exports",
	Args:    cobra.ExactArgs(1),
	RunE:    runItemsDrop,
}

var itemsReplayCmd = &cobra.Command{
	Use:   "replay --stage <stage>",
	Short: "Run a stage again over landed items, and print how many it queued",
	Long: "Runs one stage again over the items that --connector and --key select, or\n" +
		"over every item where neither is given, and prints how many it queued. A new\n" +
		"extractor version, rule set or prompt is replayed this way, and its output is\n" +
		"written beside the old one's.\n" +
		"\n" +
		"--stage is one of " + strings.Join(server.ReplayStages, ", ") + ".\n" +
		"The service refuses a stage that it holds no replay for, a --key that names\n" +
		"no item, and a --connector that sighted no item. A stage that calls a model\n" +
		"first prints its count and an estimate of its tokens by kind, and queues only\n" +
		"with --yes. A model-free stage, such as parse, queues at once.\n" +
		"\n" +
		"Each run sends one Idempotency-Key, and its retries send the same, so the\n" +
		"service queues each replay once.",
	Example: "  # parse every item again with the extractors the service holds\n" +
		"  ingestion items replay --stage parse\n" +
		"\n" +
		"  # one connector's items\n" +
		"  ingestion items replay --stage parse --connector reddit-exports\n" +
		"\n" +
		"  # one item, by key or by URL\n" +
		"  ingestion items replay --stage parse --key reddit/t3_aq7ggd",
	Args: cobra.NoArgs,
	RunE: runItemsReplay,
}

func init() {
	goclikit.AsNamespace(itemsCmd)
	itemsDropCmd.Flags().String("connector", "", "Drop the file into this drop connector (required)")
	itemsDropCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	_ = itemsDropCmd.MarkFlagRequired("connector")
	itemsListCmd.Flags().Int("limit", server.DefaultItemsListed, "List at most this many items")
	itemsListCmd.Flags().Bool("keys", false, "Print each item's key alone, one per line")
	itemsListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	itemsShowCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	itemsReplayCmd.Flags().String("stage", "", "Run this stage again: "+strings.Join(server.ReplayStages, ", "))
	itemsReplayCmd.Flags().String("connector", "", "Run it over the items this connector sighted")
	itemsReplayCmd.Flags().String("key", "", "Run it over the item under this key or URL")
	itemsReplayCmd.Flags().Bool("yes", false, "Queue a model stage once its estimate is read")
	itemsReplayCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{itemsListCmd, itemsShowCmd, itemsReplayCmd, itemsDropCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	itemsCmd.AddCommand(itemsListCmd, itemsShowCmd, itemsReplayCmd, itemsDropCmd)
	rootCmd.AddCommand(itemsCmd)
}

func runItemsReplay(cmd *cobra.Command, _ []string) error {
	stage, _ := cmd.Flags().GetString("stage")
	connector, _ := cmd.Flags().GetString("connector")
	key, _ := cmd.Flags().GetString("key")
	yes, _ := cmd.Flags().GetBool("yes")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	if !slices.Contains(server.ReplayStages, stage) {
		return fmt.Errorf("%w: --stage is one of %s, not %q", goclikit.ErrUsage, strings.Join(server.ReplayStages, ", "), stage)
	}
	req := server.ReplayRequest{Stage: stage, Confirm: yes}
	if connector = strings.TrimSpace(connector); connector != "" {
		req.Connector = &connector
	}
	if key = strings.TrimSpace(key); key != "" {
		req.Key = &key
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var answer server.ReplayAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteReplays, "/api/v1/replays", body, &answer)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeInvalidKey {
		return fmt.Errorf("%w: %s", goclikit.ErrUsage, refused.Message)
	}
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	if answer.Queued && answer.Items == 0 {
		_, err = fmt.Fprintf(w, "Queued nothing: %s runs on none of the selected items.\n", answer.Stage)
		return err
	}
	if answer.Queued {
		_, err = fmt.Fprintf(w, "Queued %s on %d items.\n", answer.Stage, answer.Items)
		return err
	}
	estimate := store.Tokens{}
	if answer.Estimate != nil {
		estimate = *answer.Estimate
	}
	_, err = fmt.Fprintf(w, "%s would run on %d items, estimated at uncached input %d, cache write %d, cache read %d and output %d tokens.\n"+
		"Nothing is queued. --yes queues them.\n", answer.Stage, answer.Items,
		estimate.UncachedInput, estimate.CacheWrite, estimate.CacheRead, estimate.Output)
	return err
}

func runItemsList(cmd *cobra.Command, _ []string) error {
	limit, _ := cmd.Flags().GetInt("limit")
	keys, _ := cmd.Flags().GetBool("keys")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	if limit < 1 {
		return fmt.Errorf("%w: --limit is at least 1, not %d", goclikit.ErrUsage, limit)
	}
	if keys && asJSON {
		return fmt.Errorf("%w: --keys and --json each choose the output, so give one", goclikit.ErrUsage)
	}
	answer := server.ItemsAnswer{SchemaVersion: 1, Items: []server.ItemListed{}}
	// The server answers at most its own page, with a cursor to the rest.
	for len(answer.Items) < limit {
		q := url.Values{}
		q.Set("limit", strconv.Itoa(limit-len(answer.Items)))
		if answer.Next != nil {
			q.Set("after", *answer.Next)
		}
		var page server.ItemsAnswer
		if _, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteItems, "/api/v1/items?"+q.Encode(), &page); err != nil {
			return err
		}
		answer.Items = append(answer.Items, page.Items...)
		answer.Next = page.Next
		if page.Next == nil {
			break
		}
	}
	if answer.Next != nil && !asJSON {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "More items are held than the %d listed; a larger --limit lists them.\n", limit)
	}
	w := cmd.OutOrStdout()
	switch {
	case asJSON:
		encoded, err := json.Marshal(answer)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(w, string(encoded))
		return err
	case keys:
		for _, it := range answer.Items {
			if _, err := fmt.Fprintln(w, it.Key); err != nil {
				return err
			}
		}
		return nil
	}
	writeItems(w, answer.Items)
	return nil
}

func writeItems(w io.Writer, items []server.ItemListed) {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "ITEM\tZONE\tPHASE\tCONNECTORS\tLANDED\tTITLE")
	for _, it := range items {
		phase := it.Phase
		if it.WaitingAt != nil {
			phase += " at " + *it.WaitingAt
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n",
			it.Key, it.Zone, phase, strings.Join(it.Connectors, ","), localTime(it.CreatedTS), orDash(it.Title))
	}
	if len(items) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	_ = table.Flush()
}

func runItemsShow(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	// The server resolves a URL to its key, so a CLI of another release reaches
	// the item that the service's own itemkey names.
	var answer server.ItemAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteItem, "/api/v1/items/"+url.PathEscape(strings.TrimSpace(args[0])), &answer)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeInvalidKey {
		return fmt.Errorf("%w: %s", goclikit.ErrUsage, refused.Message)
	}
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	writeItem(w, answer)
	return nil
}

func writeItem(w io.Writer, a server.ItemAnswer) {
	it := a.Item
	phase := it.Phase
	if it.WaitingAt != nil {
		phase += " at " + *it.WaitingAt
	}
	_, _ = fmt.Fprintf(w, "%s   %s   %s\n", it.Key, it.Zone, phase)
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(label, format string, args ...any) {
		_, _ = fmt.Fprintf(table, "  %s\t"+format+"\n", append([]any{label}, args...)...)
	}
	if it.Title != nil {
		row("title", "%s", *it.Title)
	}
	if it.URL != nil {
		row("url", "%s", *it.URL)
	}
	if it.ParentKey != nil {
		row("parent", "%s", *it.ParentKey)
	}
	for _, s := range a.Sightings {
		tags := "none"
		if len(s.Tags) > 0 {
			tags = strings.Join(s.Tags, ",")
		}
		row("sighted", "%s  %s  %s  tags %s  external %s", s.Connector, s.Zone, localTime(s.FirstSeenTS), tags, s.ExternalKey)
	}
	landed := localTime(it.CreatedTS) + "  " + it.MediaType
	if it.BlobSHA256 != nil {
		landed += "  blob " + (*it.BlobSHA256)[:12]
	}
	if it.SizeBytes != nil {
		landed += "  " + byteSize(*it.SizeBytes)
	}
	if it.MD5 != nil {
		landed += "  md5 " + *it.MD5
	}
	row("landed", "%s", landed)
	for _, h := range a.Homes {
		home := h.Root + ":" + h.Path
		if h.GoneTS != nil {
			home += "  gone " + localTime(*h.GoneTS)
		}
		row("home", "%s", home)
	}
	for _, o := range a.Outputs {
		out := o.Producer + " v" + o.ProducerVersion + "  " + o.State
		if o.Units != nil {
			out += fmt.Sprintf("  %d units", *o.Units)
		}
		if len(o.Findings) > 0 {
			out += fmt.Sprintf("  %d findings", len(o.Findings))
		}
		if o.Tokens != nil {
			out += fmt.Sprintf("  uncached input %d, cache write %d, cache read %d, output %d",
				o.Tokens.UncachedInput, o.Tokens.CacheWrite, o.Tokens.CacheRead, o.Tokens.Output)
		}
		row(o.Stage, "%s", out)
		for _, f := range o.Findings {
			row("finding", "%s", findingLine(f))
		}
	}
	for _, d := range a.Decisions {
		row(d.Gate, "%s  by %s  cause %s", d.Verdict, d.DecidedBy, d.Cause)
	}
	for _, d := range a.Deliveries {
		row("delivery", "%s  %s  attempts %d", d.Destination, d.Outcome, d.Attempts)
	}
	_ = table.Flush()
}

// spanShown bounds the runes of a finding's span that items show prints.
const spanShown = 120

// findingLine is one finding as items show prints it. Its span is outsider
// text, so it prints quoted, with every hidden or control character escaped.
func findingLine(f server.FindingView) string {
	line := f.Rule + "  " + f.Severity
	if f.Unit != nil {
		line += fmt.Sprintf("  unit %d", *f.Unit)
	}
	if f.Span != nil {
		span := []rune(*f.Span)
		if len(span) > spanShown {
			span = append(span[:spanShown], '…')
		}
		line += "  " + strconv.Quote(string(span))
	}
	return line
}

func orDash(s *string) string {
	if s == nil || *s == "" {
		return "—"
	}
	return *s
}

// byteSize is n as a person reads a size, such as 1.2 KiB.
func byteSize(n int64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func runItemsDrop(cmd *cobra.Command, args []string) error {
	connector, _ := cmd.Flags().GetString("connector")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	body, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	name := filepath.Base(args[0])
	mediaType := mime.TypeByExtension(filepath.Ext(name))
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	// The key covers all the server compares, so a copy under another name
	// lands as one more sighting rather than reusing the first drop's key.
	sum := sha256.Sum256(append([]byte(connector+"\x00"+name+"\x00"+mediaType+"\x00"), body...))
	var answer server.PushAnswer
	status, raw, err := callJSON(cmd.Context(), configPath(cmd), api, call{
		Route: server.RouteConnectorDrops, Target: "/api/v1/connectors/" + url.PathEscape(connector) + "/drops",
		Header: http.Header{
			"Content-Type":        {mediaType},
			"Content-Disposition": {mime.FormatMediaType("attachment", map[string]string{"filename": name})},
			server.IdempotencyKey: {"drop-" + hex.EncodeToString(sum[:16])},
		},
		Body: body,
	}, &answer)
	if err != nil {
		return refusedAsUsage(err)
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	created := 0
	for _, it := range answer.Items {
		if it.Created {
			created++
		}
	}
	if status == http.StatusOK {
		_, err = fmt.Fprintf(w, "%s was already dropped into %s, so nothing landed: it holds %s.\n", name, connector, itemCount(len(answer.Items)))
		return err
	}
	_, err = fmt.Fprintf(w, "Dropped %s into %s: it holds %s, %d of them new.\n", name, connector, itemCount(len(answer.Items)), created)
	return err
}

func itemCount(n int) string {
	if n == 1 {
		return "1 item"
	}
	return fmt.Sprintf("%d items", n)
}
