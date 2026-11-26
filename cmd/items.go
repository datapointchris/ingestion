package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var itemsCmd = &cobra.Command{
	Use:   "items",
	Short: "Read the items the service holds",
	Example: "  # the newest items\n" +
		"  ingestion items list\n" +
		"\n" +
		"  # one item and every record it holds\n" +
		"  ingestion items show reddit/t3_aq7ggd",
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
		"each stage's output, each gate's decision and each delivery. A delivery\n" +
		"prints its outcome, the destination's id for the record that holds the item,\n" +
		"its attempts and the destination's last answer. It names its task's\n" +
		"capability where no worker heard from lately claims it. An item read as is\n" +
		"that reaches no deliverer says why, such as a side with no default\n" +
		"destination.\n" +
		"A URL resolves to the key of the artifact it names, so a Reddit or YouTube\n" +
		"link reaches its post or video.",
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
		"first prints how many items it would run on and an estimate of their tokens\n" +
		"by kind, the median of its newest stored runs times that count, and queues\n" +
		"only with --yes. A model-free stage, such as parse, queues at once.\n" +
		"\n" +
		"A structure replay profiles each PDF again, such as with a new pdf-structure,\n" +
		"and its end writes the PDF's sections beside the old. Where the PDF already\n" +
		"holds chapters, a new chapter row names the chapter item with the same start\n" +
		"page and title, and one that matches none makes no item.\n" +
		"\n" +
		"A quality replay runs on each scanned item that holds no answer of the prompt\n" +
		"in force and that no person placed. An item that a tag, a rule or a\n" +
		"confident guess placed keeps its placement whatever the answer says.\n" +
		"--placed-by person runs it only over each labeled item, one that a person\n" +
		"placed after the gates sent it to review, as a measurement against the\n" +
		"placement. Its answer and its decision are kept, and the placement, its\n" +
		"delivery and its model read stay as they are. `ingestion gates agreement`\n" +
		"then counts how often the gate agreed.\n" +
		"\n" +
		"--stage quality-arm asks the quality gate's prompt of the model that --model\n" +
		"names, over each labeled item that the gate answered under that prompt and no\n" +
		"arm has. Each answer is kept at its own stage and decides nothing, so the\n" +
		"item's phase, placement and delivery stay as they are. `ingestion gates\n" +
		"agreement` prints each arm beside the gate's answers on the same items. Its\n" +
		"estimate is the median of the arm's own stored runs, of any model.\n" +
		"\n" +
		"A deliver replay queues the delivery that each item's placement sends it to,\n" +
		"where it is not settled: one pending, or one never queued. A delivery that a\n" +
		"destination holds or refused is never written again.\n" +
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
		"  ingestion items replay --stage parse --key reddit/t3_aq7ggd\n" +
		"\n" +
		"  # measure the quality gate against each placement a person made from review\n" +
		"  ingestion items replay --stage quality --connector reddit-exports --placed-by person\n" +
		"\n" +
		"  # ask the gate's prompt of another model on one labeled item\n" +
		"  ingestion items replay --stage quality-arm --model sonnet --key reddit/t3_aq7ggd",
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
	itemsReplayCmd.Flags().String("placed-by", "", "Run it only over the items that this placed from review: "+store.PlacedByPerson)
	itemsReplayCmd.Flags().String("model", "", "Ask this model, as "+store.StageQualityArm+" does")
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
	placedBy, _ := cmd.Flags().GetString("placed-by")
	yes, _ := cmd.Flags().GetBool("yes")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	if !slices.Contains(server.ReplayStages, stage) {
		return fmt.Errorf("%w: --stage is one of %s, not %q", goclikit.ErrUsage, strings.Join(server.ReplayStages, ", "), stage)
	}
	req := server.ReplayRequest{Stage: stage, Confirm: yes}
	if cmd.Flags().Changed("placed-by") {
		if placedBy != store.PlacedByPerson {
			return fmt.Errorf("%w: --placed-by is %s, not %q", goclikit.ErrUsage, store.PlacedByPerson, placedBy)
		}
		req.PlacedBy = &placedBy
	}
	if connector = strings.TrimSpace(connector); connector != "" {
		req.Connector = &connector
	}
	if key = strings.TrimSpace(key); key != "" {
		req.Key = &key
	}
	if model, _ := cmd.Flags().GetString("model"); strings.TrimSpace(model) != "" {
		model = strings.TrimSpace(model)
		req.Model = &model
	} else if stage == store.StageQualityArm {
		return fmt.Errorf("%w: --stage %s asks the model that --model names, so give one", goclikit.ErrUsage, stage)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	var answer server.ReplayAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteReplays, "/api/v1/replays", body, &answer)
	if err := argumentRefusal(err); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	stageWords := answer.Stage
	if answer.Model != nil {
		stageWords += " (" + *answer.Model + ")"
	}
	if answer.Items == 0 {
		_, err = fmt.Fprintf(w, "Queued nothing: %s runs on none of the selected items.\n", stageWords)
		return err
	}
	if answer.Queued {
		_, err = fmt.Fprintf(w, "Queued %s on %d items.\n", stageWords, answer.Items)
		return err
	}
	if e := answer.Estimate; e != nil {
		_, err = fmt.Fprintf(w, "%s would run on %d items, estimated at uncached input %d, cache write %d, cache read %d and output %d tokens.\n",
			stageWords, answer.Items, e.UncachedInput, e.CacheWrite, e.CacheRead, e.Output)
	} else {
		_, err = fmt.Fprintf(w, "%s would run on %d items. No stored run of it gives an estimate of its tokens.\n", stageWords, answer.Items)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, "Nothing is queued. --yes queues them.")
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
	if err := argumentRefusal(err); err != nil {
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
		out := o.Producer + " " + versionWords(o.ProducerVersion) + "  " + o.State
		if o.Units != nil {
			out += fmt.Sprintf("  %d units", *o.Units)
		}
		if len(o.Findings) > 0 {
			out += fmt.Sprintf("  %d findings", len(o.Findings))
		}
		if o.Model != nil {
			out += "  " + *o.Model
			if o.Effort != nil {
				out += " at " + *o.Effort
			}
			if o.AnsweredModel != nil && *o.AnsweredModel != *o.Model {
				out += ", answered on " + *o.AnsweredModel
			}
		}
		if o.DispatchID != nil {
			out += "  dispatch " + *o.DispatchID
		}
		if o.Tokens != nil {
			out += fmt.Sprintf("  uncached input %d, cache write %d, cache read %d, output %d",
				o.Tokens.UncachedInput, o.Tokens.CacheWrite, o.Tokens.CacheRead, o.Tokens.Output)
		}
		row(o.Stage, "%s", out)
		if o.Error != nil && o.State == store.OutputFailed {
			row("", "%s", quoted(*o.Error))
		}
		if (o.Stage == store.StageQuality || o.Stage == store.StageQualityArm) && o.State == store.OutputDone {
			writeQualityAnswer(row, o.Answer)
		}
		if o.Stage == store.StageModelRead && o.State == store.OutputDone {
			writeModelReadAnswer(row, o)
		}
		for _, f := range o.Findings {
			row("finding", "%s", findingLine(f))
		}
	}
	writeSections(row, a.Sections)
	for _, d := range a.Decisions {
		decision := d.Verdict
		if d.Destination != nil {
			decision += " on " + *d.Destination
		}
		decision += "  by " + d.DecidedBy
		if d.Confidence != nil {
			decision += fmt.Sprintf(" at %.2f", *d.Confidence)
		}
		decision += "  cause " + d.Cause
		if d.Lens != nil {
			decision += "  lens " + *d.Lens
		}
		row(d.Gate, "%s", decision)
	}
	for _, d := range a.Deliveries {
		delivery := d.Destination + "  " + d.Outcome
		if d.DestinationID != nil {
			delivery += " as " + *d.DestinationID
		}
		delivery += fmt.Sprintf("  attempts %d", d.Attempts)
		if d.LastError != nil {
			delivery += "  " + *d.LastError
		}
		if d.UnclaimedCapability != nil {
			delivery += "  no worker heard from lately claims " + *d.UnclaimedCapability
		}
		row("delivery", "%s", delivery)
	}
	if u := a.Undelivered; u != nil {
		row("delivery", "none: %s", u.Message)
	}
	if wt := a.Waiting; wt != nil {
		row("waiting", "%s: %s", wt.Reason, wt.Message)
	}
	_ = table.Flush()
}

// writeSections prints a line for each producer version of the item's
// sections, counting its entries and chapters, then a line for each chapter.
// A title is outsider text, so it prints quoted.
func writeSections(row func(label, format string, args ...any), sections []server.SectionView) {
	for start := 0; start < len(sections); {
		end := start + 1
		for end < len(sections) && sections[end].Producer == sections[start].Producer &&
			sections[end].ProducerVersion == sections[start].ProducerVersion {
			end++
		}
		version := sections[start:end:end]
		chapters := 0
		for _, s := range version {
			if s.Kind == store.SectionChapter {
				chapters++
			}
		}
		row("sections", "%s %s  %d entries, %d chapters", version[0].Producer, versionWords(version[0].ProducerVersion), len(version), chapters)
		for _, s := range version {
			if s.Kind != store.SectionChapter && len(version) > 1 {
				continue
			}
			line := fmt.Sprintf("%s pdf pages %d-%d", s.Kind, s.PDFPageStart, s.PDFPageEnd)
			if s.PrintedPageStart != nil && s.PrintedPageEnd != nil {
				line += fmt.Sprintf(", printed %s-%s", *s.PrintedPageStart, *s.PrintedPageEnd)
			}
			if s.ChildKey != nil {
				line += "  " + *s.ChildKey
			}
			if s.Title != nil {
				line += "  " + quoted(*s.Title)
			}
			row("", "%s", line)
		}
		start = end
	}
}

// writeQualityAnswer prints the quality gate's answer. Its idea, reason and
// themes are a model's words about outsider text, so they print quoted.
func writeQualityAnswer(row func(label, format string, args ...any), raw json.RawMessage) {
	var a qualitygate.Answer
	if json.Unmarshal(raw, &a) != nil {
		return
	}
	row("", "quality %d  %s at %.2f  %s  side %s at %.2f", a.Quality, a.Verdict, a.VerdictConfidence, a.Genre, a.Side, a.SideConfidence)
	row("", "idea %s", quoted(a.Idea))
	if a.Reason != "" {
		row("", "reason %s", quoted(a.Reason))
	}
	if len(a.Domains) > 0 {
		row("", "domains %s", quoted(strings.Join(a.Domains, ", ")))
	}
}

// writeModelReadAnswer prints a model read's lens, its summary and its
// suggestion. A model wrote both from outsider text, so they print quoted, up
// to answerShown. The findings that follow it are the scanner's over the
// suggestion.
func writeModelReadAnswer(row func(label, format string, args ...any), o server.OutputView) {
	if o.Lens != nil {
		lens := *o.Lens
		if o.LensVersion != nil {
			lens += " " + versionWords(*o.LensVersion)
		}
		row("", "lens %s", lens)
	}
	var a modelread.Answer
	if json.Unmarshal(o.Answer, &a) != nil {
		return
	}
	if a.Summary != nil {
		row("", "summary %s", quotedUpTo(*a.Summary, answerShown))
	}
	if a.Suggestion != nil {
		row("", "suggestion %s", quotedUpTo(*a.Suggestion, answerShown))
	}
}

const (
	// spanShown bounds the runes of a finding's span that items show prints.
	spanShown = 120
	// answerShown bounds the runes of a model read's summary or suggestion
	// that items show prints. Keep it at or above summary's maxLength in
	// prompts/model-read/answer.schema.json, so an answer prints whole.
	answerShown = 3000
)

// versionWords is a producer version as a person reads it: v2 for a number,
// and a version such as the rules' as it is.
func versionWords(version string) string {
	if version != "" && '0' <= version[0] && version[0] <= '9' {
		return "v" + version
	}
	return version
}

// findingLine is one finding as items show prints it. Its span is outsider
// text, so it prints quoted, with every hidden or control character escaped.
func findingLine(f server.FindingView) string {
	line := f.Rule + "  " + f.Severity
	if f.Unit != nil {
		line += fmt.Sprintf("  unit %d", *f.Unit)
	}
	if f.Span != nil {
		line += "  " + quoted(*f.Span)
	}
	return line
}

// quoted is text that may carry outsider words, cut to spanShown runes and
// quoted with every hidden or control character escaped.
func quoted(text string) string { return quotedUpTo(text, spanShown) }

// quotedUpTo is quoted with its cut at shown runes.
func quotedUpTo(text string, shown int) string {
	runes := []rune(text)
	if len(runes) > shown {
		runes = append(runes[:shown], '…')
	}
	return strconv.Quote(string(runes))
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

// dropDeadline bounds a drop's tries. The server answers once it has landed
// every item the file holds, at about 0.4 s a Reddit save, so an export of a
// few hundred saves outlasts the deadline every other command keeps.
const dropDeadline = 10 * time.Minute

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
		Body:     body,
		Deadline: dropDeadline,
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
