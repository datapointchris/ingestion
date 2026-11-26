package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/server"
)

var pushCmd = &cobra.Command{
	Use:   "push <key or url> --to psyche --relate",
	Short: "Hand one item's parse and its scan's findings to psyche relate on this machine",
	Long: "Reads the item's parse and the findings of the scan that read it from the\n" +
		"service, writes them as the one document `psyche relate --parse` reads, and runs\n" +
		"`psyche relate --parse -` on this machine with that document on its stdin.\n" +
		"psyche prints its answer as it runs, and --json asks psyche for its JSON.\n" +
		"\n" +
		"Each command, link and piece of code that the scan flagged reaches psyche as a\n" +
		"suggestion rather than a finding. --enriched adds what the service derived from\n" +
		"the parse: the quality gate's idea, genre, domains, themes and quality, and the\n" +
		"newest model read's summary and suggestion. Without it psyche reads the parse\n" +
		"and the findings alone.\n" +
		"\n" +
		"A push starts no stage in the service, so no model reads the item there. It\n" +
		"hands on only an item that the service holds a scanned parse of. A URL resolves\n" +
		"to the key of the artifact it names. The command exits 4 when the service gives\n" +
		"no answer, and psyche never runs then. It exits 2 when it names an item that the\n" +
		"service holds no record of, and 1 when the item has no scanned parse yet or\n" +
		"psyche fails.",
	Example: "  ingestion push reddit/t3_aq7ggd --to psyche --relate\n" +
		"  ingestion push https://www.reddit.com/r/golang/comments/aq7ggd/ --to psyche --relate --enriched\n" +
		"  ingestion push reddit/t3_aq7ggd --to psyche --relate --json",
	Args: cobra.ExactArgs(1),
	RunE: runPush,
}

func init() {
	pushCmd.Flags().String("to", "", "Hand the item to this program: "+push.Program+" (required)")
	pushCmd.Flags().Bool("relate", false, "Run "+push.Program+" relate, the verb that reads a pushed parse")
	pushCmd.Flags().Bool("enriched", false, "Add the quality gate's and the model read's answers to the parse")
	pushCmd.Flags().Bool("json", false, "Ask "+push.Program+" for its answer as JSON")
	pushCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	_ = pushCmd.MarkFlagRequired("to")
	addConfigFlag(pushCmd)
	rootCmd.AddCommand(pushCmd)
}

func runPush(cmd *cobra.Command, args []string) error {
	to, _ := cmd.Flags().GetString("to")
	relate, _ := cmd.Flags().GetBool("relate")
	enriched, _ := cmd.Flags().GetBool("enriched")
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	if to != push.Program {
		return fmt.Errorf("%w: --to names %q, and a push hands an item only to %s", goclikit.ErrUsage, to, push.Program)
	}
	if !relate {
		return fmt.Errorf("%w: a push to %s takes --relate, the one %s verb that reads a pushed parse", goclikit.ErrUsage, push.Program, push.Program)
	}
	ctx, config, ref := cmd.Context(), configPath(cmd), strings.TrimSpace(args[0])
	// The server resolves a URL to its key, as items show relies on.
	target := "/api/v1/items/" + url.PathEscape(ref)
	var parse server.ParseAnswer
	_, err := getJSON(ctx, config, api, server.RouteItemParse, target+"/parse", &parse)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeParseNotFound {
		return fmt.Errorf("%s, so there is nothing to hand %s yet. `ingestion items show %s` says where it waits", refused.Message, push.Program, ref)
	}
	if err := argumentRefusal(err); err != nil {
		return err
	}
	var derived *push.Enriched
	if enriched {
		var item server.ItemAnswer
		if _, err := getJSON(ctx, config, api, server.RouteItem, target, &item); err != nil {
			return err
		}
		if derived = push.EnrichedFrom(item); derived == nil {
			_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s holds no quality gate answer or model read, so %s reads the parse alone\n", parse.Key, push.Program)
		}
	}
	doc, err := push.Write(parse, derived)
	switch {
	case errors.Is(err, push.ErrNotScanned):
		return fmt.Errorf("%s holds a parse that no scan has read yet, and a push hands on only a scanned parse. `ingestion items show %s` says where it waits", parse.Key, parse.Key)
	case err != nil:
		return fmt.Errorf("%s: %w", parse.Key, err)
	}
	enrichedWords := ""
	if derived != nil {
		enrichedWords = ", enriched"
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "pushing %s to %s relate: %s, %s, %s%s\n", doc.Key, push.Program,
		counted(len(doc.Units), "unit"), counted(len(doc.Findings), "finding"), counted(len(doc.Suggested), "suggestion"), enrichedWords)
	return push.Relate(ctx, doc, asJSON, cmd.OutOrStdout(), cmd.ErrOrStderr())
}

// counted is n of noun, as a person writes it.
func counted(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
