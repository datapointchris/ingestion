package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/earnedtrust"
	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var gatesRoutingRulesCmd = &cobra.Command{
	Use:   "routing-rules",
	Short: "Accept, retire and read the routing rules that a person accepted, which the side gate reads after the declared ones",
	Long: "A routing rule places an item before the quality gate reads it. A rule written\n" +
		"by hand lives in the declaration set's routing-rules.yml. A rule that a person\n" +
		"accepts, such as from a proposal drawn from their verdicts, is kept by the\n" +
		"service, one version per write, and the side gate reads each one in force after\n" +
		"every declared rule: each keep, then each block, by name.\n" +
		"\n" +
		"A block drops each item that the rule matches, as rule:<name>, and keeps its\n" +
		"row. A keep places it on a destination. Each rule's review share of the items\n" +
		"it matches waits in review instead, as rule-would-hide, so a wrong rule shows\n" +
		"itself. A seeded draw over the rule's name and the item's key picks them, so a\n" +
		"replay picks the same items, and a higher share keeps every item that a lower\n" +
		"one picked. A rule never weighs in the review tab's order.",
	Example: "  ingestion gates routing-rules list\n" +
		"  ingestion gates routing-rules put quiet-feed --block --connector feed-reader --review-share 0.1\n" +
		"  ingestion gates routing-rules retire quiet-feed",
}

var gatesRoutingRulesListCmd = &cobra.Command{
	Use:   "list",
	Short: "Print the newest version of each accepted routing rule",
	Long: "Prints each accepted rule by name at its newest version: what it does, what it\n" +
		"matches, its review share, whether it is in force or retired, and who wrote the\n" +
		"version. The declared rules are in the set's routing-rules.yml.",
	Example: "  ingestion gates routing-rules list\n" +
		"  ingestion gates routing-rules list --json",
	Args: cobra.NoArgs,
	RunE: runGatesRoutingRulesList,
}

var gatesRoutingRulesPutCmd = &cobra.Command{
	Use:   "put <name>",
	Short: "Accept a routing rule, as a new version of the rule called name",
	Long: "Writes the rule as your word, under person:<login>. --block drops each item\n" +
		"that it matches, and --keep places each on the destination named. The match is\n" +
		"any of --connector, --tag, and --field with each --value it holds for, and each\n" +
		"one named must hold on one of the item's sightings. --review-share is the share\n" +
		"of matched items that wait in review as rule-would-hide instead, from 0 to 1.\n" +
		"\n" +
		"The write runs the side gate again over each scanned item that waits on it, so\n" +
		"the rule decides what it matches there. An item that a tag, a rule, a model's\n" +
		"confident guess or a person placed stays where it is. Writing what the newest\n" +
		"version already says appends nothing. The name cannot be a declared rule's.",
	Example: "  ingestion gates routing-rules put quiet-feed --block --connector feed-reader --review-share 0.1\n" +
		"  ingestion gates routing-rules put cooking --keep reading-list --connector reddit-exports \\\n" +
		"    --field subreddit --value Cooking --review-share 0.05",
	Args: cobra.ExactArgs(1),
	RunE: runGatesRoutingRulesPut,
}

var gatesRoutingRulesRetireCmd = &cobra.Command{
	Use:   "retire <name>",
	Short: "Retire an accepted routing rule, which takes it out of force",
	Long: "Appends a retired copy of the rule's newest version, under person:<login>. Each\n" +
		"item that the rule placed or dropped stays where it is, and each item that its\n" +
		"share sent to review still waits there. Retiring a retired rule changes nothing.\n" +
		"`put` brings it back as a new version.",
	Example: "  ingestion gates routing-rules retire quiet-feed",
	Args:    cobra.ExactArgs(1),
	RunE:    runGatesRoutingRulesRetire,
}

func init() {
	goclikit.AsNamespace(gatesRoutingRulesCmd)
	for _, c := range []*cobra.Command{gatesRoutingRulesListCmd, gatesRoutingRulesPutCmd, gatesRoutingRulesRetireCmd} {
		c.Flags().Bool("json", false, "Print the service's answer as JSON")
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	f := gatesRoutingRulesPutCmd.Flags()
	f.Bool("block", false, "Drop each item that the rule matches")
	f.String("keep", "", "Place each item that the rule matches on this destination")
	f.String("connector", "", "Match an item that this connector sighted")
	f.String("tag", "", "Match an item that a sighting tagged with this")
	f.String("field", "", "Match an item whose sighting's field holds one of --value")
	f.StringArray("value", nil, "A value of --field that the rule matches, once per value")
	f.Float64("review-share", 0, "The share of matched items that wait in review as rule-would-hide, from 0 to 1")
	f.String("proposal", "", "The id of the proposal that you accepted, which the version keeps")
	_ = gatesRoutingRulesPutCmd.MarkFlagRequired("review-share")
	gatesRoutingRulesPutCmd.MarkFlagsMutuallyExclusive("block", "keep")
	gatesRoutingRulesPutCmd.MarkFlagsOneRequired("block", "keep")
	gatesRoutingRulesPutCmd.MarkFlagsOneRequired("connector", "tag", "field")
	gatesRoutingRulesPutCmd.MarkFlagsRequiredTogether("field", "value")
	gatesRoutingRulesCmd.AddCommand(gatesRoutingRulesListCmd, gatesRoutingRulesPutCmd, gatesRoutingRulesRetireCmd)
	gatesCmd.AddCommand(gatesRoutingRulesCmd)
}

func runGatesRoutingRulesList(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.RoutingRulesAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteRoutingRules, "/api/v1/routing-rules", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "NAME\tVERSION\tACTION\tMATCH\tDESTINATION\tREVIEW SHARE\tSTATE\tDECIDED BY")
	for _, r := range answer.Rules {
		state := "in force"
		if r.Retired {
			state = "retired"
		}
		_, _ = fmt.Fprintf(table, "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", r.Name, r.Sequence, r.Action, matchWords(r.Match), orDash(r.Destination),
			earnedtrust.Percent(r.ReviewShare), state, r.DecidedBy)
	}
	if len(answer.Rules) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	return table.Flush()
}

func runGatesRoutingRulesPut(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	f := cmd.Flags()
	keep, _ := f.GetString("keep")
	share, _ := f.GetFloat64("review-share")
	body := server.RoutingRuleRequest{Action: store.RuleBlock, Destination: keep, ReviewShare: &share}
	if keep != "" {
		body.Action = store.RuleKeep
	}
	body.Match.Connector, _ = f.GetString("connector")
	body.Match.Tag, _ = f.GetString("tag")
	body.Match.Field, _ = f.GetString("field")
	body.Match.Values, _ = f.GetStringArray("value")
	body.Proposal, _ = f.GetString("proposal")
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var answer server.RoutingRuleAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteRoutingRule, ruleTarget(args[0], ""), encoded, &answer)
	if err := argumentRefusal(err, server.CodeInvalidBody, server.CodeRoutingRuleRefused); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	r := answer.Rule
	if !answer.Appended {
		_, err = fmt.Fprintf(w, "Routing rule %s already says this at version %d, so nothing was appended.\n", r.Name, r.Sequence)
		return err
	}
	does := "drops each item " + matchWords(r.Match)
	if r.Destination != nil {
		does = "places each item " + matchWords(r.Match) + " on " + *r.Destination
	}
	_, err = fmt.Fprintf(w, "Accepted routing rule %s as version %d. It %s, and sends %s of them to review as rule-would-hide.\n",
		r.Name, r.Sequence, does, earnedtrust.Percent(r.ReviewShare))
	return err
}

func runGatesRoutingRulesRetire(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.RoutingRuleAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteRoutingRuleRetire, ruleTarget(args[0], "/retire"), []byte(`{}`), &answer)
	if err := argumentRefusal(err, server.CodeInvalidBody, server.CodeRoutingRuleNotFound); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	if !answer.Appended {
		_, err = fmt.Fprintf(w, "Routing rule %s was retired already, at version %d.\n", answer.Rule.Name, answer.Rule.Sequence)
		return err
	}
	_, err = fmt.Fprintf(w, "Retired routing rule %s as version %d. Each item it placed stays where it is, and each it sent to review still waits there.\n",
		answer.Rule.Name, answer.Rule.Sequence)
	return err
}

func ruleTarget(name, verb string) string {
	return "/api/v1/routing-rules/" + url.PathEscape(strings.TrimSpace(name)) + verb
}

// matchWords says what a match holds for, each condition joined by and.
func matchWords(m connectors.Match) string {
	var parts []string
	if m.Connector != "" {
		parts = append(parts, "that "+m.Connector+" sighted")
	}
	if m.Tag != "" {
		parts = append(parts, "tagged "+m.Tag)
	}
	if m.Field != "" {
		parts = append(parts, "whose "+m.Field+" is "+strings.Join(m.Values, " or "))
	}
	return strings.Join(parts, " and ")
}
