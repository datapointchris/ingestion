package cmd

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/earnedtrust"
	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/server"
)

var gatesCmd = &cobra.Command{
	Use:   "gates",
	Short: "Read how the quality gate and the side gate decide, and how far the quality gate is trusted",
	Example: "  # how often each gate's answer agrees with a person's placement\n" +
		"  ingestion gates agreement\n" +
		"  # where each family of the quality gate stands on its trust ladder\n" +
		"  ingestion gates trust list",
}

var gatesAgreementCmd = &cobra.Command{
	Use:   "agreement",
	Short: "Print how often each gate's answer agrees with a person's placement from review",
	Long: "Compares the quality gate's answer on each labeled item with the label. The\n" +
		"label is a person's placement of an item that the gates sent to review first.\n" +
		"A placement that no review came before labels nothing, as one made before the\n" +
		"gates ran. A pass agrees with a placement on a side, and a\n" +
		"drop with a person's drop. The side guess agrees where it names the person's\n" +
		"side. An unsure answer neither agrees nor disagrees, and agreement is agreed\n" +
		"over agreed and disagreed.\n" +
		"\n" +
		"Each answer counts as the model gave it, before a floor turns a drop or a\n" +
		"guess into review, so the rows by confidence band show where a floor would\n" +
		"sit. Each gate breaks down by band, connector, model and prompt version. A\n" +
		"labeled item that the gate never answered, as one it sent to review for want\n" +
		"of a rule, holds no answer, and `ingestion items replay --stage quality\n" +
		"--placed-by person` asks the gate for one.\n" +
		"\n" +
		"Each arm, the gate's prompt asked of another model by `ingestion items replay\n" +
		"--stage quality-arm --model <model>`, prints after the gate. Its answers sit\n" +
		"beside the gate's answers to the same prompt on the same items.",
	Example: "  ingestion gates agreement\n" +
		"  ingestion gates agreement --json",
	Args: cobra.NoArgs,
	RunE: runGatesAgreement,
}

var gatesTrustCmd = &cobra.Command{
	Use:   "trust",
	Short: "Read and promote how far each family of the quality gate is trusted to hide its drops",
	Long: "A family is one connector under one version of the quality gate's prompt,\n" +
		"answered by one model at one effort. While a family stands on rung 0, every item that the gate drops waits in\n" +
		"review as gate-would-drop. Your marks move it. low-quality on a drop agrees\n" +
		"and climbs. read on a drop, or low-quality on a pass, disagrees. read on a\n" +
		"pass is kept and moves nothing, because a pass shows its item on any rung.\n" +
		"Placing on a destination an item that the gate dropped disagrees too,\n" +
		"whether its drop showed in review or was hidden. Any disagreement returns\n" +
		"the family to rung 0.\n" +
		"\n" +
		"A straight run of agreeing marks earns asking, and one yes promotes the\n" +
		"family. A promoted family climbs on the drops that the gate answers after\n" +
		"the yes. It shows its rung's share of the gate's drops, picked by a seeded\n" +
		"draw over the family and the gate's answer. It drops the rest as\n" +
		"quality-drop, which keeps their rows. A new prompt version, model or effort\n" +
		"starts its families on rung 0.",
	Example: "  ingestion gates trust list\n" +
		"  ingestion gates trust promote reddit-exports@1-3f9a0b2c@claude-haiku-4-5@medium",
}

var gatesTrustListCmd = &cobra.Command{
	Use:   "list",
	Short: "Print each gate family's rung, straight run and the share of drops it shows",
	Long: "Prints each family by connector, prompt version, model and effort: its\n" +
		"rung, its run on the rung, its straight run since the last disagreement\n" +
		"against the run that earns asking, the share of the gate's drops it shows,\n" +
		"and who promoted it.\n" +
		"ASKS reads yes on a family whose straight run earned asking and that\n" +
		"nobody has promoted.",
	Example: "  ingestion gates trust list\n" +
		"  ingestion gates trust list --json",
	Args: cobra.NoArgs,
	RunE: runGatesTrustList,
}

var gatesTrustPromoteCmd = &cobra.Command{
	Use:   "promote <family>",
	Short: "Promote a gate family, so it hides a share of its drops once its straight run earns it",
	Long: "Records your yes on the family, under person:<login>. A family whose\n" +
		"straight run has reached the run that earns asking climbs to rung 1 at once,\n" +
		"and shows the first share of its drops. One promoted before that climbs once\n" +
		"its run reaches it, with no second yes. A disagreement later returns it to\n" +
		"rung 0, and it climbs back to rung 1 on its own once its run reaches the\n" +
		"threshold again. Promoting a promoted family changes nothing.",
	Example: "  ingestion gates trust promote reddit-exports@1-3f9a0b2c@claude-haiku-4-5@medium",
	Args:    cobra.ExactArgs(1),
	RunE:    runGatesTrustPromote,
}

func init() {
	goclikit.AsNamespace(gatesCmd)
	goclikit.AsNamespace(gatesTrustCmd)
	for _, c := range []*cobra.Command{gatesAgreementCmd, gatesTrustListCmd, gatesTrustPromoteCmd} {
		c.Flags().Bool("json", false, "Print the service's answer as JSON")
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	gatesTrustCmd.AddCommand(gatesTrustListCmd, gatesTrustPromoteCmd)
	gatesCmd.AddCommand(gatesAgreementCmd, gatesTrustCmd)
	rootCmd.AddCommand(gatesCmd)
}

func runGatesTrustList(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.GateTrustAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteGateTrust, "/api/v1/gates/trust", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	shares := make([]string, 0, len(answer.Rule.Shares))
	for _, s := range answer.Rule.Shares {
		shares = append(shares, earnedtrust.Percent(s))
	}
	_, _ = fmt.Fprintf(w, "A straight run of %d agreeing marks earns asking. Rungs 1 up show %s of the gate's drops.\n\n",
		answer.Rule.Run, strings.Join(shares, ", "))
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "FAMILY\tRUNG\tRUN\tSTRAIGHT RUN\tSHOWS\tASKS\tPROMOTED BY")
	for _, g := range answer.Families {
		asks := "no"
		if g.AsksPromotion {
			asks = "yes"
		}
		_, _ = fmt.Fprintf(table, "%s\t%d\t%d\t%d of %d\t%s\t%s\t%s\n", g.Family, g.Rung, g.Run, g.StraightRun, g.Threshold,
			earnedtrust.Percent(g.Share), asks, orDash(g.PromotedBy))
	}
	if len(answer.Families) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	return table.Flush()
}

func runGatesTrustPromote(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.PromotionAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteGateTrustPromote,
		"/api/v1/gates/trust/"+url.PathEscape(args[0])+"/promote", []byte(`{}`), &answer)
	if err := argumentRefusal(err, server.CodeFamilyNotFound); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	g := answer.Family
	if g.Rung == 0 {
		_, err = fmt.Fprintf(w, "Promoted %s. Its straight run is %d of %d, so it shows every drop until the run reaches %d.\n",
			g.Family, g.StraightRun, g.Threshold, g.Threshold)
		return err
	}
	_, err = fmt.Fprintf(w, "Promoted %s to rung %d, which shows %s of the gate's drops and drops the rest as quality-drop.\n",
		g.Family, g.Rung, earnedtrust.Percent(g.Share))
	return err
}

func runGatesAgreement(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var report accuracy.Report
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteGateAgreement, "/api/v1/gates/agreement", &report)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	l := report.Labels
	_, _ = fmt.Fprintf(w, "%d items a person placed from review: %d personal, %d fleet, %d dropped. %d hold no quality answer.\n",
		l.Items, l.Personal, l.Fleet, l.Drop, report.Unanswered)
	writeGate(w, "quality gate, drop floor", report.Quality)
	writeGate(w, "side guess, floor", report.Side)
	for _, arm := range report.Arms {
		_, _ = fmt.Fprintf(w, "\n%s arm on %d labeled items, beside the gate's answers to the same prompt\n", arm.Model, arm.Items)
		writeGate(w, arm.Model+" quality verdict, drop floor", arm.Arm.Quality)
		writeGate(w, "the gate's quality verdict on the same items, drop floor", arm.Gate.Quality)
		writeGate(w, arm.Model+" side guess, floor", arm.Arm.Side)
		writeGate(w, "the gate's side guess on the same items, floor", arm.Gate.Side)
	}
	return nil
}

// writeGate prints one gate's agreement as a table, a row overall and one for
// each band, connector, model and prompt version.
func writeGate(w io.Writer, title string, g accuracy.Gate) {
	_, _ = fmt.Fprintf(w, "\n%s %.2f\n", title, g.Floor)
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "BY\tCOMPARED\tAGREED\tDISAGREED\tUNSURE\tAGREEMENT\tANSWER BY PLACEMENT")
	row := func(by string, c accuracy.Counts) {
		_, _ = fmt.Fprintf(table, "%s\t%d\t%d\t%d\t%d\t%s\t%s\n", by, c.Comparisons, c.Agreed, c.Disagreed, c.Unsure, percent(c.Agreement), pairs(c.Answers))
	}
	row("all", g.Overall)
	for _, b := range g.ByConfidence {
		row(fmt.Sprintf("confidence %.1f-%.1f", b.From, b.To), b.Counts)
	}
	for _, c := range g.ByConnector {
		row("connector "+c.Connector, c.Counts)
	}
	for _, m := range g.ByModel {
		row("model "+m.Model, m.Counts)
	}
	for _, v := range g.ByPromptVersion {
		row("prompt "+versionWords(v.PromptVersion), v.Counts)
	}
	_ = table.Flush()
}

func percent(share *float64) string {
	if share == nil {
		return "—"
	}
	return fmt.Sprintf("%.0f%%", *share*100)
}

// pairs is each answer beside each placement, such as pass/personal 12.
func pairs(answers []accuracy.Pair) string {
	parts := make([]string, 0, len(answers))
	for _, p := range answers {
		parts = append(parts, fmt.Sprintf("%s/%s %d", p.Answer, p.Label, p.Count))
	}
	return strings.Join(parts, ", ")
}
