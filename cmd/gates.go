package cmd

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/server"
)

var gatesCmd = &cobra.Command{
	Use:   "gates",
	Short: "Read how the quality gate and the side gate decide",
	Example: "  # how often each gate's answer agrees with a person's placement\n" +
		"  ingestion gates agreement",
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

func init() {
	goclikit.AsNamespace(gatesCmd)
	gatesAgreementCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	gatesAgreementCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	addConfigFlag(gatesAgreementCmd)
	gatesCmd.AddCommand(gatesAgreementCmd)
	rootCmd.AddCommand(gatesCmd)
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
