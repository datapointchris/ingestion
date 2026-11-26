package cmd

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/server"
)

var itemsMarkCmd = &cobra.Command{
	Use:   "mark <key or url> <low-quality|read>",
	Short: "Mark an item low-quality or read, which teaches the quality gate's trust",
	Long: "Marks the item as your word, recorded under person:<login>. low-quality says\n" +
		"it held nothing worth keeping. read says you read it and it gave what you\n" +
		"wanted. A mark moves nothing: the item stays where it is placed.\n" +
		"\n" +
		"Each mark reads against the quality gate's newest decision on the item.\n" +
		"low-quality agrees with a drop and disagrees with a pass, and read the\n" +
		"other way round. An agreement with a drop counts toward each family of\n" +
		"that decision, one connector under one version of the gate's prompt, and\n" +
		"a disagreement returns each to rung 0. An agreement with a pass is kept\n" +
		"and moves no family, because a pass shows its item whatever a family's\n" +
		"standing. `ingestion gates trust list` shows where each stands. Marking\n" +
		"an item again with the mark it holds changes nothing until the gate\n" +
		"answers it again.",
	Example: "  ingestion items mark reddit/t3_1h9xk2 low-quality\n" +
		"  ingestion items mark youtube/dQw4w9WgXcQ read --json",
	Args: cobra.ExactArgs(2),
	RunE: runItemsMark,
}

func init() {
	itemsMarkCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	itemsMarkCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	addConfigFlag(itemsMarkCmd)
	itemsCmd.AddCommand(itemsMarkCmd)
}

func runItemsMark(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	encoded, err := json.Marshal(server.MarkRequest{Mark: args[1]})
	if err != nil {
		return err
	}
	var answer server.MarkAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, server.RouteItemMarks,
		"/api/v1/items/"+url.PathEscape(strings.TrimSpace(args[0]))+"/marks", encoded, &answer)
	if err := argumentRefusal(err, server.CodeInvalidBody); err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	_, err = fmt.Fprintln(w, markedSentence(answer))
	return err
}

// markedSentence says what the mark read against and which families it moved.
func markedSentence(a server.MarkAnswer) string {
	m := a.Mark
	if m.GateVerdict == nil {
		return fmt.Sprintf("Marked %s %s. The quality gate has not decided it, so no family moved.", m.ItemKey, m.Mark)
	}
	if m.Agreement == gatetrust.Neither {
		return fmt.Sprintf("Marked %s %s. The quality gate's %s is neither kept nor dropped, so no family moved.", m.ItemKey, m.Mark, *m.GateVerdict)
	}
	if len(a.Families) == 0 {
		return fmt.Sprintf("Marked %s %s, which %s with the quality gate's %s. A pass shows its item whatever a family's standing, so no family moved.",
			m.ItemKey, m.Mark, agreementWord(m.Agreement), *m.GateVerdict)
	}
	moved := make([]string, 0, len(a.Families))
	for _, g := range a.Families {
		moved = append(moved, fmt.Sprintf("%s rung %d, straight run %d of %d", g.Family, g.Rung, g.StraightRun, g.Threshold))
	}
	return fmt.Sprintf("Marked %s %s, which %s with the quality gate's %s: %s.", m.ItemKey, m.Mark, agreementWord(m.Agreement), *m.GateVerdict,
		strings.Join(moved, "; "))
}

func agreementWord(agreement string) string {
	if agreement == gatetrust.Agreed {
		return "agrees"
	}
	return "disagrees"
}
