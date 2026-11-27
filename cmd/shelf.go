package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/gotermboard/terminalcontrol"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
)

var shelfCmd = &cobra.Command{
	Use:   "shelf",
	Short: "Read the items sorted to the shelf, open one, which runs what its verb names, or take one off",
	Long: "A sort by " + strings.Join(route.ShelfVerbs(), ", ") + " puts an item on the\n" +
		"shelf, where nothing runs until you open it. Opening a relate hands the item's\n" +
		"parse to " + push.Program + " relate on this machine. Opening a summary queues one model\n" +
		"read under its lens and keeps it, so a second opening shows the same summary with\n" +
		"no model call. An opened item stays on the shelf until `ingestion shelf remove`\n" +
		"takes it off.",
	Example: "  ingestion shelf list\n" +
		"  ingestion shelf open youtube/aqz-KE-bpKQ\n" +
		"  ingestion shelf remove youtube/aqz-KE-bpKQ",
}

var shelfListCmd = &cobra.Command{
	Use:   "list",
	Short: "List the items on the shelf, the oldest landed first",
	Long: "Lists each item on the shelf: its key, the verb that shelved it, where it\n" +
		"stands, when it was sorted, and its title. A state is one of:\n" +
		"\n" +
		"  waiting   nobody opened it, so nothing ran\n" +
		"  opened    a relate that you opened, which ran on your machine\n" +
		"  reading   a summary whose model read is queued or running\n" +
		"  done      a summary whose model read is kept\n" +
		"  failed    a summary whose model read failed, which `ingestion tasks retry` runs again\n" +
		"\n" +
		"A title is outsider text where the zone is outside.",
	Example: "  ingestion shelf list\n" +
		"  ingestion shelf list --json",
	Args: cobra.NoArgs,
	RunE: runShelfList,
}

var shelfOpenCmd = &cobra.Command{
	Use:   "open <key or url>",
	Short: "Open a shelved item: relate it in " + push.Program + " here, or read its summary",
	Long: "Opens the item as your decision, recorded under person:<login> with cause\n" +
		route.CauseShelfOpen + ". A relate then reads the item's parse and its scan from the\n" +
		"service, and runs " + push.Program + " relate on this machine, as `ingestion push <key> --to\n" +
		push.Program + " --relate` does. Each opening of a relate runs it again here, and the\n" +
		"service keeps nothing of it.\n" +
		"\n" +
		"A summary's first opening queues one model read under its lens, and prints that\n" +
		"it is reading. Open it again once it is done to print its summary and its\n" +
		"suggestion, which a model wrote over the item's text. A later opening queues\n" +
		"nothing. --json prints the service's answer, or for a relate " + push.Program + "'s answer as\n" +
		"JSON. An item that is not on the shelf exits 2, and a summary whose read failed\n" +
		"exits 1.",
	Example: "  ingestion shelf open youtube/aqz-KE-bpKQ\n" +
		"  ingestion shelf open reddit/t3_1h9xk2 --json",
	Args: cobra.ExactArgs(1),
	RunE: runShelfOpen,
}

var shelfRemoveCmd = &cobra.Command{
	Use:   "remove <key or url>",
	Short: "Take an item off the shelf, keeping everything it holds",
	Long: "Takes the item off the shelf as your decision, recorded under person:<login>\n" +
		"with cause " + route.CauseShelfRemove + ". The decision keeps the newest read's verdict and\n" +
		"lens, so a summary's model read already queued runs on, and a kept summary stays\n" +
		"in the item, which `ingestion items show` prints. Removing it again changes\n" +
		"nothing. An item that is not on the shelf exits 2.",
	Example: "  ingestion shelf remove youtube/aqz-KE-bpKQ",
	Args:    cobra.ExactArgs(1),
	RunE:    runShelfRemove,
}

func init() {
	goclikit.AsNamespace(shelfCmd)
	shelfListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	shelfOpenCmd.Flags().Bool("json", false, "Print the service's answer, or for a relate "+push.Program+"'s, as JSON")
	shelfRemoveCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{shelfListCmd, shelfOpenCmd, shelfRemoveCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	shelfCmd.AddCommand(shelfListCmd, shelfOpenCmd, shelfRemoveCmd)
	rootCmd.AddCommand(shelfCmd)
}

func runShelfList(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.ShelfAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteShelf, "/api/v1/shelf", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "ITEM\tVERB\tSTATE\tSORTED\tTITLE")
	for _, it := range answer.Items {
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", it.Key, it.Verb, it.State, localTime(it.SortedTS), terminalcontrol.Strip(orDash(it.Title)))
	}
	if len(answer.Items) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	return table.Flush()
}

// openShelved posts the opening of the item under key, on the server the
// config file at path names, or api where it is set, and answers the
// service's answer and its body as it came.
func openShelved(ctx context.Context, path, api, key string) (server.ShelfOpenAnswer, []byte, error) {
	var answer server.ShelfOpenAnswer
	raw, err := sendJSON(ctx, path, api, server.RouteShelfOpen, "/api/v1/shelf/"+url.PathEscape(strings.TrimSpace(key))+"/open", []byte("{}"), &answer)
	return answer, raw, err
}

func runShelfOpen(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	ctx, config := cmd.Context(), configPath(cmd)
	answer, raw, err := openShelved(ctx, config, api, args[0])
	if err := argumentRefusal(err, server.CodeNotShelved); err != nil {
		return err
	}
	out, stderr := cmd.OutOrStdout(), cmd.ErrOrStderr()
	it := answer.Item
	if it.OpensHere {
		_, _ = fmt.Fprintf(stderr, "opened %s, sorted by %s: handing it to %s relate\n", it.Key, it.Verb, push.Program)
		return relateShelved(ctx, config, api, it.Key, asJSON, out, stderr)
	}
	if asJSON {
		_, err := out.Write(raw)
		return err
	}
	return writeShelfSummary(out, answer)
}

// relateShelved hands the shelved item under key to psyche relate here, as a
// push of a landed item does.
func relateShelved(ctx context.Context, config, api, key string, asJSON bool, out, stderr io.Writer) error {
	target := "/api/v1/items/" + url.PathEscape(key)
	var parse server.ParseAnswer
	_, err := getJSON(ctx, config, api, server.RouteItemParse, target+"/parse", &parse)
	var refused *refusal
	if err != nil && (!errors.As(err, &refused) || refused.Code != server.CodeParseNotFound || refused.Status >= http.StatusInternalServerError) {
		return argumentRefusal(err)
	}
	return relateParse(ctx, config, api, target, parse, push.Wait, false, asJSON, out, stderr)
}

// writeShelfSummary prints where an opened summary stands, and the summary
// once it is kept.
func writeShelfSummary(w io.Writer, a server.ShelfOpenAnswer) error {
	it := a.Item
	switch it.State {
	case route.ShelfDone:
		var read struct {
			Summary    *string `json:"summary"`
			Suggestion *string `json:"suggestion"`
		}
		if err := json.Unmarshal(it.Summary, &read); err != nil {
			return fmt.Errorf("%s: the kept summary does not read: %w", it.Key, err)
		}
		_, _ = fmt.Fprintf(w, "%s, %s under the %s lens:\n\n", it.Key, it.Verb, *it.Lens)
		if read.Summary != nil {
			_, _ = fmt.Fprintf(w, "%s\n", terminalcontrol.Strip(*read.Summary))
		}
		if read.Suggestion != nil {
			_, _ = fmt.Fprintf(w, "\nSuggestion: %s\n", terminalcontrol.Strip(*read.Suggestion))
		}
		return nil
	case route.ShelfFailed:
		why := "its task failed"
		if it.Error != nil {
			why = *it.Error
		}
		return fmt.Errorf("the model read of %s under the %s lens failed: %s. `ingestion items show %s` names its task, which `ingestion tasks retry` runs again",
			it.Key, *it.Lens, why, it.Key)
	}
	opened := "Opened"
	if !a.Opened {
		opened = "Already opened"
	}
	_, err := fmt.Fprintf(w, "%s %s: a model reads it under the %s lens. Open it again once it is done to read its summary.\n", opened, it.Key, *it.Lens)
	return err
}

// removeShelved posts the removal of the item under key from the shelf, on
// the server the config file at path names, or api where it is set, and
// answers the service's answer and its body as it came.
func removeShelved(ctx context.Context, path, api, key string) (server.ShelfRemoveAnswer, []byte, error) {
	var answer server.ShelfRemoveAnswer
	raw, err := sendJSON(ctx, path, api, server.RouteShelfRemove, "/api/v1/shelf/"+url.PathEscape(strings.TrimSpace(key))+"/remove", []byte("{}"), &answer)
	return answer, raw, err
}

// removedSentence says what a removal did.
func removedSentence(a server.ShelfRemoveAnswer) string {
	if !a.Removed {
		return a.Key + " was already off the shelf, so nothing changed."
	}
	return "Took " + a.Key + " off the shelf."
}

func runShelfRemove(cmd *cobra.Command, args []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	answer, raw, err := removeShelved(cmd.Context(), configPath(cmd), api, args[0])
	if err := argumentRefusal(err, server.CodeNotShelved); err != nil {
		return err
	}
	if asJSON {
		_, err := cmd.OutOrStdout().Write(raw)
		return err
	}
	_, err = fmt.Fprintln(cmd.OutOrStdout(), removedSentence(answer))
	return err
}
