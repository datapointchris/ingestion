package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var reviewCmd = &cobra.Command{
	Use:   "review",
	Short: "Read the items that wait on a person, and place or drop one",
}

var reviewListCmd = &cobra.Command{
	Use:   "list",
	Short: "List the items that wait on a person's placement, the longest waiting first",
	Long: "Lists each item that no tag, rule or confident guess placed, and each one\n" +
		"that holds no usable parse, the longest waiting first. Each row shows the\n" +
		"item's key, its zone, why it waits, the quality gate's side guess where it\n" +
		"gave one, when it began to wait, and its title. A reason is one of:\n" +
		"\n" +
		"  tags-disagree  its sightings' tags name destinations on two sides\n" +
		"  below-floor    the quality gate's guess fell below its confidence floor\n" +
		"  no-guess       no tag or rule placed it, and the quality gate gave no guess\n" +
		"  no-parse       no parse of it is usable\n" +
		"\n" +
		"A title is outsider text where the zone is outside.",
	Example: "  ingestion review list\n" +
		"  ingestion review list --json",
	Args: cobra.NoArgs,
	RunE: runReviewList,
}

var reviewPlaceCmd = &cobra.Command{
	Use:   "place <key or url> <destination>",
	Short: "Place an item on a destination, and print the side and read it takes",
	Long: "Places the item on a destination that the set in force declares, as your\n" +
		"decision, recorded under person:<login>. The item takes the destination's\n" +
		"side, and the read gate then chooses whether it is read as is or by a model.\n" +
		"A destination that takes no side drops the item, which keeps its row. Any\n" +
		"item can be placed, including one that a rule already placed.\n" +
		"\n" +
		"--cause names the aspect that decided it, such as subreddit, channel, theme\n" +
		"or title, so a filter can learn from it. Left out, the cause is unstated.\n" +
		"--note carries the questions and why the item was saved, which a model read\n" +
		"answers. Placing an item again with the same destination, cause and note\n" +
		"changes nothing.",
	Example: "  ingestion review place reddit/t3_1h9xk2 reading-list --cause subreddit\n" +
		"  ingestion review place reddit/t3_1h9xk2 work-inbox --note \"what would this change for retries?\"",
	Args: cobra.ExactArgs(2),
	RunE: runReviewPlace,
}

var reviewDropCmd = &cobra.Command{
	Use:   "drop <key or url> --cause <cause>",
	Short: "Drop an item, which keeps its row, and name what decided it",
	Long: "Drops the item as your decision, recorded under person:<login>. A drop is\n" +
		"soft: the item keeps its row and every record, and a later placement moves\n" +
		"it. --cause names the aspect that decided it, such as subreddit, channel,\n" +
		"theme or title, so a filter can learn from it.",
	Example: "  ingestion review drop reddit/t3_1hb0qa --cause subreddit",
	Args:    cobra.ExactArgs(1),
	RunE:    runReviewDrop,
}

func init() {
	goclikit.AsNamespace(reviewCmd)
	reviewListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{reviewPlaceCmd, reviewDropCmd} {
		c.Flags().String("cause", "", "Name the aspect that decided it, such as subreddit, channel, theme or title")
		c.Flags().String("note", "", "Carry the questions and why the item was saved, for a model read to answer")
		c.Flags().Bool("json", false, "Print the service's answer as JSON")
	}
	_ = reviewDropCmd.MarkFlagRequired("cause")
	for _, c := range []*cobra.Command{reviewListCmd, reviewPlaceCmd, reviewDropCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	reviewCmd.AddCommand(reviewListCmd, reviewPlaceCmd, reviewDropCmd)
	rootCmd.AddCommand(reviewCmd)
}

func runReviewList(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.ReviewAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteReview, "/api/v1/review", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		_, err := w.Write(raw)
		return err
	}
	writeReview(w, answer.Items)
	return nil
}

func writeReview(w io.Writer, items []server.ReviewItem) {
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(table, "ITEM\tZONE\tREASON\tGUESS\tWAITING SINCE\tTITLE")
	for _, it := range items {
		guess := "—"
		if it.Guess != nil {
			guess = fmt.Sprintf("%s %.2f", it.Guess.Side, it.Guess.Confidence)
		}
		_, _ = fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\n", it.Key, it.Zone, it.Reason, guess, localTime(it.WaitingSinceTS), orDash(it.Title))
	}
	if len(items) == 0 {
		_, _ = fmt.Fprintln(table, "(none)")
	}
	_ = table.Flush()
}

func runReviewPlace(cmd *cobra.Command, args []string) error {
	body := server.PlaceRequest{Destination: args[1]}
	body.Cause, _ = cmd.Flags().GetString("cause")
	if note, _ := cmd.Flags().GetString("note"); note != "" {
		body.Note = &note
	}
	return sendReview(cmd, server.RouteReviewPlace, args[0], "place", body)
}

func runReviewDrop(cmd *cobra.Command, args []string) error {
	body := server.DropRequest{}
	body.Cause, _ = cmd.Flags().GetString("cause")
	if note, _ := cmd.Flags().GetString("note"); note != "" {
		body.Note = &note
	}
	return sendReview(cmd, server.RouteReviewDrop, args[0], "drop", body)
}

// sendReview posts body to the item's verb under route, and prints what the
// decisions it made placed.
func sendReview(cmd *cobra.Command, route server.Route, key, verb string, body any) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	var answer server.PlacementAnswer
	raw, err := sendJSON(cmd.Context(), configPath(cmd), api, route,
		"/api/v1/review/"+url.PathEscape(strings.TrimSpace(key))+"/"+verb, encoded, &answer)
	var refused *refusal
	if errors.As(err, &refused) && refused.Status != http.StatusInternalServerError &&
		(refused.Code == server.CodeItemNotFound || refused.Code == server.CodeInvalidKey ||
			refused.Code == server.CodeDestinationNotDeclared || refused.Code == server.CodeInvalidBody) {
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
	_, err = fmt.Fprintln(w, placedSentence(answer))
	return err
}

// placedSentence says where the answer's decisions put the item, and how it
// is read.
func placedSentence(a server.PlacementAnswer) string {
	side := a.Decisions[0]
	switch {
	case side.Destination == nil:
		return fmt.Sprintf("Dropped %s, cause %s.", a.Key, side.Cause)
	case side.Verdict != connectors.SidePersonal && side.Verdict != connectors.SideFleet:
		return fmt.Sprintf("Placed %s on %s, which takes no side, so it is dropped.", a.Key, *side.Destination)
	}
	read := "unread"
	for _, d := range a.Decisions[1:] {
		if d.Gate != store.GateRead {
			continue
		}
		read = "read " + d.Verdict
		if d.Lens != nil {
			read = "read by a model under the " + *d.Lens + " lens"
		}
	}
	return fmt.Sprintf("Placed %s on %s: side %s, %s.", a.Key, *side.Destination, side.Verdict, read)
}
