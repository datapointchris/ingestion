package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var reviewCmd = &cobra.Command{
	Use:   "review",
	Short: "Read the items that wait on a person, and place, sort or drop one",
	Example: "  # the items waiting on a person, the longest waiting first\n" +
		"  ingestion review list\n" +
		"\n" +
		"  # place one on a destination, naming what decided it\n" +
		"  ingestion review place reddit/t3_1h9xk2 reading-list --cause subreddit\n" +
		"\n" +
		"  # sort one by a verb, which places, reads or shelves it\n" +
		"  ingestion review sort youtube/aqz-KE-bpKQ watch --playlist \"Later\"",
}

var reviewListCmd = &cobra.Command{
	Use:   "list",
	Short: "List the items that wait on a person's placement, the longest waiting first",
	Long: "Lists each item that the side gate sent to review, and each one that holds\n" +
		"no usable parse, the longest waiting first. Each row shows the item's key,\n" +
		"its zone, why it waits, the quality gate's side guess where it gave one,\n" +
		"when it began to wait, and its title. A reason is one of:\n" +
		"\n" +
		reviewReasonLines() +
		"\n" +
		"A title is outsider text where the zone is outside.",
	Example: "  ingestion review list\n" +
		"  ingestion review list --json",
	Args: cobra.NoArgs,
	RunE: runReviewList,
}

// reviewReasonLines gives each of route.ReviewReasons its help line.
func reviewReasonLines() string {
	var b strings.Builder
	for _, r := range route.ReviewReasons {
		fmt.Fprintf(&b, "  %-15s %s\n", r.Name, r.Help)
	}
	return b.String()
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
		"answers. With no note, a read by a model answers a note that a model drafts\n" +
		"from the item's opening. An item whose draft fell below the note floor waits\n" +
		"as note-unclear. Placing an item that holds a draft with no --note takes that\n" +
		"draft. Placing an item again with the same destination, cause and note\n" +
		"changes nothing. A destination whose deliverer adds the item to a playlist is\n" +
		"refused and exits 2, because only a sort by watch picks the playlist.",
	Example: "  ingestion review place reddit/t3_1h9xk2 reading-list --cause subreddit\n" +
		"  ingestion review place reddit/t3_1h9xk2 work-inbox --note \"what would this change for retries?\"",
	Args: cobra.ExactArgs(2),
	RunE: runReviewPlace,
}

var reviewSortCmd = &cobra.Command{
	Use:   "sort <key or url> <verb>",
	Short: "Sort an item by a verb, which places it, reads it, shelves it, or adds it to a playlist",
	Long: "Sorts the item by a verb as your decision, recorded under person:<login> with\n" +
		"the verb as its cause. Each verb places the item on a side and says what runs:\n" +
		"\n" +
		verbLines() +
		"\n" +
		"A verb places the item on the destination whose deliverer it names, or for\n" +
		"fleet-relate on the fleet side's default, as review list --json lists under\n" +
		"verbs. A shelf verb places it on no destination, and nothing runs until\n" +
		"`ingestion shelf open <key>`. --playlist names the playlist that a watch adds\n" +
		"the video to, by its YouTube id or the title that ypl holds. Left out, a watch\n" +
		"takes serve's [watch] default_playlist. A verb whose destination the set in\n" +
		"force does not declare, or whose deliverer does not take the item, is refused\n" +
		"and exits 2. A watch is refused the same way where the item's delivery holds\n" +
		"another playlist. A sort that repeats the newest side decision, playlist\n" +
		"included, changes nothing.",
	Example: "  ingestion review sort youtube/aqz-KE-bpKQ learning\n" +
		"  ingestion review sort youtube/aqz-KE-bpKQ watch --playlist \"Later\"\n" +
		"  ingestion review sort reddit/t3_1h9xk2 personal-summarize",
	Args:      cobra.ExactArgs(2),
	ValidArgs: route.VerbNames(),
	RunE:      runReviewSort,
}

// verbLines gives each of route.Verbs its help line.
func verbLines() string {
	var b strings.Builder
	for _, v := range route.Verbs {
		fmt.Fprintf(&b, "  %-20s %s\n", v.Verb, v.Help)
	}
	return b.String()
}

var reviewDropCmd = &cobra.Command{
	Use:   "drop <key or url>",
	Short: "Drop an item, which keeps its row, and name what decided it",
	Long: "Drops the item as your decision, recorded under person:<login>. A drop is\n" +
		"soft: the item keeps its row and every record, and a later placement moves\n" +
		"it. --cause names the aspect that decided it, such as subreddit, channel,\n" +
		"theme or title, so a filter can learn from it. Left out, the cause is\n" +
		"unstated.",
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
	reviewSortCmd.Flags().String("playlist", "", "Name the playlist that a watch adds the video to")
	reviewSortCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{reviewListCmd, reviewPlaceCmd, reviewSortCmd, reviewDropCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	reviewCmd.AddCommand(reviewListCmd, reviewPlaceCmd, reviewSortCmd, reviewDropCmd)
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

func runReviewSort(cmd *cobra.Command, args []string) error {
	body := server.SortRequest{Verb: strings.TrimSpace(args[1])}
	if _, ok := route.VerbOf(body.Verb); !ok {
		return fmt.Errorf("%w: %q is no verb. A sort takes one of %s", goclikit.ErrUsage, body.Verb, strings.Join(route.VerbNames(), ", "))
	}
	body.Playlist, _ = cmd.Flags().GetString("playlist")
	return sendReview(cmd, server.RouteReviewSort, args[0], "sort", body)
}

func runReviewDrop(cmd *cobra.Command, args []string) error {
	body := server.DropRequest{}
	body.Cause, _ = cmd.Flags().GetString("cause")
	if note, _ := cmd.Flags().GetString("note"); note != "" {
		body.Note = &note
	}
	return sendReview(cmd, server.RouteReviewDrop, args[0], "drop", body)
}

// argumentRefusal is err as a usage error where the server refused what the
// command line named: an item it holds no record of, a key it cannot read, or
// a refusal whose code is one of codes. Every verb that names an item reads
// it, so a missing item exits 2 on each.
func argumentRefusal(err error, codes ...string) error {
	var refused *refusal
	if errors.As(err, &refused) && refused.Status < http.StatusInternalServerError &&
		(refused.Code == server.CodeItemNotFound || refused.Code == server.CodeInvalidKey || slices.Contains(codes, refused.Code)) {
		return fmt.Errorf("%w: %s", goclikit.ErrUsage, refused.Message)
	}
	return err
}

// sendReview posts body to the item's verb under route, and prints what the
// decisions it made placed.
func sendReview(cmd *cobra.Command, route server.Route, key, verb string, body any) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	answer, raw, err := decideReview(cmd.Context(), configPath(cmd), api, route, key, verb, body)
	if err := argumentRefusal(err, server.CodeDestinationNotDeclared, server.CodeDestinationPushOnly, server.CodeDestinationTakesPlaylist,
		server.CodeChapterOfBook, server.CodeInvalidBody, server.CodeVerbRefused); err != nil {
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

// decideReview posts body to the item's verb under route, on the server the
// config file at path names, or api where it is set. It answers the
// decisions the server made and the body as it came.
func decideReview(ctx context.Context, path, api string, route server.Route, key, verb string, body any) (server.PlacementAnswer, []byte, error) {
	var answer server.PlacementAnswer
	encoded, err := json.Marshal(body)
	if err != nil {
		return answer, nil, err
	}
	raw, err := sendJSON(ctx, path, api, route, "/api/v1/review/"+url.PathEscape(strings.TrimSpace(key))+"/"+verb, encoded, &answer)
	return answer, raw, err
}

// placedSentence says where the answer's decisions put the item, and how it
// is read.
func placedSentence(a server.PlacementAnswer) string {
	side := a.Decisions[0]
	var read *server.DecisionView
	for i, d := range a.Decisions[1:] {
		if d.Gate == store.GateRead {
			read = &a.Decisions[i+1]
		}
	}
	switch {
	case read != nil && read.Verdict == store.VerdictShelf:
		return fmt.Sprintf("Shelved %s by %s: nothing runs until ingestion shelf open %s.", a.Key, side.Cause, a.Key)
	case side.Destination == nil:
		return fmt.Sprintf("Dropped %s, cause %s.", a.Key, side.Cause)
	case side.Verdict != connectors.SidePersonal && side.Verdict != connectors.SideFleet:
		return fmt.Sprintf("Placed %s on %s, which takes no side, so it is dropped.", a.Key, *side.Destination)
	case read != nil && read.Cause == route.CauseLearningRefused:
		return fmt.Sprintf("%s is back in review as %s: the quality gate's newest answer did not pass it, so nothing goes to %s.",
			a.Key, route.CauseLearningRefused, *side.Destination)
	}
	how := "unread"
	switch {
	case read == nil:
	case read.Lens != nil:
		how = "read by a model under the " + *read.Lens + " lens"
	default:
		how = "read " + read.Verdict
	}
	to := *side.Destination
	if side.Playlist != nil {
		to += " in the playlist " + *side.Playlist
	}
	return fmt.Sprintf("Placed %s on %s: side %s, %s.", a.Key, to, side.Verdict, how)
}
