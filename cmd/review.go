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
	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

var reviewCmd = &cobra.Command{
	Use:   "review",
	Short: "Read the items that wait on a person, and place, archive or drop one",
	Example: "  # the items waiting on a person, the longest waiting first\n" +
		"  ingestion review list\n" +
		"\n" +
		"  # place one by a choice, naming what decided it\n" +
		"  ingestion review place reddit/t3_1h9xk2 articles --cause subreddit\n" +
		"  ingestion review place youtube/aqz-KE-bpKQ watch --playlist \"Later\"\n" +
		"\n" +
		"  # archive one you are done with, naming what you liked\n" +
		"  ingestion review archive youtube/aqz-KE-bpKQ --liked channel",
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
	Use:   "place <key or url> <choice or destination>",
	Short: "Place an item by one of review's choices, and print the side and read it takes",
	Long: "Places the item by a choice as your decision, recorded under person:<login>,\n" +
		"with the choice as the read decision's cause. Each choice places the item on\n" +
		"a side and a destination, and says what runs:\n" +
		"\n" +
		choiceLines(route.PlacingChoiceNames()) +
		"\n" +
		"A choice places the item on the destination whose deliverer it names, for\n" +
		"suggestion on the fleet side's default, and for psyche on the destination\n" +
		"named psyche, as review list --json lists under choices. A name that is no\n" +
		"choice names a destination of the set in force, and places the item on it\n" +
		"with the side it takes. A name that is both a choice's and a destination's\n" +
		"is read as the choice. The push-only psyche and the watch, which picks a\n" +
		"playlist, take only their choices. Any item can be placed, including one that\n" +
		"a rule already placed.\n" +
		"\n" +
		"psyche hands the item's parse and its scan's findings to psyche relate on this\n" +
		"machine once the placement lands, as `ingestion push --to psyche --relate`\n" +
		"does, waiting up to " + push.Wait.String() + " for the scan. The sentence saying where it went goes\n" +
		"to stderr, psyche's answer to stdout, and --json asks psyche for its JSON.\n" +
		"Where psyche fails, the item stays placed, and that push runs it again.\n" +
		"\n" +
		"--playlist names the playlist that a watch adds the video to, by its YouTube\n" +
		"id or the title that ypl holds. Left out, a watch takes serve's [watch]\n" +
		"default_playlist. --cause names the aspect that decided it, such as\n" +
		"subreddit, channel, theme or title, so a filter can learn from it. Left out,\n" +
		"the cause is unstated. --note carries the questions and why the item was\n" +
		"saved, which a suggestion's model read answers. With no note, a suggestion\n" +
		"answers a note that a model drafts from the item's opening, and takes a draft\n" +
		"already done. A choice whose destination the set in force does not declare,\n" +
		"or whose deliverer does not take the item, is refused and exits 2. A watch is\n" +
		"refused the same way where the item's delivery holds another playlist.\n" +
		"Placing an item again with the same choice, cause, note and playlist changes\n" +
		"nothing.",
	Example: "  ingestion review place reddit/t3_1h9xk2 articles --cause subreddit\n" +
		"  ingestion review place reddit/t3_1h9xk2 suggestion --note \"what would this change for retries?\"\n" +
		"  ingestion review place youtube/aqz-KE-bpKQ watch --playlist \"Later\"\n" +
		"  ingestion review place youtube/aqz-KE-bpKQ psyche\n" +
		"  ingestion review place reddit/t3_1h9xk2 reading-list",
	Args:      cobra.ExactArgs(2),
	ValidArgs: route.PlacingChoiceNames(),
	RunE:      runReviewPlace,
}

// choiceLines gives each choice that names names its help line.
func choiceLines(names []string) string {
	var b strings.Builder
	for _, name := range names {
		c, _ := route.ChoiceOf(name)
		fmt.Fprintf(&b, "  %-11s %s\n", c.Name, c.Help)
	}
	return b.String()
}

var reviewArchiveCmd = &cobra.Command{
	Use:   "archive <key or url>",
	Short: "Archive an item you are done with, keeping its record with no destination",
	Long: "Archives the item as your decision, recorded under person:<login>: done, such\n" +
		"as a video watched elsewhere. It takes the personal side and no destination,\n" +
		"and its read decision is archive, so nothing reads or delivers it. It also\n" +
		"writes your read mark, which teaches the quality gate as `ingestion items mark`\n" +
		"does.\n" +
		"\n" +
		"--liked names what you liked, which the archive keeps as its cause, so a\n" +
		"reader of the decisions learns what to keep from it: " + strings.Join(route.Likes, ", ") + ".\n" +
		"Left out, the cause is unstated. Archiving an item again with the same like changes\n" +
		"nothing.",
	Example: "  ingestion review archive youtube/aqz-KE-bpKQ --liked channel\n" +
		"  ingestion review archive reddit/t3_1h9xk2",
	Args: cobra.ExactArgs(1),
	RunE: runReviewArchive,
}

var reviewDropCmd = &cobra.Command{
	Use:   "drop <key or url>",
	Short: "Drop an item, which keeps its row, and name what decided it",
	Long: "Drops the item as your decision, recorded under person:<login>. A drop is\n" +
		"soft: the item keeps its row and every record, and a later placement moves\n" +
		"it. --cause names the aspect that decided it, such as subreddit, channel,\n" +
		"theme or title, so a filter can learn from it. Left out, the cause is\n" +
		"unstated. --cause " + route.CauseLowQuality + " also writes your low-quality mark, which\n" +
		"teaches the quality gate as `ingestion items mark` does.",
	Example: "  ingestion review drop reddit/t3_1hb0qa --cause subreddit\n" +
		"  ingestion review drop youtube/aqz-KE-bpKQ --cause " + route.CauseLowQuality,
	Args: cobra.ExactArgs(1),
	RunE: runReviewDrop,
}

func init() {
	goclikit.AsNamespace(reviewCmd)
	reviewListCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{reviewPlaceCmd, reviewDropCmd} {
		c.Flags().String("cause", "", "Name the aspect that decided it, such as subreddit, channel, theme or title")
		c.Flags().String("note", "", "Carry the questions and why the item was saved, for a model read to answer")
		c.Flags().Bool("json", false, "Print the service's answer as JSON")
	}
	reviewPlaceCmd.Flags().String("playlist", "", "Name the playlist that a watch adds the video to")
	reviewArchiveCmd.Flags().String("liked", "", "Name what you liked: "+strings.Join(route.Likes, ", "))
	reviewArchiveCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	for _, c := range []*cobra.Command{reviewListCmd, reviewPlaceCmd, reviewArchiveCmd, reviewDropCmd} {
		c.Flags().String("api", "", "Call this server rather than the config file's api")
		addConfigFlag(c)
	}
	reviewCmd.AddCommand(reviewListCmd, reviewPlaceCmd, reviewArchiveCmd, reviewDropCmd)
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
	name := strings.TrimSpace(args[1])
	body := server.PlaceRequest{Choice: name}
	c, ok := route.ChoiceOf(name)
	switch {
	case ok && !c.Places():
		return fmt.Errorf("%w: %s is a command of its own: `ingestion review %s %s`", goclikit.ErrUsage, name, name, args[0])
	case !ok:
		body = server.PlaceRequest{Destination: name}
	}
	body.Playlist, _ = cmd.Flags().GetString("playlist")
	body.Cause, _ = cmd.Flags().GetString("cause")
	if note, _ := cmd.Flags().GetString("note"); note != "" {
		body.Note = &note
	}
	if !ok || !c.RunsHere {
		return sendReview(cmd, server.RouteReviewPlace, args[0], "place", body)
	}
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	ctx, config, stderr := cmd.Context(), configPath(cmd), cmd.ErrOrStderr()
	answer, _, err := decideReview(ctx, config, api, server.RouteReviewPlace, args[0], "place", body)
	if err := reviewRefusal(err); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(stderr, placedSentence(answer))
	return relateHere(ctx, config, api, answer.Key, asJSON, cmd.OutOrStdout(), stderr)
}

// relateHere hands the landed item under key to psyche relate on this machine,
// as a push of a landed item does.
func relateHere(ctx context.Context, config, api, key string, asJSON bool, out, stderr io.Writer) error {
	target := "/api/v1/items/" + url.PathEscape(key)
	var parse server.ParseAnswer
	_, err := getJSON(ctx, config, api, server.RouteItemParse, target+"/parse", &parse)
	var refused *refusal
	if err != nil && (!errors.As(err, &refused) || refused.Code != server.CodeParseNotFound || refused.Status >= http.StatusInternalServerError) {
		return argumentRefusal(err)
	}
	return relateParse(ctx, config, api, target, parse, push.Wait, false, asJSON, out, stderr)
}

func runReviewArchive(cmd *cobra.Command, args []string) error {
	body := server.ArchiveRequest{}
	body.Liked, _ = cmd.Flags().GetString("liked")
	if body.Liked != "" && !slices.Contains(route.Likes, body.Liked) {
		return fmt.Errorf("%w: --liked names %q, and an archive likes one of %s", goclikit.ErrUsage, body.Liked, strings.Join(route.Likes, ", "))
	}
	return sendReview(cmd, server.RouteReviewArchive, args[0], "archive", body)
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
	if err := reviewRefusal(err); err != nil {
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

// reviewRefusal is err as a usage error where the server refused what a
// review command named, and err as it is otherwise.
func reviewRefusal(err error) error {
	return argumentRefusal(err, server.CodeDestinationNotDeclared, server.CodeDestinationPushOnly, server.CodeDestinationTakesPlaylist,
		server.CodeChapterOfBook, server.CodeInvalidBody, server.CodeChoiceRefused)
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
	case read != nil && read.Verdict == store.VerdictArchive:
		liked := ""
		switch side.Cause {
		case route.LikedThisItem:
			liked = ", liked for this item alone"
		case route.LikedChannel, route.LikedTheme:
			liked = ", liked for its " + side.Cause
		}
		return fmt.Sprintf("Archived %s%s. Its record stays with no destination, and your read mark teaches the quality gate.", a.Key, liked)
	case side.Destination == nil && side.Cause == route.CauseLowQuality:
		return fmt.Sprintf("Dropped %s, cause %s, and marked it low quality, which teaches the quality gate.", a.Key, side.Cause)
	case side.Destination == nil:
		return fmt.Sprintf("Dropped %s, cause %s.", a.Key, side.Cause)
	case side.Verdict != connectors.SidePersonal && side.Verdict != connectors.SideFleet:
		return fmt.Sprintf("Placed %s on %s, which takes no side, so it is dropped.", a.Key, *side.Destination)
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
