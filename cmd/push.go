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
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/server"
)

var pushCmd = &cobra.Command{
	Use:   "push <key or url> --to psyche --relate",
	Short: "Hand one item's parse and its scan's findings to psyche relate on this machine, landing a new link first",
	Long: "Reads the item's parse and the findings of the scan that read it from the\n" +
		"service, writes them as the one document `psyche relate --parse` reads, and runs\n" +
		"`psyche relate --parse -` on this machine with that document on its stdin.\n" +
		"psyche prints its answer as it runs, and --json asks psyche for its JSON.\n" +
		"\n" +
		"A URL resolves to the key of the artifact it names. A link that the service has\n" +
		"not landed lands first, through the push connector that --connector names, as an\n" +
		"outside item that you place at psyche. That placement keeps every gate and model\n" +
		"stage off the item. Only a link that an extractor of the service reads at its\n" +
		"source lands, such as a YouTube video's, and the service stores nothing for any\n" +
		"other. The set in force declares the connector and a destination named psyche.\n" +
		"\n" +
		"The push then waits up to --wait for the item's parse and the scan of it, and\n" +
		"hands them on. A captions parse waits its turn at ypl's door, which the default\n" +
		"wait leaves room for. --wait 0 hands on only a parse already scanned.\n" +
		"\n" +
		"A finding of a rule that lists what the item suggests reaches psyche as a\n" +
		"suggestion rather than a finding. --enriched adds the service's current quality\n" +
		"gate answer and model read, and names each part it left out and why. Without it\n" +
		"psyche reads the parse and the findings alone.\n" +
		"\n" +
		"A push of an item already landed starts no stage in the service, so no model\n" +
		"reads the item there. The command exits 4 when the service gives no answer by\n" +
		"the end of the wait, and psyche never runs then. It exits 2 for a key that the\n" +
		"service has not landed, a link that it does not land, a connector that takes no\n" +
		"pushed link, or a set that declares no destination psyche. It exits 1 when the\n" +
		"wait ends with no scanned parse, naming the stage it waited at, when that stage\n" +
		"failed or nothing queued it, and when psyche fails.",
	Example: "  ingestion push https://www.youtube.com/watch?v=<video id> --to psyche --relate\n" +
		"  ingestion push reddit/t3_aq7ggd --to psyche --relate\n" +
		"  ingestion push https://www.reddit.com/r/golang/comments/aq7ggd/ --to psyche --relate --enriched\n" +
		"  ingestion push reddit/t3_aq7ggd --to psyche --relate --json --wait 0",
	Args: cobra.ExactArgs(1),
	RunE: runPush,
}

func init() {
	pushCmd.Flags().String("to", "", "Hand the item to this program: "+push.Program+" (required)")
	pushCmd.Flags().Bool("relate", false, "Run "+push.Program+" relate, the verb that reads a pushed parse")
	pushCmd.Flags().Bool("enriched", false, "Add the quality gate's and the model read's answers to the parse")
	pushCmd.Flags().Bool("json", false, "Ask "+push.Program+" for its answer as JSON")
	pushCmd.Flags().String("connector", push.Connector, "Land a new link through this push connector")
	pushCmd.Flags().Duration("wait", push.Wait, "Wait this long for the item's parse and its scan")
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
	connector, _ := cmd.Flags().GetString("connector")
	wait, _ := cmd.Flags().GetDuration("wait")
	api, _ := cmd.Flags().GetString("api")
	if to != push.Program {
		return fmt.Errorf("%w: --to names %q, and a push hands an item only to %s", goclikit.ErrUsage, to, push.Program)
	}
	if !relate {
		return fmt.Errorf("%w: a push to %s takes --relate, the one %s verb that reads a pushed parse", goclikit.ErrUsage, push.Program, push.Program)
	}
	if wait < 0 {
		return fmt.Errorf("%w: --wait is a duration from 0, not %s", goclikit.ErrUsage, wait)
	}
	ctx, config, ref := cmd.Context(), configPath(cmd), strings.TrimSpace(args[0])
	stderr := cmd.ErrOrStderr()
	// The server resolves a URL to its key, as items show relies on.
	target := "/api/v1/items/" + url.PathEscape(ref)
	var parse server.ParseAnswer
	_, err := getJSON(ctx, config, api, server.RouteItemParse, target+"/parse", &parse)
	var refused *refusal
	switch {
	case errors.As(err, &refused) && refused.Code == server.CodeItemNotFound && refused.Status < http.StatusInternalServerError:
		if !isLink(ref) {
			return fmt.Errorf("%w: %s is not landed yet, so the service holds %s. A push lands a new item only from its link, so give its URL",
				goclikit.ErrUsage, ref, refused.Message)
		}
		pushed, err := pushLink(ctx, config, api, connector, ref)
		if err != nil {
			return err
		}
		landedWords := "landed"
		if !pushed.Created {
			landedWords = "found"
		}
		_, _ = fmt.Fprintf(stderr, "%s %s through %s, placed at %s\n", landedWords, pushed.Key, connector, push.Program)
		target = "/api/v1/items/" + url.PathEscape(pushed.Key)
	case errors.As(err, &refused) && refused.Code == server.CodeParseNotFound:
	case err != nil:
		return argumentRefusal(err)
	}
	if parse.RulesVersion == nil {
		if parse, err = waitForScan(ctx, config, api, target, wait, stderr); err != nil {
			return err
		}
	}
	var added push.Enrichment
	if enriched {
		var item server.ItemAnswer
		if _, err := getJSON(ctx, config, api, server.RouteItem, target, &item); err != nil {
			return err
		}
		added = push.Enrich(item)
		for _, left := range added.Left {
			_, _ = fmt.Fprintln(stderr, left)
		}
		if len(added.Left) > 0 {
			_, _ = fmt.Fprintf(stderr, "`ingestion items show %s` says where each stage stands\n", parse.Key)
		}
	}
	doc, err := push.Write(parse, added.Enriched)
	if err != nil {
		return fmt.Errorf("%s: %w", parse.Key, err)
	}
	enrichedWords := ""
	if len(added.From) > 0 {
		enrichedWords = ", enriched by " + strings.Join(added.From, " and ")
	}
	_, _ = fmt.Fprintf(stderr, "pushing %s to %s relate: %s, %s, %s%s\n", doc.Key, push.Program,
		counted(len(doc.Units), "unit"), counted(len(doc.Findings), "finding"), counted(len(doc.Suggested), "suggestion"), enrichedWords)
	return push.Relate(ctx, doc, asJSON, cmd.OutOrStdout(), stderr)
}

// isLink says whether ref is an http or https URL rather than a key.
func isLink(ref string) bool {
	u, err := url.Parse(ref)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https")
}

// pushLink lands link through connector as an item placed at psyche, and
// answers the item that the service landed or already held.
func pushLink(ctx context.Context, config, api, connector, link string) (server.PushedItem, error) {
	body, err := json.Marshal(server.PushRequest{Connector: connector, URL: link, Destination: push.Program})
	if err != nil {
		return server.PushedItem{}, err
	}
	var answer server.PushAnswer
	_, _, err = callJSON(ctx, config, api, call{
		Route: server.RouteItemsPush, Target: "/api/v1/items", Header: http.Header{"Content-Type": {"application/json"}}, Body: body,
	}, &answer)
	var refused *refusal
	switch {
	case errors.As(err, &refused) && refused.Code == server.CodeDestinationNotDeclared:
		return server.PushedItem{}, fmt.Errorf("%w: %s. A push places a new link at %s, so the set in force declares a destination %s",
			goclikit.ErrUsage, refused.Message, push.Program, push.Program)
	case errors.As(err, &refused) && (refused.Code == server.CodeConnectorNotFound || refused.Code == server.CodeConnectorMode):
		return server.PushedItem{}, fmt.Errorf("%w: %s. --connector names a push connector of the set in force", goclikit.ErrUsage, refused.Message)
	case errors.As(err, &refused) && (refused.Code == server.CodeLinkNotRead || refused.Code == server.CodeInvalidLink):
		return server.PushedItem{}, fmt.Errorf("%w: %s", goclikit.ErrUsage, refused.Message)
	case err != nil:
		return server.PushedItem{}, err
	case len(answer.Items) != 1:
		return server.PushedItem{}, fmt.Errorf("the push of one link answered %d items", len(answer.Items))
	}
	return answer.Items[0], nil
}

// The pause between two reads of an item that a push waits on starts at
// waitPollFloor and doubles to waitPollCeiling. waitReadFloor is the least
// time that one read inside a wait is given, so the read at the wait's end
// still reaches a server that is up.
const (
	waitPollFloor   = 250 * time.Millisecond
	waitPollCeiling = 5 * time.Second
	waitReadFloor   = 2 * time.Second
)

// waitForScan reads the parse of the item at target until a scan has read it,
// for up to wait. Each read that finds none reads the item too, to name the
// stage that it waits at, and stops where no run of that stage is left to end
// it. Each read ends by the wait's end, and a read that the server gave no
// answer to is polled again until then. It says on stderr which stage it
// waits at, each time that changes.
func waitForScan(ctx context.Context, config, api, target string, wait time.Duration, stderr io.Writer) (server.ParseAnswer, error) {
	deadline := time.Now().Add(wait)
	key, _ := url.PathUnescape(strings.TrimPrefix(target, "/api/v1/items/"))
	pause, said, seen := waitPollFloor, "", ""
	for {
		var limit time.Duration
		if wait > 0 {
			limit = max(min(answerDeadline, time.Until(deadline)), waitReadFloor)
		}
		var parse server.ParseAnswer
		_, _, err := callJSON(ctx, config, api, call{Route: server.RouteItemParse, Target: target + "/parse", Deadline: limit}, &parse)
		var refused *refusal
		switch {
		case err == nil && parse.RulesVersion != nil:
			return parse, nil
		case err == nil, errors.As(err, &refused) && refused.Code == server.CodeParseNotFound:
			var item server.ItemAnswer
			if _, _, err = callJSON(ctx, config, api, call{Route: server.RouteItem, Target: target, Deadline: limit}, &item); err == nil {
				key = item.Item.Key
				stage, ended := push.WaitingOn(item)
				if ended != "" {
					return parse, fmt.Errorf("%s's %s %s, so there is nothing to hand %s. `ingestion items show %s` says more", key, stage, ended, push.Program, key)
				}
				seen = stage
			}
		}
		silent := err
		switch {
		case err == nil:
		case errors.Is(err, errNoAnswer):
		case errors.As(err, &refused) && refused.Status >= http.StatusInternalServerError:
			silent = fmt.Errorf("%w: %v", errNoAnswer, err)
		default:
			return parse, argumentRefusal(err)
		}
		switch left := time.Until(deadline); {
		case left > 0 && silent == nil && seen != said:
			_, _ = fmt.Fprintf(stderr, "waiting up to %s for %s's %s\n", left.Round(time.Second), key, seen)
			said = seen
		case left > 0:
		case silent != nil && seen == "":
			return parse, fmt.Errorf("%s: the wait of %s ended before the server answered: %w", key, wait, silent)
		case silent != nil:
			return parse, fmt.Errorf("%s was last seen at its %s, and the wait of %s ended before the server answered again. "+
				"That stage may still go on in the service, and a push run again picks it up: %w", key, seen, wait, silent)
		case wait == 0:
			return parse, fmt.Errorf("%s waits at its %s, so there is nothing to hand %s yet. `ingestion items show %s` says where it waits, "+
				"and --wait waits for it", key, seen, push.Program, key)
		default:
			return parse, fmt.Errorf("%s still waits at its %s after %s, so nothing went to %s. "+
				"`ingestion items show %s` says where it stands, and a push run again hands it on once it is scanned", key, seen, wait, push.Program, key)
		}
		select {
		case <-ctx.Done():
			return parse, ctx.Err()
		case <-time.After(min(pause, time.Until(deadline))):
		}
		pause = min(2*pause, waitPollCeiling)
	}
}

// counted is n of noun, as a person writes it.
func counted(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
