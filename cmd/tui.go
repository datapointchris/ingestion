package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goclilogin"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"
	"github.com/datapointchris/gotermstyle"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/datapointchris/ingestion/push"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/history"
	"github.com/datapointchris/ingestion/views/review"
)

// The size --once draws at where neither the environment nor a terminal gives one. It is wide
// enough for the side panel.
const (
	onceDefaultWidth  = 188
	onceDefaultHeight = 60
)

// The tabs, in the order tab cycles them.
const (
	tabReview  = "review"
	tabHistory = "history"
)

// tuiTab is one tab: its paragraph of the help, and its board, which an open program draws and
// --once and --json print. Every list of the tabs comes from tuiTabList, so a tab is declared once.
type tuiTab struct {
	name, about string
	open        func(ctx context.Context, o tuiOptions) tuiBoard
}

// tuiBoard is a tab's board: it reads every source once for a printed frame, answering why its
// main read failed, prints its snapshot, and holds the writes its keys queued, which quitting
// awaits.
type tuiBoard interface {
	gotermboard.Board
	ReadAll(ctx context.Context) error
	AwaitWrites(announce func(doing []string)) []keywrites.Ended
	print(out io.Writer, asJSON bool) error
}

func tuiTabList() []tuiTab {
	return []tuiTab{
		{
			tabReview,
			"review lists the items that wait on a person's placement, grouped by channel. Each row\n" +
				"gives the channel and the title, then why it waits, the gate's verdict and confidence,\n" +
				"its quality estimate, its fit to your current work, its side guess, the source's\n" +
				"length, views and likes per view, and how long ago the item was made at its source.\n" +
				"A narrow screen cuts the title first. v cycles the grouping: by channel, by connector,\n" +
				"or none for one list. s cycles the sort, and the strip names both. / filters on the\n" +
				"title and the channel. The side panel reads the item under the cursor: its card, the\n" +
				"quality gate's whole answer with its reason, the scan's findings, and its text. enter\n" +
				"reads it full screen.\n" +
				"\n" +
				"A choice's key places the item at once, and the bar under the strip says where each\n" +
				"sends it:\n" +
				"\n" +
				tuiChoiceLines() +
				"\n" +
				review.ChoiceBindings[route.ChoiceWatch].Key + " asks which playlist, listing the service's default first and then the playlists\n" +
				"that `" + playlistsProgram + " playlists list` reads on this machine. " + review.ChoiceBindings[route.ChoicePsyche].Key + " hands the terminal to\n" +
				push.Program + " relate on this machine once the placement lands, and enter returns to the board.\n" +
				review.ChoiceBindings[route.ChoiceArchive].Key + " asks what you liked: the channel, the theme or this item alone, and\n" +
				"the decision keeps the answer as its cause. " + review.KeyDrop + " drops the item after asking what decided it,\n" +
				"and a drop for low quality marks it low quality too. " + review.KeyNote + " writes a note in $VISUAL or $EDITOR,\n" +
				"then places the item with the note by the choice, or the destination no choice reaches,\n" +
				"whose digit you pick. Each decision keeps the card as it was drawn and how long you\n" +
				"took. " + review.KeyComments + " reads what a video's top comments say, with one model call that the card\n" +
				"shows once it lands, and asks again where the read failed. o opens its URL. Each key\n" +
				"calls the routes that `ingestion review place`, `review archive` and `review drop` call,\n" +
				"under the same login, so the decision is yours.",
			func(ctx context.Context, o tuiOptions) tuiBoard {
				return reviewTab{review.NewBoard(ctx, reviewService(o), review.Outside{Open: openInBrowser})}
			},
		},
		{
			tabHistory,
			"history lists every item that came through ingestion, the newest landed first, up to\n" +
				fmt.Sprint(history.Limit) + ". Each row shows its outcome, where it went, who decided and why, its\n" +
				"connectors, its phase, how long ago it landed, and its title. An outcome is placed\n" +
				"at a destination, archived, dropped with its cause, split into a memo's topics, or\n" +
				"undecided, in review or on its way there. Who decided is a person, a service, a\n" +
				"rule, a tag, a model, the side gate, or a chapter's book. A book's chapters take\n" +
				"its decisions, so the tab leaves them out, and `ingestion items list --chapters`\n" +
				"lists them. " + history.KeyOutcomeBack + " and " + history.KeyOutcome + " step the outcome filter back and on, reading the items\n" +
				"again under it, so the dropped list audits what the gates, the rules and you\n" +
				"dropped. v cycles the grouping: none, by outcome, by connector, or by who decided.\n" +
				"/ filters on the title, the URL, the channel and who decided.\n" +
				"\n" +
				"A choice's key places the item under the cursor as review's does, and " + review.ChoiceBindings[route.ChoiceArchive].Key + "\n" +
				"archives it. Each calls the routes that `ingestion review place` and `review\n" +
				"archive` call, under the same login, so the decision is yours. Placing an item that\n" +
				"the gate dropped disagrees with the gate, as a placement from review does.\n" +
				"Archiving it writes the read mark, which disagrees too. Either returns each family\n" +
				"of the gate's answer to rung 0. The row stays, and the read after the decision\n" +
				"draws its new outcome.",
			func(ctx context.Context, o tuiOptions) tuiBoard {
				return historyTab{history.NewBoard(ctx, historyService{reviewService(o)}, review.Outside{Open: openInBrowser})}
			},
		},
	}
}

// tuiChoiceLines gives each choice's key and its help line.
func tuiChoiceLines() string {
	var b strings.Builder
	for _, c := range route.Choices {
		fmt.Fprintf(&b, "  %s  %-11s %s\n", review.ChoiceBindings[c.Name].Key, c.Name, c.Help)
	}
	return b.String()
}

func tuiTabNames() []string {
	var names []string
	for _, tab := range tuiTabList() {
		names = append(names, tab.name)
	}
	return names
}

// tuiOptions is what the command's flags ask.
type tuiOptions struct {
	view, config, api string
	once, asJSON      bool
}

var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "A tab per view of what waits on a person and where every item went: " + strings.Join(tuiTabNames(), ", "),
	Example: "  ingestion tui\n" +
		"  ingestion tui --view review\n" +
		"  COLUMNS=188 LINES=60 ingestion tui --view review --once > frame.txt\n" +
		"  ingestion tui --view review --json | jq '.items[] | {source, key: .item.key, reason: .item.reason}'\n" +
		"  ingestion tui --view history --json | jq '.items[].item | {key, outcome, by: .decided.decider}'",
	Args: cobra.NoArgs,
	RunE: runTUICommand,
}

func init() {
	paragraphs := []string{
		"One program with a tab per view, each keeping its own cursor, folds and filter. Each\n" +
			"read refreshes at its own pace, and the table draws its last good read with that\n" +
			"read's age, so a service that stops answering leaves the table drawn and says why.\n" +
			"? lists every key and every read, and q quits once every decision sent has ended.\n" +
			"\n" +
			"Text that a stranger wrote, such as a title, a post or a comment, is drawn with every\n" +
			"terminal control taken out, and as typed rather than as markdown.\n" +
			"\n" +
			"The next run opens on the display the last one left: each tab's panel, summaries,\n" +
			"grouping and folds, kept in $XDG_STATE_HOME/ingestion/tui-display.json.\n" +
			"\n" +
			"--once prints one frame at COLUMNS by LINES, and --json the snapshot the frame is\n" +
			"drawn from, of the tab --view names, so a script reads what a person reads. With no\n" +
			"terminal and neither flag it refuses.",
	}
	for _, tab := range tuiTabList() {
		paragraphs = append(paragraphs, tab.about)
	}
	tuiCmd.Long = strings.Join(paragraphs, "\n\n")
	tuiCmd.Flags().String("view", "", "Open on this tab: "+strings.Join(tuiTabNames(), ", "))
	tuiCmd.Flags().Bool("once", false, "Print one frame of the tab at COLUMNS by LINES, then exit")
	tuiCmd.Flags().Bool("json", false, "Print the tab's snapshot as JSON")
	tuiCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	tuiCmd.MarkFlagsMutuallyExclusive("once", "json")
	addConfigFlag(tuiCmd)
	rootCmd.AddCommand(tuiCmd)
}

func runTUICommand(cmd *cobra.Command, _ []string) error {
	o := tuiOptions{config: configPath(cmd)}
	o.view, _ = cmd.Flags().GetString("view")
	o.api, _ = cmd.Flags().GetString("api")
	o.once, _ = cmd.Flags().GetBool("once")
	o.asJSON, _ = cmd.Flags().GetBool("json")
	if err := gotermboard.RefuseUnknownView(o.view, tuiTabNames()); err != nil {
		return fmt.Errorf("%w: %v", goclikit.ErrUsage, err)
	}
	if o.once || o.asJSON {
		return printTUITab(cmd.Context(), cmd.OutOrStdout(), o)
	}
	if !onTerminal() {
		return fmt.Errorf("%w: ingestion tui needs a terminal; --once prints one frame and --json the snapshot", goclikit.ErrUsage)
	}
	return runTUI(cmd.Context(), cmd.ErrOrStderr(), o)
}

// onTerminal says a person holds the keyboard and reads the screen, which the program needs.
func onTerminal() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

// printTUITab reads the tab --view names, or the first, and prints it once. It builds no other.
// A main read that failed still prints, then fails the command as the CLI's own read would, so a
// server that gave no answer exits 4.
func printTUITab(ctx context.Context, out io.Writer, o tuiOptions) error {
	tabs := tuiTabList()
	at := max(slices.IndexFunc(tabs, func(tab tuiTab) bool { return tab.name == o.view }), 0)
	board := tabs[at].open(ctx, o)
	readErr := board.ReadAll(ctx)
	if err := board.print(out, o.asJSON); err != nil {
		return err
	}
	return readErr
}

// runTUI runs the tabs until the person quits, then waits for every decision a key sent, so
// quitting never leaves one unsent. Each that did not land is part of the command's error.
func runTUI(ctx context.Context, stderr io.Writer, o tuiOptions) error {
	var opened []gotermboard.Tab
	var boards []tuiBoard
	for _, tab := range tuiTabList() {
		board := tab.open(ctx, o)
		boards = append(boards, board)
		opened = append(opened, gotermboard.Tab{Name: tab.name, Board: board})
	}
	tabs, err := gotermboard.NewTabs(opened, gotermstyle.PaletteFor(os.Stdout), o.view)
	if err != nil {
		return err
	}
	if dir := goclilogin.StateDir(product); dir != "" {
		tabs.RemembersDisplayIn(filepath.Join(dir, "tui-display.json"))
	}
	_, err = tea.NewProgram(tabs).Run()
	errs := []error{err}
	for _, board := range boards {
		announce := func(doing []string) {
			_, _ = fmt.Fprintf(stderr, "ingestion tui waits for %d decision(s) to end: %s\n", len(doing), strings.Join(doing, "; "))
		}
		for _, ended := range board.AwaitWrites(announce) {
			if ended.Failed {
				errs = append(errs, errors.New(ended.Said))
				continue
			}
			_, _ = fmt.Fprintln(stderr, ended.Said)
		}
	}
	return errors.Join(errs...)
}

// reviewTab is the review board, printed as gotermboard prints any tab.
type reviewTab struct{ *review.Board }

func (t reviewTab) print(out io.Writer, asJSON bool) error {
	return gotermboard.PrintTab(out, asJSON, t.Board, gotermstyle.PaletteFor(os.Stdout), onceDefaultWidth, onceDefaultHeight)
}

// historyTab is the history board, printed as gotermboard prints any tab.
type historyTab struct{ *history.Board }

func (t historyTab) print(out io.Writer, asJSON bool) error {
	return gotermboard.PrintTab(out, asJSON, t.Board, gotermstyle.PaletteFor(os.Stdout), onceDefaultWidth, onceDefaultHeight)
}

// historyService is history.Service over the calls `ingestion items list` and `ingestion review`
// make, under the same login as reviewService.
type historyService struct{ reviewService }

// Items is `ingestion items list --outcome <outcome> --limit <limit> --json`. It refuses an
// answer from a service that predates each item's outcome, which the tab draws on every row.
func (s historyService) Items(ctx context.Context, outcome string, limit int) (server.ItemsAnswer, error) {
	answer, err := listItems(ctx, s.config, s.api, itemsQuery{outcome: outcome, limit: limit})
	if err != nil {
		return answer, refusalWords(err)
	}
	if answer.SchemaVersion < server.ItemsOutcomeVersion {
		return answer, fmt.Errorf("the service answers the items list at schema version %d, which gives no item its outcome: upgrade ingestion serve to read the history", answer.SchemaVersion)
	}
	return answer, nil
}

// reviewService is review.Service over the calls `ingestion review` makes, under the login the
// config file at o.config names, or on o.api where it is set.
type reviewService tuiOptions

func (s reviewService) Queue(ctx context.Context) (server.ReviewAnswer, error) {
	var answer server.ReviewAnswer
	_, err := getJSON(ctx, s.config, s.api, server.RouteReview, "/api/v1/review", &answer)
	return answer, refusalWords(err)
}

func (s reviewService) Parse(ctx context.Context, key string) (server.ParseAnswer, bool, error) {
	var answer server.ParseAnswer
	_, err := getJSON(ctx, s.config, s.api, server.RouteItemParse, "/api/v1/items/"+url.PathEscape(key)+"/parse", &answer)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeParseNotFound {
		return answer, false, nil
	}
	return answer, err == nil, refusalWords(err)
}

func (s reviewService) Place(ctx context.Context, key string, body server.PlaceRequest) (string, error) {
	bare := body
	bare.Presented, bare.DecidedAfterMS = nil, nil
	return s.decide(ctx, server.RouteReviewPlace, key, "place", body, bare)
}

func (s reviewService) Drop(ctx context.Context, key string, body server.DropRequest) (string, error) {
	bare := body
	bare.Presented, bare.DecidedAfterMS = nil, nil
	return s.decide(ctx, server.RouteReviewDrop, key, "drop", body, bare)
}

func (s reviewService) Archive(ctx context.Context, key string, body server.ArchiveRequest) (string, error) {
	bare := body
	bare.Presented, bare.DecidedAfterMS = nil, nil
	return s.decide(ctx, server.RouteReviewArchive, key, "archive", body, bare)
}

// Relate is `ingestion push <key> --to psyche --relate` under the same config and server, which
// hands the item's parse to psyche here and places nothing, then waits for enter, so its answer
// stays on the screen until the person reads it.
func (s reviewService) Relate(ctx context.Context, key string) *exec.Cmd {
	self, err := os.Executable()
	if err != nil {
		self = product
	}
	args := []string{self, "push", key, "--to", push.Program, "--relate", "--config", s.config}
	if s.api != "" {
		args = append(args, "--api", s.api)
	}
	script := `"$@"; status=$?; printf '\n%s' 'enter returns to the board '; read -r _; exit "$status"`
	return exec.CommandContext(ctx, "sh", append([]string{"-c", script, "sh"}, args...)...)
}

// playlistsProgram lists the playlists that a watch picks from, on this machine.
const playlistsProgram = "ypl"

// Playlists runs `ypl playlists list --json` on this machine, which lists the playlists that ypl
// holds under the person's own login.
func (s reviewService) Playlists(ctx context.Context) ([]review.Playlist, error) {
	path, err := exec.LookPath(playlistsProgram)
	if err != nil {
		return nil, fmt.Errorf("%s is not on this machine's PATH, so no playlist is listed: type one", playlistsProgram)
	}
	var stderr strings.Builder
	cmd := exec.CommandContext(ctx, path, "playlists", "list", "--json")
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s playlists list: %s", playlistsProgram, gotermboard.LastLine([]byte(stderr.String()), err))
	}
	var lists []review.Playlist
	if err := json.Unmarshal(out, &lists); err != nil {
		return nil, fmt.Errorf("%s playlists list --json printed no list of playlists: %w", playlistsProgram, err)
	}
	return lists, nil
}

// decide sends body, and sends bare, the same decision with no card, where a server built before
// the tab sent a card refuses body for the card alone. The decision is the person's, so it lands
// either way, and the sentence says the card was not kept.
func (s reviewService) decide(ctx context.Context, route server.Route, key, verb string, body, bare any) (string, error) {
	answer, _, err := decideReview(ctx, s.config, s.api, route, key, verb, body)
	var refused *refusal
	if errors.As(err, &refused) && refused.Code == server.CodeUnknownField && (refused.Field == "presented" || refused.Field == "decided_after_ms") {
		if answer, _, err = decideReview(ctx, s.config, s.api, route, key, verb, bare); err == nil {
			return placedSentence(answer) + " The service reads no card yet, so the card as shown was not kept.", nil
		}
	}
	if err != nil {
		return "", refusalWords(err)
	}
	return placedSentence(answer), nil
}

func (s reviewService) Comments(ctx context.Context, key string) (server.ReviewComments, error) {
	var answer server.ItemCommentsAnswer
	if _, err := sendJSON(ctx, s.config, s.api, server.RouteItemComments, "/api/v1/items/"+url.PathEscape(key)+"/comments", []byte("{}"), &answer); err != nil {
		return server.ReviewComments{}, refusalWords(err)
	}
	return answer.Comments, nil
}

// refusalWords is err with a refusal cut to the message the server gave, which the strip and the
// footer have room for, and err as it is otherwise.
func refusalWords(err error) error {
	var refused *refusal
	if errors.As(err, &refused) && refused.Message != "" {
		return fmt.Errorf("%s answered %d: %s", refused.Server, refused.Status, refused.Message)
	}
	return err
}

// openInBrowser hands url to the system's browser. Whatever the opener prints is discarded,
// because it would land on the board's screen.
func openInBrowser(url string) error {
	browser.Stdout, browser.Stderr = io.Discard, io.Discard
	return browser.OpenURL(url)
}
