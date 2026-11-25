package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
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

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/review"
)

// The size --once draws at where neither the environment nor a terminal gives one. It is wide
// enough for the side panel.
const (
	onceDefaultWidth  = 188
	onceDefaultHeight = 60
)

// The tabs, in the order tab cycles them.
const tabReview = "review"

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
			"review lists the items that wait on a person's placement, grouped by why each waits,\n" +
				"with the gate's verdict and confidence, its quality estimate, its side guess and how\n" +
				"long the item has waited. The side panel reads the item under the cursor: its URL,\n" +
				"the quality gate's whole answer, the scan's findings, and its text, the post first\n" +
				"and then each comment. enter reads it full screen. A digit places the item on the\n" +
				"destination numbered with it under the strip. x drops it after asking what decided\n" +
				"it. n writes a note in $VISUAL or $EDITOR, then places the item with the note on the\n" +
				"destination you pick. o opens its URL. Each key calls the routes that `ingestion\n" +
				"review place` and `drop` call, under the same login, so the decision is yours.",
			func(ctx context.Context, o tuiOptions) tuiBoard {
				return reviewTab{review.NewBoard(ctx, reviewService(o), review.Outside{Open: openInBrowser})}
			},
		},
	}
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
	Short: "A tab per view of what waits on a person: " + strings.Join(tuiTabNames(), ", "),
	Example: "  ingestion tui\n" +
		"  ingestion tui --view review\n" +
		"  COLUMNS=188 LINES=60 ingestion tui --view review --once > frame.txt\n" +
		"  ingestion tui --view review --json | jq '.items[] | {source, key: .item.key, reason: .item.reason}'",
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
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		return fmt.Errorf("%w: ingestion tui needs a terminal; --once prints one frame and --json the snapshot", goclikit.ErrUsage)
	}
	return runTUI(cmd.Context(), cmd.ErrOrStderr(), o)
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
	answer, _, err := decideReview(ctx, s.config, s.api, server.RouteReviewPlace, key, "place", body)
	if err != nil {
		return "", refusalWords(err)
	}
	return placedSentence(answer), nil
}

func (s reviewService) Drop(ctx context.Context, key string, body server.DropRequest) (string, error) {
	answer, _, err := decideReview(ctx, s.config, s.api, server.RouteReviewDrop, key, "drop", body)
	if err != nil {
		return "", refusalWords(err)
	}
	return placedSentence(answer), nil
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
