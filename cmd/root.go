package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goselfupdate/autoupdate"
	"github.com/datapointchris/gotermstyle/cobrahelp"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	// Registers the freshrss source, so a set may declare its connectors and a worker polls its reading list.
	_ "github.com/datapointchris/ingestion/freshrss"
	// Registers the reddit-export drop format, so a set naming it reads and a drop into it lands.
	_ "github.com/datapointchris/ingestion/redditexport"
	// Registers the ypl source, so a set may declare its connectors and a worker polls and deletes through it.
	_ "github.com/datapointchris/ingestion/ypl"
)

// A subcommand is one file in this package that builds its *cobra.Command and
// adds it to rootCmd from its own init.
var rootCmd = &cobra.Command{
	Use:   "ingestion",
	Short: "Land each outside item once and deliver it where it belongs",
	Long: "ingestion takes in items from outside sources: exports, feeds, playlists,\n" +
		"folders and links pushed from a phone. It lands each item once under one key,\n" +
		"parses and scans it, gates it by quality and by side, and delivers it to the\n" +
		"one place it belongs.\n" +
		"\n" +
		"Every command answers --help.",
	Example: "  ingestion --version\n" +
		"  ingestion version\n" +
		"  ingestion update",
	// Execute prints the error itself, so cobra's own printer would say it twice.
	SilenceErrors: true,
	// A command that fails while running is not a misuse of its flags, and a
	// usage dump would bury the reason.
	SilenceUsage: true,
}

// Execute runs the command tree and exits with its code.
func Execute() {
	cobrahelp.Install(rootCmd)
	err := goclikit.Execute(context.Background(), rootCmd, autoupdate.Config{Update: updateConfig()})
	if err != nil {
		os.Exit(exitCode(rootCmd.ErrOrStderr(), err))
	}
}

// exitCode prints what err leaves unsaid to w and answers the process's exit code.
func exitCode(w io.Writer, err error) int {
	if !errors.Is(err, goclikit.ErrReported) {
		_, _ = fmt.Fprintln(w, err)
	}
	// 2 says the command line was wrong rather than the run. A caller retries
	// with different arguments only on this code.
	if errors.Is(err, goclikit.ErrUsage) {
		return 2
	}
	// 3 says a check ran and found what only a person can fix, so a scheduled
	// check tells it from a check that could not run. A declaration set that
	// does not hold is one, and so is an apply from a checkout that lacks the
	// set in force.
	if errors.Is(err, errIssue) || errors.Is(err, connectors.ErrInvalid) || errors.Is(err, errStaleCheckout) {
		return 3
	}
	// 4 says the server gave no answer, so the command did nothing.
	if errors.Is(err, errNoAnswer) {
		return 4
	}
	return 1
}

// errIssue marks a check that ran and found what only a person can fix.
var errIssue = errors.New("a person must look at each")
