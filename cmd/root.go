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
)

// A subcommand is one file in this package that builds its *cobra.Command and
// adds it to rootCmd from its own init.
var rootCmd = &cobra.Command{
	Use:   "ingestion",
	Short: "Land each item from an outside source once, and deliver it to the one place it belongs",
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
	return 1
}
