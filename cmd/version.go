package cmd

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/datapointchris/goselfupdate"
	"github.com/spf13/cobra"
)

// goreleaser sets these through -ldflags.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// buildVersion reports the running binary's version. `go install pkg@vX.Y.Z`
// applies no ldflags but stamps the module version into build info, so without
// this fallback such an install reports a dev build and `ingestion update`
// refuses to run.
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	return resolveVersion(version, info.Main.Version)
}

func resolveVersion(ldflagsVersion, moduleVersion string) string {
	if ldflagsVersion != "dev" && ldflagsVersion != "" {
		return ldflagsVersion
	}
	// A local `go build` stamps a pseudo-version, which is valid semver, so only
	// a plain release version counts.
	if !goselfupdate.IsReleaseVersion(moduleVersion) {
		return ldflagsVersion
	}
	return strings.TrimPrefix(moduleVersion, "v")
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version, its commit and its build date",
	Example: `  # The release running, with the commit and date it was built from
  ingestion version

  # The release alone, on one line
  ingestion --version`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		current := buildVersion()
		w := cmd.OutOrStdout()
		_, _ = fmt.Fprintf(w, "ingestion %s\n", current)
		_, _ = fmt.Fprintf(w, "  commit: %s\n", commit)
		_, _ = fmt.Fprintf(w, "  built:  %s\n", date)
		if current == "dev" {
			if info, ok := debug.ReadBuildInfo(); ok {
				_, _ = fmt.Fprintf(w, "  go:     %s\n", info.GoVersion)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)

	rootCmd.Version = buildVersion()

	// cobra adds its own --version only where none exists, and its own claims
	// -v, which stays free for a counted verbosity flag.
	rootCmd.Flags().Bool("version", false, "print the version and exit")
}
