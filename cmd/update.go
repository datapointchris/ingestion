package cmd

import (
	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goselfupdate"
)

// updateConfig names where releases come from. The `update` command and the
// update notice in Execute share it, so the two cannot point at different
// releases.
func updateConfig() goselfupdate.Config {
	return goselfupdate.Config{
		Owner:   "datapointchris",
		Repo:    "ingestion",
		Binary:  "ingestion",
		Version: buildVersion(),
	}
}

func init() {
	rootCmd.AddCommand(goclikit.UpdateCommand(updateConfig()))
}
