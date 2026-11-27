package cmd

import (
	"fmt"
	"io"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/server"
)

var blobsCmd = &cobra.Command{
	Use:   "blobs",
	Short: "Read the bytes the service keeps under their SHA-256",
	Example: "  # each stored blob with no row, and each blob row whose bytes are missing\n" +
		"  ingestion blobs unpaired",
}

var blobsUnpairedCmd = &cobra.Command{
	Use:   "unpaired",
	Short: "List each stored blob with no row, and each blob row whose bytes are missing",
	Long: "Compares the blob volume with the blob rows, as git fsck compares objects\n" +
		"with refs, and deletes nothing. It lists each blob in the volume that no row\n" +
		"names, such as the bytes of a landing whose write failed. It lists each row\n" +
		"whose bytes neither the volume nor a live home holds. It lists each file in\n" +
		"the volume that is not a blob and is too old for a write to be running it,\n" +
		"such as a temporary file that no rename finished.\n" +
		"\n" +
		"It also lists each blob row older than an hour that no item, home or output\n" +
		"names. A poll leaves one when it keeps a page for an item that already held\n" +
		"its bytes, or when its report never lands.\n" +
		"\n" +
		"It exits 3 where the volume and the rows disagree, because only a person can\n" +
		"fix that, and 1 where it could not compare. A blob that nothing names sets no\n" +
		"exit code, because an ordinary poll leaves one.",
	Example: "  ingestion blobs unpaired\n" +
		"  ingestion blobs unpaired --json",
	Args: cobra.NoArgs,
	RunE: runBlobsUnpaired,
}

func init() {
	goclikit.AsNamespace(blobsCmd)
	blobsUnpairedCmd.Flags().Bool("json", false, "Print the service's answer as JSON")
	blobsUnpairedCmd.Flags().String("api", "", "Call this server rather than the config file's api")
	addConfigFlag(blobsUnpairedCmd)
	blobsCmd.AddCommand(blobsUnpairedCmd)
	rootCmd.AddCommand(blobsCmd)
}

func runBlobsUnpaired(cmd *cobra.Command, _ []string) error {
	asJSON, _ := cmd.Flags().GetBool("json")
	api, _ := cmd.Flags().GetString("api")
	var answer server.UnpairedAnswer
	raw, err := getJSON(cmd.Context(), configPath(cmd), api, server.RouteBlobsUnpaired, "/api/v1/blobs/unpaired", &answer)
	if err != nil {
		return err
	}
	w := cmd.OutOrStdout()
	if asJSON {
		if _, err := w.Write(raw); err != nil {
			return err
		}
	} else {
		writeUnpaired(w, answer)
	}
	if n := len(answer.Unrowed) + len(answer.Missing) + len(answer.Leftovers); n > 0 {
		return fmt.Errorf("%d unpaired: %d blobs with no row, %d rows with no bytes, %d leftover files: %w",
			n, len(answer.Unrowed), len(answer.Missing), len(answer.Leftovers), errIssue)
	}
	return nil
}

func writeUnpaired(w io.Writer, a server.UnpairedAnswer) {
	_, _ = fmt.Fprintf(w, "Blobs: %d in the volume, %d rows.\n", a.Files, a.Rows)
	if len(a.Unnamed) > 0 {
		var size int64
		for _, b := range a.Unnamed {
			size += b.SizeBytes
		}
		_, _ = fmt.Fprintf(w, "\nRows that nothing names (%d, %s):\n", len(a.Unnamed), byteSize(size))
		for _, b := range a.Unnamed {
			_, _ = fmt.Fprintf(w, "  %s  %s  %s  created %s\n", b.SHA256, b.MediaType, byteSize(b.SizeBytes), localTime(b.CreatedTS))
		}
	}
	if len(a.Unrowed)+len(a.Missing)+len(a.Leftovers) == 0 {
		_, _ = fmt.Fprintln(w, "Every blob has its row, and every row has its bytes.")
		return
	}
	if len(a.Unrowed) > 0 {
		_, _ = fmt.Fprintf(w, "\nBlobs with no row (%d):\n", len(a.Unrowed))
		for _, f := range a.Unrowed {
			_, _ = fmt.Fprintf(w, "  %s  %s  modified %s\n", *f.SHA256, byteSize(f.SizeBytes), localTime(f.ModifiedTS))
		}
	}
	if len(a.Missing) > 0 {
		_, _ = fmt.Fprintf(w, "\nRows with no bytes (%d):\n", len(a.Missing))
		for _, b := range a.Missing {
			_, _ = fmt.Fprintf(w, "  %s  %s  %s\n", b.SHA256, b.MediaType, byteSize(b.SizeBytes))
		}
	}
	if len(a.Leftovers) > 0 {
		_, _ = fmt.Fprintf(w, "\nLeftover files (%d):\n", len(a.Leftovers))
		for _, f := range a.Leftovers {
			_, _ = fmt.Fprintf(w, "  sha256/%s  %s  modified %s\n", *f.Path, byteSize(f.SizeBytes), localTime(f.ModifiedTS))
		}
	}
}
