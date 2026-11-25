package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/datapointchris/ingestion/worker"
)

const (
	defaultSlots = 2
	defaultLease = 5 * time.Minute
	defaultPoll  = 5 * time.Second
)

// workerHandlers lists the stage handlers for a worker to run. A stage whose work
// runs in a worker adds its handler here.
func workerHandlers() []worker.Handler { return nil }

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run a worker that leases tasks from the service",
}

var workerRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Lease tasks by capability from the service and run them",
	Long: "worker run leases tasks from the service whose capability one of its handlers\n" +
		"holds, runs each, extends the lease while it runs, and reports how it ended. It\n" +
		"holds no store, and reads and writes only through the service's API.\n" +
		"\n" +
		"It authenticates as the service client the config file's client_id names,\n" +
		"with that client's secret in INGESTION_CLIENT_SECRET, and calls the file's api.\n" +
		"It reads the file's [worker] table:\n" +
		"\n" +
		"  slots  how many tasks run at once (default 2)\n" +
		"  lease  how long each lease runs before a heartbeat extends it (default 5m)\n" +
		"  poll   how often an idle worker claims again (default 5s)",
	Example: "  # a worker, as a service client\n" +
		"  INGESTION_CLIENT_SECRET=... ingestion worker run --config /etc/ingestion/config.toml",
	Args: cobra.NoArgs,
	RunE: runWorker,
}

func init() {
	goclikit.AsNamespace(workerCmd)
	addConfigFlag(workerRunCmd)
	workerCmd.AddCommand(workerRunCmd)
	rootCmd.AddCommand(workerCmd)
}

func runWorker(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	path := configPath(cmd)
	cfg, err := readConfig(path)
	if err != nil {
		return err
	}
	d, err := loadDeployment(ctx, path, "")
	if err != nil {
		return err
	}
	if os.Getenv(clientSecretEnv) == "" && !d.HandsTokens() {
		return fmt.Errorf("worker run authenticates as a service client, so set %s to the secret of client %s", clientSecretEnv, d.ClientID.Value)
	}
	source, err := tokenSource(ctx, d)
	if err != nil {
		return err
	}
	host, _ := os.Hostname()
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	run := worker.Config{
		ID: host + "-" + hex.EncodeToString(suffix), Version: buildVersion(),
		Slots: cfg.Worker.Slots, Lease: cfg.Worker.Lease.Duration, Poll: cfg.Worker.Poll.Duration,
		Log: slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	}
	if run.Slots <= 0 {
		run.Slots = defaultSlots
	}
	if run.Lease <= 0 {
		run.Lease = defaultLease
	}
	if run.Poll <= 0 {
		run.Poll = defaultPoll
	}
	client := worker.NewClient(d.API.Value, oauth2.NewClient(ctx, source))
	return worker.Run(ctx, client, run, workerHandlers()...)
}
