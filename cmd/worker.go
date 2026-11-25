package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goclilogin"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/worker"
)

const (
	defaultSlots = 2
	defaultLease = 5 * time.Minute
	defaultPoll  = 5 * time.Second
)

// workerHandlers lists the stage handlers for a worker to run: each
// extractor's, and each of deliverers by its name. A stage whose work runs in
// a worker adds its handler here.
func workerHandlers(client *worker.Client, deliverers map[string]deliver.Deliverer, log *slog.Logger) []worker.Handler {
	handlers := extract.Builtin().Handlers(client, log)
	for _, name := range slices.Sorted(maps.Keys(deliverers)) {
		handlers = append(handlers, deliver.Handler(name, deliverers[name]))
	}
	return handlers
}

// openDeliverers is each deliverer that cfg sets up, by the name it registers.
// issuer is the file's, which a destination's table takes where it names
// none. A worker claims no task of a deliverer that it leaves out.
func openDeliverers(ctx context.Context, cfg fileConfig, issuer string, log *slog.Logger) (map[string]deliver.Deliverer, error) {
	out := map[string]deliver.Deliverer{}
	life := cfg.Deliver.Life
	if life.API == "" {
		log.Info("[deliver.life] names no api, so this worker claims no " + deliver.LifeArticles + " task")
		return out, nil
	}
	if life.ClientID == "" {
		return nil, fmt.Errorf("[deliver.life] names no client_id, the service client that life admits this worker as")
	}
	secret := os.Getenv(lifeClientSecretEnv)
	if secret == "" {
		return nil, fmt.Errorf("[deliver.life] names client %s, so set %s to its secret", life.ClientID, lifeClientSecretEnv)
	}
	if life.Issuer != "" {
		issuer = life.Issuer
	}
	source, err := goclilogin.ClientCredentialsTokenSource(ctx,
		goclilogin.ServiceClient{Issuer: issuer, ClientID: life.ClientID, Scopes: deliver.LifeArticlesScopes}, secret)
	if err != nil {
		return nil, fmt.Errorf("[deliver.life]: %w", err)
	}
	out[deliver.LifeArticles] = deliver.NewLifeArticles(life.API, oauth2.NewClient(ctx, source))
	return out, nil
}

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
		"It authenticates with the token the file's token_command prints, or else as\n" +
		"the service client the file's client_id names, with that client's secret in\n" +
		"INGESTION_CLIENT_SECRET. It calls the file's api, and reads its [worker] table:\n" +
		"\n" +
		"  slots  how many tasks run at once (default " + strconv.Itoa(defaultSlots) + ")\n" +
		"  lease  how long each lease runs before a heartbeat extends it (default " + shortDuration(defaultLease) + ")\n" +
		"  poll   how often an idle worker claims again (default " + shortDuration(defaultPoll) + ")\n" +
		"\n" +
		"It delivers each item placed on a destination whose deliverer is " + deliver.LifeArticles + "\n" +
		"where the file's [deliver.life] table names life's api. It writes to life as\n" +
		"the service client that table names, with that client's secret in\n" +
		lifeClientSecretEnv + ":\n" +
		"\n" +
		"  api        life's address\n" +
		"  client_id  the service client that life admits with scope " + deliver.LifeArticlesScopes[0] + "\n" +
		"  issuer     the OIDC provider that signs its tokens (default the file's issuer)",
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
	deliverers, err := openDeliverers(ctx, cfg, d.Issuer.Value, run.Log)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	client := worker.NewClient(d.API.Value, oauth2.NewClient(ctx, source))
	return worker.Run(ctx, client, run, workerHandlers(client, deliverers, run.Log)...)
}
