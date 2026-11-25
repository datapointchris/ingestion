package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/datapointchris/goclikit"
	"github.com/datapointchris/goclilogin"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/store"
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

// openDeliverers opens the deliverer that each [deliver.<name>] table in cfg
// names, by that name. issuer is the file's, which a table takes where it
// names none. A table that names no deliverer this build registers, or that
// cannot open, is logged and left out, so the worker's other stages run on.
// A worker claims no task of a deliverer that it leaves out, and items show
// names the capability of each delivery that no worker claims.
func openDeliverers(ctx context.Context, cfg fileConfig, issuer string, log *slog.Logger) map[string]deliver.Deliverer {
	out := map[string]deliver.Deliverer{}
	for _, name := range slices.Sorted(maps.Keys(cfg.Deliver)) {
		table := cfg.Deliver[name]
		r, ok := deliver.Lookup(name)
		if !ok {
			log.Error("[deliver."+name+"] names no deliverer this build registers, so the worker opens none for it",
				"registered", strings.Join(deliver.Names(), ", "))
			continue
		}
		client, err := deliverClient(ctx, name, table, issuer, r.Scopes)
		if err != nil {
			log.Error("[deliver."+name+"] cannot open, so the worker claims no "+store.DeliverCapability(name)+" task", "error", err)
			continue
		}
		out[name] = r.Open(table.API, client)
	}
	return out
}

// deliverClient is the HTTP client that carries the token of the service
// client that table names: the token its token_command prints, or a
// client-credentials token for its client_id, whose secret is in
// deliverSecretEnv(name).
func deliverClient(ctx context.Context, name string, table serviceClient, issuer string, scopes []string) (*http.Client, error) {
	if table.API == "" {
		return nil, errors.New("it names no api")
	}
	var source oauth2.TokenSource
	var err error
	switch {
	case len(table.TokenCommand) > 0 && table.ClientID != "":
		return nil, errors.New("it names both token_command and client_id, and a client authenticates one way")
	case len(table.TokenCommand) > 0:
		source, err = goclilogin.CommandTokenSource(ctx, table.TokenCommand)
	case table.ClientID != "":
		secret := os.Getenv(deliverSecretEnv(name))
		if secret == "" {
			return nil, fmt.Errorf("it names client %s, so set %s to its secret", table.ClientID, deliverSecretEnv(name))
		}
		if table.Issuer != "" {
			issuer = table.Issuer
		}
		source, err = goclilogin.ClientCredentialsTokenSource(ctx,
			goclilogin.ServiceClient{Issuer: issuer, ClientID: table.ClientID, Scopes: scopes}, secret)
	default:
		return nil, errors.New("it names neither client_id nor token_command")
	}
	if err != nil {
		return nil, err
	}
	return oauth2.NewClient(ctx, source), nil
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
		"It runs each deliverer that a [deliver.<name>] table opens, where <name> is a\n" +
		"deliverer this build registers: " + strings.Join(deliver.Names(), ", ") + ". A table names:\n" +
		"\n" +
		"  api            the app's address\n" +
		"  client_id      the service client it writes as, whose secret is in\n" +
		"                 INGESTION_DELIVER_<NAME>_CLIENT_SECRET, such as\n" +
		"                 " + deliverSecretEnv(deliver.LifeArticles) + "\n" +
		"  token_command  in place of client_id, a command that prints the client's token\n" +
		"  issuer         the OIDC provider that signs its tokens (default the file's issuer)\n" +
		"\n" +
		"The " + deliver.LifeArticles + " client needs the scope " + strings.Join(deliver.LifeArticlesScopes, " ") + ". A table that\n" +
		"cannot open is logged and left out, and the worker's other stages run on.",
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
	deliverers := openDeliverers(ctx, cfg, d.Issuer.Value, run.Log)
	client := worker.NewClient(d.API.Value, oauth2.NewClient(ctx, source))
	return worker.Run(ctx, client, run, workerHandlers(client, deliverers, run.Log)...)
}
