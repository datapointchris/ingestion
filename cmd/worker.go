package cmd

import (
	"cmp"
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

	"github.com/datapointchris/ingestion/contents"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/sources"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	defaultSlots = 2
	defaultLease = 5 * time.Minute
	defaultPoll  = 5 * time.Second
)

// workerHandlers lists the stage handlers for a worker to run: each
// extractor's, the structure's where pdf-structure is installed, the scan's
// where scanner is installed, the contents stage's, the quality gate's and
// its arm's where port is open, the model read's where both are, each of
// deliverers by its name, and each of sourceHandlers, which openSources
// answers. A stage whose work runs in a worker adds its handler here.
func workerHandlers(client *worker.Client, deliverers map[string]deliver.Deliverer, sourceHandlers []worker.Handler,
	port modelport.Port, scanner *scan.Scanner, models modelConfig, log *slog.Logger,
) []worker.Handler {
	handlers := extract.Builtin().Handlers(client, log)
	if structure := sections.NewHandler(sections.Program, client, log); structure != nil {
		handlers = append(handlers, structure)
	}
	if scanner != nil {
		handlers = append(handlers, scan.NewHandler(scanner, client, log))
	}
	if port != nil {
		quality := modelstage.Settings{Model: models.Quality.Model, Effort: models.Quality.Effort}
		handlers = append(handlers,
			contents.NewHandler(port, client, modelstage.Settings{Model: models.Contents.Model, Effort: models.Contents.Effort}, log),
			qualitygate.NewHandler(port, client, quality, log), qualitygate.NewArmHandler(port, client, quality, log))
	}
	if port != nil && scanner != nil {
		handlers = append(handlers, modelread.NewHandler(port, client, scanner,
			modelstage.Settings{Model: models.ModelRead.Model, Effort: models.ModelRead.Effort}, log))
	}
	for _, name := range slices.Sorted(maps.Keys(deliverers)) {
		handlers = append(handlers, deliver.Handler(name, deliverers[name]))
	}
	return append(handlers, sourceHandlers...)
}

// openSources opens one client for each registered source whose
// [source.<name>] table cfg holds, with every scope the source asks, and
// answers the handler of each stage the source serves through it. issuer is
// the file's, which a table takes where it names none. A registered source
// with no table, or whose table cannot open, is logged and left out, so the
// worker's other stages run on and its tasks wait queued for a worker that
// opens it.
func openSources(ctx context.Context, cfg fileConfig, issuer string, log *slog.Logger) []worker.Handler {
	var out []worker.Handler
	for _, name := range sources.Names() {
		r, _ := sources.Lookup(name)
		claimsNone := "so the worker claims no " + strings.Join(r.Capabilities(), " or ") + " task"
		table, ok := cfg.Sources[name]
		if !ok {
			log.Warn("the config holds no [source."+name+"] table, "+claimsNone, "source", name)
			continue
		}
		client, err := serviceHTTPClient(ctx, table, sourceSecretEnv(name), issuer, r.Scopes)
		if err != nil {
			log.Error("[source."+name+"] cannot open, "+claimsNone, "error", err)
			continue
		}
		out = append(out, r.Handlers(table.API, client, log)...)
	}
	return out
}

// sourcesHelp lists each registered source: its name and the variable that
// holds its client's secret on one line, and its client's scopes below.
func sourcesHelp() string {
	var b strings.Builder
	for _, name := range sources.Names() {
		r, _ := sources.Lookup(name)
		fmt.Fprintf(&b, "  %s  %s\n  %s  %s\n", name, sourceSecretEnv(name), strings.Repeat(" ", len(name)), strings.Join(r.Scopes, " "))
	}
	return b.String()
}

// openModelPort opens the adapter that cfg's [model] table names. With none
// named, or one that cannot open, it logs why and answers nil, so the worker
// claims no model stage's task and its other stages run on. items show then
// names the setting that a waiting item lacks.
func openModelPort(ctx context.Context, cfg modelConfig, issuer string, log *slog.Logger) modelport.Port {
	var port modelport.Port
	var err error
	switch cfg.Adapter {
	case "":
		log.Warn("the config names no [model] adapter, so the worker claims no model stage's task", "stages", store.ModelStages())
		return nil
	case modelport.AdapterQueue:
		var client *http.Client
		if client, err = serviceHTTPClient(ctx, cfg.Queue.serviceClient, modelSecretEnv, issuer, modelport.QueueScopes); err == nil {
			port = modelport.NewQueue(cfg.Queue.API, cmp.Or(cfg.Queue.Agent, modelport.DefaultAgent), client)
		}
	case modelport.AdapterClaudeCLI:
		port, err = modelport.NewClaudeCLI(cfg.ClaudeCLI.Command)
	default:
		err = fmt.Errorf("adapter %q is none of %s", cfg.Adapter, strings.Join(modelport.Adapters, ", "))
	}
	if err != nil {
		log.Error("[model] cannot open, so the worker claims no model stage's task", "stages", store.ModelStages(),
			"adapter", cfg.Adapter, "error", err)
		return nil
	}
	return port
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
		client, err := serviceHTTPClient(ctx, table.serviceClient, deliverSecretEnv(name), issuer, r.Scopes)
		if err != nil {
			log.Error("[deliver."+name+"] cannot open, so the worker claims no "+store.DeliverCapability(name)+" task", "error", err)
			continue
		}
		out[name] = r.Open(table.API, client)
	}
	return out
}

// serviceHTTPClient is the HTTP client that carries the token of the service
// client that table names: the token its token_command prints, or a
// client-credentials token for its client_id, whose secret is in the
// variable secretEnv.
func serviceHTTPClient(ctx context.Context, table serviceClient, secretEnv, issuer string, scopes []string) (*http.Client, error) {
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
		secret := os.Getenv(secretEnv)
		if secret == "" {
			return nil, fmt.Errorf("it names client %s, so set %s to its secret", table.ClientID, secretEnv)
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
	Example: "  # a worker, as a service client\n" +
		"  INGESTION_CLIENT_SECRET=... ingestion worker run --config /etc/ingestion/config.toml",
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
		"Its [worker.slot_limits] table caps how many tasks of a capability, or of a\n" +
		"slot group's capabilities together, run at once, within slots, so a long task\n" +
		"leaves the other slots free. Each key is a slot group or a capability in no\n" +
		"group, and each value 1 or more. The " + extract.SlotGroupHeavy + " group holds the Docling and\n" +
		"Whisper parses, whose runs each hold gigabytes, and runs one at a time unless\n" +
		"the table names it. A capability that nothing caps may take every slot:\n" +
		"\n" +
		"  [worker.slot_limits]\n" +
		"  " + extract.SlotGroupHeavy + " = 1\n" +
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
		"  root           for a deliverer that writes an item by its file, the\n" +
		"                 [serve.homes] root its paths are below, which serve reads\n" +
		"\n" +
		"The " + deliver.LifeArticles + " client needs the scope " + strings.Join(deliver.LifeArticlesScopes, " ") + ", and the\n" +
		deliver.LearningResources + " client " + strings.Join(deliver.LearningResourcesScopes, " ") + ". A table that\n" +
		"cannot open is logged and left out, and the worker's other stages run on.\n" +
		"\n" +
		"It runs each stage that a source registers, such as its poll and its\n" +
		"delete, where a [source.<name>] table names how to reach that source. The\n" +
		"table takes api, client_id, token_command and issuer as a [deliver.<name>]\n" +
		"table does. Each source this build registers, with the variable that holds\n" +
		"its client's secret and the scopes its client needs:\n" +
		"\n" +
		sourcesHelp() +
		"\n" +
		"It runs each model stage through the adapter that the [model] table names.\n" +
		"An arm's task asks for the model that its replay named, at the quality\n" +
		"gate's effort. It runs the model read only where it also runs the scan,\n" +
		"because the scanner reads each suggestion before it is kept:\n" +
		"\n" +
		"  [model]\n" +
		"  adapter            queue or claude-cli. With none, the worker claims no\n" +
		"                     model stage's task, and the item waits there\n" +
		"  [model.queue]      an HTTP dispatch service\n" +
		"  api                its address\n" +
		"  agent              the kind of run it starts (default " + modelport.DefaultAgent + ")\n" +
		"  client_id          the service client it dispatches as, whose secret is in\n" +
		"                     " + modelSecretEnv + "\n" +
		"  token_command      in place of client_id, a command that prints its token\n" +
		"  issuer             the OIDC provider that signs its tokens (default the file's)\n" +
		"  [model.claude-cli] claude -p on this machine, with no tools and no settings\n" +
		"  command            the claude binary (default claude, found on PATH)\n" +
		"  [model.contents]   what the contents stage asks its run for\n" +
		"  model              the model (default " + contents.DefaultModel + ")\n" +
		"  effort             the effort (default " + contents.DefaultEffort + ")\n" +
		"  [model.quality]    what the quality gate asks its run for\n" +
		"  model              the model (default " + qualitygate.DefaultModel + ")\n" +
		"  effort             the effort (default " + qualitygate.DefaultEffort + ")\n" +
		"  [model.model-read] what the model read asks its run for\n" +
		"  model              the model (default " + modelread.DefaultModel + ")\n" +
		"  effort             the effort (default " + modelread.DefaultEffort + ")\n" +
		"\n" +
		"The [model.queue] client needs the scopes " + strings.Join(modelport.QueueScopes, " ") + ".",
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
		Slots: cfg.Worker.Slots, SlotLimits: cfg.Worker.SlotLimits, Lease: cfg.Worker.Lease.Duration, Poll: cfg.Worker.Poll.Duration,
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
	port := openModelPort(ctx, cfg.Model, d.Issuer.Value, run.Log)
	client := worker.NewClient(d.API.Value, oauth2.NewClient(ctx, source))
	sourceHandlers := openSources(ctx, cfg, d.Issuer.Value, run.Log)
	return worker.Run(ctx, client, run, workerHandlers(client, deliverers, sourceHandlers, port, scan.Installed(run.Log), cfg.Model, run.Log)...)
}
