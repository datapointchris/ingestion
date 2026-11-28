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

	"github.com/datapointchris/ingestion/comments"
	"github.com/datapointchris/ingestion/contents"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/lens"
	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/note"
	"github.com/datapointchris/ingestion/promptproposal"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/publicweb"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/sources"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/topics"
	"github.com/datapointchris/ingestion/worker"
)

const (
	defaultSlots = 3
	defaultLease = 5 * time.Minute
	defaultPoll  = 5 * time.Second
)

// workerHandlers lists the stage handlers for a worker to run: each
// extractor's, the structure's where pdf-structure is installed, the scan's
// where scanner is installed, the contents stage's, the topics stage's, the quality gate's, its
// arm's, the note's and the prompt proposal's draft where port is open, the
// model read's where both are, each of deliverers by its name, each handler
// of opened, which openSources answers, and the comments stage's where port is
// open and opened reads a source's comments. The quality gate, the note and
// the model read quote each lens's context that contexts reads. A stage whose
// work runs in a worker adds its handler here.
func workerHandlers(client *worker.Client, deliverers map[string]deliver.Deliverer, opened openedSources,
	port modelport.Port, scanner *scan.Scanner, contexts lens.Readers, models modelConfig, log *slog.Logger,
) []worker.Handler {
	handlers := extract.Builtin().Handlers(client, publicweb.NewReader(publicweb.Client()), log)
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
			topics.NewHandler(port, client, modelstage.Settings{Model: models.Topics.Model, Effort: models.Topics.Effort}, log),
			qualitygate.NewHandler(port, client, contexts, quality, log), qualitygate.NewArmHandler(port, client, contexts, quality, log),
			note.NewHandler(port, client, contexts, modelstage.Settings{Model: models.Note.Model, Effort: models.Note.Effort}, log),
			promptproposal.NewHandler(port, modelstage.Settings{Model: models.PromptProposal.Model, Effort: models.PromptProposal.Effort}, log))
	}
	if port != nil && scanner != nil {
		handlers = append(handlers, modelread.NewHandler(port, client, contexts, scanner,
			modelstage.Settings{Model: models.ModelRead.Model, Effort: models.ModelRead.Effort}, log))
	}
	if port != nil && len(opened.comments) > 0 {
		handlers = append(handlers, comments.NewHandler(port, opened.comments,
			modelstage.Settings{Model: models.Comments.Model, Effort: models.Comments.Effort}, log))
	}
	for _, name := range slices.Sorted(maps.Keys(deliverers)) {
		handlers = append(handlers, deliver.Handler(name, deliverers[name]))
	}
	return append(handlers, opened.handlers...)
}

// openedSources is what openSources opens: the handler of each stage that
// each opened source serves, and the reader of comments of each that reads
// them, by its media type.
type openedSources struct {
	handlers []worker.Handler
	comments comments.Readers
}

// openSources opens one client for each registered source whose
// [source.<name>] table cfg holds, with every scope the source asks, or as
// the login that the table names for a source that signs in as a person. It
// answers the handler of each stage the source serves through it, and its
// reader of comments. issuer is the file's, which a table takes where it
// names none. A poll keeps the bytes that its arrivals carry through blobs. A
// registered source with no table, or whose table cannot open, is logged and
// left out, so the worker's other stages run on and its tasks wait queued for
// a worker that opens it.
func openSources(ctx context.Context, cfg fileConfig, issuer string, blobs worker.BlobWriter, log *slog.Logger) openedSources {
	out := openedSources{comments: comments.Readers{}}
	for _, name := range sources.Names() {
		r, _ := sources.Lookup(name)
		claimsNone := "so the worker claims no " + strings.Join(r.Capabilities(), " or ") + " task"
		table, ok := cfg.Sources[name]
		if !ok {
			log.Warn("the config holds no [source."+name+"] table, "+claimsNone, "source", name)
			continue
		}
		client, err := sourceHTTPClient(ctx, name, r, table, issuer)
		if err != nil {
			log.Error("[source."+name+"] cannot open, "+claimsNone, "error", err)
			continue
		}
		out.handlers = append(out.handlers, r.Handlers(table.API, client, blobs, log)...)
		if r.Comments != nil {
			out.comments[r.Comments.MediaType] = r.Comments.Open(table.API, client)
		}
	}
	return out
}

// sourceHTTPClient is the client that the [source.<name>] table opens for r:
// a service client with r's scopes, or where r signs in as a person, the
// client that r's Login opens as the table's login, with the password in
// sourcePasswordEnv.
func sourceHTTPClient(ctx context.Context, name string, r sources.Registration, table sourceTable, issuer string) (*http.Client, error) {
	if r.Login == nil {
		if table.Login != "" {
			return nil, fmt.Errorf("it names login %s, and %s signs in as a service client", table.Login, name)
		}
		return serviceHTTPClient(ctx, table.serviceClient, sourceSecretEnv(name), issuer, r.Scopes)
	}
	switch {
	case table.API == "":
		return nil, errors.New("it names no api")
	case table.ClientID != "" || len(table.TokenCommand) > 0:
		return nil, fmt.Errorf("%s signs in as a person, so the table names login rather than client_id or token_command", name)
	case table.Login == "":
		return nil, fmt.Errorf("%s signs in as a person, so the table names the login", name)
	}
	password := os.Getenv(sourcePasswordEnv(name))
	if password == "" {
		return nil, fmt.Errorf("it names login %s, so set %s to its password", table.Login, sourcePasswordEnv(name))
	}
	return r.Login(table.API, table.Login, password), nil
}

// sourcesHelp lists each registered source: its name and the variable that
// holds its client's secret or its login's password on one line, and its
// client's scopes below, then each deliverer that writes through it.
func sourcesHelp() string {
	var b strings.Builder
	for _, name := range sources.Names() {
		r, _ := sources.Lookup(name)
		pad := strings.Repeat(" ", len(name))
		if r.Login != nil {
			fmt.Fprintf(&b, "  %s  %s\n  %s  signs in as the table's login\n", name, sourcePasswordEnv(name), pad)
		} else {
			fmt.Fprintf(&b, "  %s  %s\n  %s  %s\n", name, sourceSecretEnv(name), pad, strings.Join(r.Scopes, " "))
		}
		for _, d := range r.Deliverers {
			fmt.Fprintf(&b, "  %s  deliverer %s\n", pad, d.Name)
		}
	}
	return b.String()
}

// openLensReaders opens the context reader that each [lens.<name>.context]
// table in cfg names, by the lens's name, and reads it once, so the start log
// says whether it reads. issuer is the file's, which a table takes where it
// names none. A table that names no reader this build registers, or that
// cannot open, is logged and left out, so the stages under that lens run
// without its context, as they do where no table names one. Each lens with a
// note prompt and no reader is logged too.
func openLensReaders(ctx context.Context, cfg fileConfig, issuer string, log *slog.Logger) lens.Readers {
	out := lens.Readers{}
	for _, name := range slices.Sorted(maps.Keys(cfg.Lens)) {
		table := cfg.Lens[name].Context
		withoutIt := "so the note and the model read under the " + name + " lens run without its context"
		r, ok := lens.Lookup(table.Reader)
		if !ok {
			log.Error("[lens."+name+".context] names no context reader this build registers, "+withoutIt,
				"reader", table.Reader, "registered", strings.Join(lens.Names(), ", "))
			continue
		}
		client, err := serviceHTTPClient(ctx, table.serviceClient, lensSecretEnv(name), issuer, r.Scopes)
		if err != nil {
			log.Error("[lens."+name+".context] cannot open, "+withoutIt, "reader", table.Reader, "error", err)
			continue
		}
		out[name] = r.Open(table.API, client)
		log.Info("the lens's context reader opened", "lens", name, "reader", table.Reader, "api", table.API)
		if text := out.Text(ctx, name, log); text != "" {
			log.Info("the lens's context reader read the owner's current work", "lens", name, "runes", len([]rune(text)))
		}
	}
	for _, name := range slices.Sorted(maps.Keys(prompts.Note)) {
		if _, ok := out[name]; !ok {
			log.Info("no [lens."+name+".context] table opens a reader, so the note and the model read under the "+name+
				" lens run without the owner's current work", "lens", name)
		}
	}
	return out
}

// logModels logs what each model stage asks its run for, the stage's
// default where models names neither.
func logModels(models modelConfig, log *slog.Logger) {
	asked := func(m stageModel, model, effort string) string {
		return cmp.Or(m.Model, model) + " at " + cmp.Or(m.Effort, effort)
	}
	log.Info("each model stage asks its run for a model at an effort",
		"contents", asked(models.Contents, contents.DefaultModel, contents.DefaultEffort),
		"topics", asked(models.Topics, topics.DefaultModel, topics.DefaultEffort),
		"quality", asked(models.Quality, qualitygate.DefaultModel, qualitygate.DefaultEffort),
		"note", asked(models.Note, note.DefaultModel, note.DefaultEffort),
		"model-read", asked(models.ModelRead, modelread.DefaultModel, modelread.DefaultEffort),
		"prompt-proposal", asked(models.PromptProposal, promptproposal.DefaultModel, promptproposal.DefaultEffort),
		"comments", asked(models.Comments, comments.DefaultModel, comments.DefaultEffort))
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
		"Whisper parses, whose runs each hold gigabytes, and runs " + strconv.Itoa(extract.SlotGroups[extract.SlotGroupHeavy]) + " at a time unless\n" +
		"the table names it. A captions parse runs one at a time, because ypl reads one\n" +
		"video at a time. A capability that nothing caps may take every slot:\n" +
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
		"It runs each stage that a source registers, such as its poll, its delete,\n" +
		"its refresh of metrics and a deliverer that writes through it, such as\n" +
		"ypl-watch, where a [source.<name>] table names how to reach that source.\n" +
		"The table takes api, client_id, token_command and issuer as a\n" +
		"[deliver.<name>] table does. A source that signs in as a person, such as\n" +
		"FreshRSS's Google Reader API, takes api and login instead, and reads the\n" +
		"login's password from the variable it names. Each source this build\n" +
		"registers, with the variable that holds its client's secret or its login's\n" +
		"password, and the scopes its client needs:\n" +
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
		"  [model.topics]     what the topics stage asks its run for\n" +
		"  model              the model (default " + topics.DefaultModel + ")\n" +
		"  effort             the effort (default " + topics.DefaultEffort + ")\n" +
		"  [model.quality]    what the quality gate asks its run for\n" +
		"  model              the model (default " + qualitygate.DefaultModel + ")\n" +
		"  effort             the effort (default " + qualitygate.DefaultEffort + ")\n" +
		"  [model.note]       what a note's draft asks its run for\n" +
		"  model              the model (default " + note.DefaultModel + ")\n" +
		"  effort             the effort (default " + note.DefaultEffort + ")\n" +
		"  [model.model-read] what the model read asks its run for\n" +
		"  model              the model (default " + modelread.DefaultModel + ")\n" +
		"  effort             the effort (default " + modelread.DefaultEffort + ")\n" +
		"  [model.prompt-proposal]  what a prompt proposal's draft asks its run for\n" +
		"  model              the model (default " + promptproposal.DefaultModel + ")\n" +
		"  effort             the effort (default " + promptproposal.DefaultEffort + ")\n" +
		"\n" +
		"The [model.queue] client needs the scopes " + strings.Join(modelport.QueueScopes, " ") + ".\n" +
		"\n" +
		"The note and the model read quote the owner's current work under a lens\n" +
		"where a [lens.<lens>.context] table names a context reader this build\n" +
		"registers: " + strings.Join(lens.Names(), ", ") + ". The table takes api, client_id, token_command and\n" +
		"issuer as a [deliver.<name>] table does, and its client's secret is in\n" +
		"INGESTION_LENS_<LENS>_CONTEXT_CLIENT_SECRET, such as " + lensSecretEnv("fleet") + ".\n" +
		"With no table, or one that cannot open, each runs without the context.\n" +
		"A read that fails runs beside the last context read, or none.\n" +
		"\n" +
		"  [lens.fleet.context]\n" +
		"  reader             " + lens.Slate + ", whose client needs the scope " + strings.Join(lens.SlateScopes, " ") + "\n" +
		"  api                the reader's address\n" +
		"  client_id          the service client it reads as",
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
	if port != nil {
		logModels(cfg.Model, run.Log)
	}
	contexts := openLensReaders(ctx, cfg, d.Issuer.Value, run.Log)
	client := worker.NewClient(d.API.Value, oauth2.NewClient(ctx, source))
	opened := openSources(ctx, cfg, d.Issuer.Value, client, run.Log)
	handlers := workerHandlers(client, deliverers, opened, port, scan.Installed(run.Log), contexts, cfg.Model, run.Log)
	return worker.Run(ctx, client, run, handlers...)
}
