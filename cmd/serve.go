package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/datapointchris/goapiauth"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/outbox"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	defaultListen = ":8080"
	// shutdownGrace is how long a stopping service lets requests finish. It
	// stays under terminationGracePeriodSeconds in deploy/serve.yaml, so the
	// kubelet never kills a request mid-write.
	shutdownGrace = 20 * time.Second
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run the service: the store, its API, the connectors' scheduler, and the outbox",
	Long: "serve is the one writer of the store. It holds the SQLite file and the blob\n" +
		"volume under state_dir, answers the API on listen, and ships every change in\n" +
		"the store to records_otlp_url as OTLP log records. Workers lease tasks and move\n" +
		"bytes through the API. Its scheduler makes each applied poll connector's task\n" +
		"when it falls due, and scans each folder connector's folder itself.\n" +
		"\n" +
		"It reads the config file's issuer and its [serve] table:\n" +
		"\n" +
		"  issuer            the OIDC provider whose tokens the API accepts (required)\n" +
		"  [serve]\n" +
		"  listen            the address the API answers on (default " + defaultListen + ")\n" +
		"  state_dir         where the store lives (default $XDG_STATE_HOME/" + product + ")\n" +
		"  public_url        where callers reach the API, served as RFC 9728 metadata\n" +
		"  records_otlp_url  the OTLP/HTTP logs URL the outbox ships to (none ships nothing)\n" +
		"  [serve.homes]     each named root's directory, such as books = \"/srv/books\",\n" +
		"                    whose files land by reference rather than as copies (none)",
	Example: "  # the service, reading $XDG_CONFIG_HOME/ingestion/config.toml\n" +
		"  ingestion serve\n" +
		"\n" +
		"  # the service in a container, reading a mounted file\n" +
		"  ingestion serve --config /etc/ingestion/config.toml",
	Args: cobra.NoArgs,
	RunE: runServe,
}

func init() {
	addConfigFlag(serveCmd)
	rootCmd.AddCommand(serveCmd)
}

// replayStages are the stages that serve replays: the parse, by each item's
// extractor, and the delivery of each item read as is whose delivery is not
// settled.
func replayStages(extractors *extract.Registry) map[string]server.Replayable {
	return map[string]server.Replayable{
		store.StageParse: extractors.Replayable(),
		store.StageDeliver: {Queue: func(ctx context.Context, tx *store.Tx, item store.Item) (bool, error) {
			return route.QueueDelivery(ctx, tx, item.Key)
		}},
	}
}

func runServe(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	path := configPath(cmd)
	cfg, err := readConfig(path)
	if err != nil {
		return err
	}
	if cfg.Issuer == "" {
		return fmt.Errorf("%s sets no issuer. The API accepts only tokens an OIDC provider signed, so name it: issuer = \"https://auth.example.com\"", path)
	}
	listen := cfg.Serve.Listen
	if listen == "" {
		listen = defaultListen
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	version := buildVersion()

	st, err := store.Open(ctx, filepath.Join(stateDir(cfg), "ingestion.db"))
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()
	blobs, err := landing.OpenBlobs(filepath.Join(stateDir(cfg), "blobs"))
	if err != nil {
		return err
	}
	extractors := extract.Builtin()
	lander, err := landing.New(landing.Config{Store: st, Blobs: blobs, Homes: cfg.Serve.Homes, QueueParse: extractors.QueueParse})
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	gate, err := goapiauth.NewConnecting(server.GateConfig(cfg.Issuer))
	if err != nil {
		return err
	}
	relay, err := outbox.Open(ctx, st, cfg.Serve.RecordsOTLPURL, version, log)
	if err != nil {
		return err
	}
	scheduler := connectors.NewScheduler(st, lander, log)
	roots := make([]string, 0, len(cfg.Serve.Homes))
	for root := range cfg.Serve.Homes {
		roots = append(roots, root)
	}
	handler := server.Handler(server.Config{
		Store: st, Gate: gate, Issuer: cfg.Issuer, PublicURL: cfg.Serve.PublicURL, Version: version, Log: log,
	}, server.TaskRoutes(st, log, route.AfterStage(log)), server.BlobRoutes(lander, log), server.ItemRoutes(st, log),
		server.ConnectorRoutes(server.ConnectorConfig{
			Store: st, Lander: lander, Scheduler: scheduler, Roots: roots, AfterApply: route.AfterApply(log), Log: log,
		}),
		server.ReplayRoutes(st, replayStages(extractors), log),
		server.ReviewRoutes(st, log))
	srv := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

	var wg sync.WaitGroup
	defer wg.Wait()
	background, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { gate.Run(background, log) })
	wg.Go(func() { relay.Run(background) })
	wg.Go(func() { scheduler.Run(background) })

	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	log.Info("ingestion serves", "listen", listen, "store", stateDir(cfg), "issuer", cfg.Issuer, "version", version)
	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, release := context.WithTimeout(context.Background(), shutdownGrace)
	defer release()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
