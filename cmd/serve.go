package cmd

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/datapointchris/goapiauth"
	"github.com/spf13/cobra"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/comments"
	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/contents"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/metricsrefresh"
	"github.com/datapointchris/ingestion/modelread"
	"github.com/datapointchris/ingestion/outbox"
	"github.com/datapointchris/ingestion/promptproposal"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/sections"
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
		"                    whose files land by reference rather than as copies (none)\n" +
		"\n" +
		"It reads root from each [deliver.<name>] table, which a worker reads too. A\n" +
		"deliverer that writes an item by its file, such as " + deliver.LearningResources + ", takes\n" +
		"only an item with a live home below that [serve.homes] root, and each attempt\n" +
		"sends that home's path as the delivery first kept it:\n" +
		"\n" +
		"  [deliver." + deliver.LearningResources + "]\n" +
		"  root              the [serve.homes] root its file paths are below, such as books\n" +
		"\n" +
		"Where a read by a model carries no person's note, a note stage drafts one. A\n" +
		"draft whose confidence reaches the floor becomes the read's note, and one\n" +
		"below it sends the item to review as note-unclear:\n" +
		"\n" +
		"  [note]\n" +
		"  floor             a confidence from 0 to 1 (default " + strconv.FormatFloat(route.DefaultNoteFloor, 'f', -1, 64) + ")\n" +
		"\n" +
		"At each time of its [gates.proposals] schedule, it projects the quality gate's\n" +
		"misses that a person labeled since the last done proposal. Where it finds any, it\n" +
		"writes a prompt proposal holding them and queues one prompt-proposal task, which a\n" +
		"worker with a [model] adapter drafts. `ingestion gates proposals list` prints\n" +
		"each proposal:\n" +
		"\n" +
		"  [gates.proposals]\n" +
		"  schedule          a cron expression in UTC, or in the zone a CRON_TZ=<zone>\n" +
		"                    prefix names (default \"" + promptproposal.DefaultSchedule + "\")\n" +
		"\n" +
		"At each time of its [review.refresh-metrics] schedule, it queues one\n" +
		"refresh-metrics task for each source that reads its items' metrics again, such\n" +
		"as a video's views, where a connector of that source sighted an item that waits\n" +
		"in review. A worker with the source's [source.<name>] table reads them in one\n" +
		"task, and serve writes them onto those sightings alone. The refresh lands,\n" +
		"deletes and gates nothing:\n" +
		"\n" +
		"  [review.refresh-metrics]\n" +
		"  schedule          a cron expression in UTC, or in the zone a CRON_TZ=<zone>\n" +
		"                    prefix names (default \"" + metricsrefresh.DefaultSchedule + "\")",
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

// afterStage is afterStageUnder at the default note floor.
func afterStage(log *slog.Logger) server.AfterStage {
	return afterStageUnder(log, route.DefaultNoteFloor)
}

// afterStageUnder runs in the write that ends an item's task done. It runs
// what follows the stage that ended, keyed by that stage, such as the stages
// it queues or the read gate's decision on a note's draft under noteFloor,
// then runs the gates.
func afterStageUnder(log *slog.Logger, noteFloor float64) server.AfterStage {
	next := map[string][]server.AfterStage{
		store.StageParse: {extract.AfterParse(
			[]server.AfterStage{scan.QueueAfterParse, sections.QueueAfterParse}, []server.AfterStage{extract.QueueDeferredParse},
		)},
		store.StageStructure: {sections.AfterStructure, contents.AfterStructure(log), sections.QueueDoclingParse},
		store.StageContents:  {contents.AfterAnswer(log)},
		store.StageScan:      {qualitygate.QueueAfterScan},
		store.StageQuality:   {qualitygate.DecideAfterAnswer},
		store.StageNote:      {route.AfterDraft(noteFloor)},
		store.StageDeliver:   {route.AfterDelivery},
	}
	gates := route.AfterStage(log)
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		for _, run := range next[task.Stage] {
			if err := run(ctx, tx, task); err != nil {
				return err
			}
		}
		return gates(ctx, tx, task)
	}
}

// replayableStages is how a replay queues each stage again, keyed by the
// stage. A deliver replay queues the delivery of each item read as is whose
// delivery is not settled.
func replayableStages(st *store.Store, extractors *extract.Registry) map[string]server.Replayable {
	return map[string]server.Replayable{
		store.StageParse:      extractors.Replayable(),
		store.StageStructure:  sections.Replayable(),
		store.StageContents:   contents.Replayable(st),
		store.StageScan:       scan.Replayable(extractors),
		store.StageQuality:    qualitygate.Replayable(st),
		store.StageQualityArm: qualitygate.ArmReplayable(st),
		store.StageModelRead:  modelread.Replayable(st),
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
	noteFloor := route.DefaultNoteFloor
	if f := cfg.Note.Floor; f != nil {
		if *f < 0 || *f > 1 {
			return fmt.Errorf("%s: [note] floor is a confidence from 0 to 1, not %v", path, *f)
		}
		noteFloor = *f
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
	lander, err := landing.New(landing.Config{
		Store: st, Blobs: blobs, Homes: cfg.Serve.Homes, QueueParse: extractors.QueueParse, Sighted: route.AfterSighting(log),
	})
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := setDelivererRoots(cfg); err != nil {
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
	drafts, err := promptproposal.NewScheduler(st, cmp.Or(cfg.Gates.Proposals.Schedule, promptproposal.DefaultSchedule), log)
	if err != nil {
		return fmt.Errorf("%s: [gates.proposals] %w", path, err)
	}
	refreshes, err := metricsrefresh.NewScheduler(st, cmp.Or(cfg.Review.RefreshMetrics.Schedule, metricsrefresh.DefaultSchedule), log)
	if err != nil {
		return fmt.Errorf("%s: [review.refresh-metrics] %w", path, err)
	}
	roots := make([]string, 0, len(cfg.Serve.Homes))
	for root := range cfg.Serve.Homes {
		roots = append(roots, root)
	}
	handler := server.Handler(server.Config{
		Store: st, Gate: gate, Issuer: cfg.Issuer, PublicURL: cfg.Serve.PublicURL, Version: version, Log: log,
	}, server.TaskRoutes(st, lander, log, afterStageUnder(log, noteFloor)), server.BlobRoutes(lander, log), server.ItemRoutes(st, log),
		server.ConnectorRoutes(server.ConnectorConfig{
			Store: st, Lander: lander, Scheduler: scheduler, Roots: roots, AfterApply: route.AfterApply(log), Links: extractors, Log: log,
		}),
		server.ReplayRoutes(st, replayableStages(st, extractors), log),
		server.ReviewRoutes(st, log), server.TrustRoutes(st, log), accuracy.Routes(st, noteFloor, log), promptproposal.Routes(st, log),
		comments.Routes(st, log))
	srv := &http.Server{Addr: listen, Handler: handler, ReadHeaderTimeout: 10 * time.Second}

	var wg sync.WaitGroup
	defer wg.Wait()
	background, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Go(func() { gate.Run(background, log) })
	wg.Go(func() { relay.Run(background) })
	wg.Go(func() { scheduler.Run(background) })
	wg.Go(func() { drafts.Run(background) })
	wg.Go(func() { refreshes.Run(background) })

	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	log.Info("ingestion serves", "listen", listen, "store", stateDir(cfg), "issuer", cfg.Issuer, "version", version)
	wg.Go(func() { settlePhases(background, st, log) })
	wg.Go(func() { settleGateFamilies(background, st, log) })
	wg.Go(func() { fillSourceTimes(background, lander, log) })
	wg.Go(func() { queueUndeliveredChapters(background, st, log) })
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

// setDelivererRoots names the root of each [deliver.<name>] table that names
// one, refusing a root that [serve.homes] does not name.
func setDelivererRoots(cfg fileConfig) error {
	for _, name := range slices.Sorted(maps.Keys(cfg.Deliver)) {
		root := cfg.Deliver[name].Root
		if root == "" {
			continue
		}
		if _, ok := cfg.Serve.Homes[root]; !ok {
			return fmt.Errorf("[deliver.%s] names root %s, which [serve.homes] does not name", name, root)
		}
		if err := connectors.SetDelivererRoot(name, root); err != nil {
			return err
		}
	}
	return nil
}

// settlePhases settles every item's phase once the service listens, so a
// large store delays no request, and logs how long it took.
func settlePhases(ctx context.Context, st *store.Store, log *slog.Logger) {
	started := time.Now()
	items, moved, err := st.SettlePhases(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.ErrorContext(ctx, "settle each item's phase", "items", items, "moved", moved, "error", err)
		}
		return
	}
	log.InfoContext(ctx, "settled each item's phase", "items", items, "moved", moved, "took", time.Since(started).String())
}

// settleGateFamilies reads every gate family's standing again from its
// decisions once the service listens, so a release that changes the ladder's
// rule places each family again, and logs how many moved.
func settleGateFamilies(ctx context.Context, st *store.Store, log *slog.Logger) {
	var moved int
	err := st.Update(ctx, func(tx *store.Tx) error {
		var err error
		moved, err = gatetrust.Settle(ctx, tx)
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			log.ErrorContext(ctx, "settle each gate family's standing", "error", err)
		}
		return
	}
	log.InfoContext(ctx, "settled each gate family's standing", "moved", moved)
}

// queueUndeliveredChapters queues the delivery of each chapter whose book's
// delivery settled while the chapter holds none, once the service listens,
// and logs how many it queued.
func queueUndeliveredChapters(ctx context.Context, st *store.Store, log *slog.Logger) {
	started := time.Now()
	var queued int
	err := st.Update(ctx, func(tx *store.Tx) error {
		var err error
		queued, err = route.QueueUndeliveredChapters(ctx, tx)
		return err
	})
	if err != nil {
		if ctx.Err() == nil {
			log.ErrorContext(ctx, "queue the delivery of each undelivered chapter of a delivered book", "error", err)
		}
		return
	}
	log.InfoContext(ctx, "queued the delivery of each undelivered chapter of a delivered book", "queued", queued,
		"took", time.Since(started).String())
}

// fillSourceTimes reads a source time from the bytes of each item that landed
// without one, once the service listens, and logs how many it filled.
func fillSourceTimes(ctx context.Context, lander *landing.Lander, log *slog.Logger) {
	started := time.Now()
	read, filled, err := lander.FillSourceTimes(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.ErrorContext(ctx, "fill each item's source time", "read", read, "filled", filled, "error", err)
		}
		return
	}
	log.InfoContext(ctx, "filled each item's source time", "read", read, "filled", filled, "took", time.Since(started).String())
}
