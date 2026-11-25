package cmd

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datapointchris/goapiauth/authtest"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/deliver"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// lifeClient is the service client that a worker writes to the stand-in life
// as, with lifeSecret.
const (
	lifeClient = "life-svc-ingestion"
	lifeSecret = "life-secret"
)

// standInLife answers life's article create as life's OpenAPI states it: POST
// /api/v1/articles takes an ArticleCreate and answers an ArticleSaved or an
// ArticleConflict. It admits a service token whose scp holds
// life.articles.write, as life's client scopes do. A create under a key
// already held answers that article where its title, URL and save date match
// the body that saved it, and 409 otherwise. A URL held under no key takes the
// key and answers adopted. A URL held under another key answers 409.
type standInLife struct {
	*httptest.Server
	mu       sync.Mutex
	articles []*standInArticle
	creates  int
	// failing counts the creates under each key that answer 500 first.
	failing map[string]int
}

type standInArticle struct {
	id                   int64
	title, url, saveDate string
	sourceKey            *string
	// saved is the title, URL and save date of the body that saved the
	// article under its key, or that adopted it.
	saved string
}

func newStandInLife(t *testing.T, provider *authtest.Provider) *standInLife {
	t.Helper()
	provider.Register(lifeClient, lifeSecret, deliver.LifeArticlesScopes...)
	verifier := provider.Verifier(t, "life-cli-", "life-svc-")
	l := &standInLife{failing: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/articles", func(w http.ResponseWriter, r *http.Request) {
		token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		caller, err := verifier.Verify(r.Context(), token)
		if !bearer || err != nil {
			lifeError(w, http.StatusUnauthorized, "Not authenticated")
			return
		}
		if !caller.Service || !slices.Contains(caller.Scopes, "life.articles.write") {
			lifeError(w, http.StatusForbidden, "this client's scopes reach no POST /api/v1/articles")
			return
		}
		var body struct {
			Title     *string `json:"title"`
			URL       *string `json:"url"`
			SaveDate  *string `json:"save_date"`
			SourceKey *string `json:"source_key"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Title == nil || body.URL == nil || body.SaveDate == nil ||
			(body.SourceKey != nil && *body.SourceKey == "") {
			lifeError(w, http.StatusUnprocessableEntity, "title: Field required")
			return
		}
		saveDate, err := time.Parse(time.RFC3339, *body.SaveDate)
		if err != nil {
			lifeError(w, http.StatusUnprocessableEntity, "save_date: Input should be a valid datetime")
			return
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.creates++
		if body.SourceKey != nil && l.failing[*body.SourceKey] > 0 {
			l.failing[*body.SourceKey]--
			lifeError(w, http.StatusInternalServerError, "Internal server error: OperationalError")
			return
		}
		saved := *body.Title + "\x00" + *body.URL + "\x00" + saveDate.UTC().Format(time.RFC3339)
		if body.SourceKey != nil {
			if held := l.find(func(a *standInArticle) bool { return a.sourceKey != nil && *a.sourceKey == *body.SourceKey }); held != nil {
				if held.saved != saved {
					lifeConflictAnswer(w, "source_key", held)
					return
				}
				lifeSavedAnswer(w, http.StatusOK, held, false)
				return
			}
		}
		if held := l.find(func(a *standInArticle) bool { return a.url == *body.URL }); held != nil {
			if held.sourceKey != nil || body.SourceKey == nil {
				lifeConflictAnswer(w, "url", held)
				return
			}
			held.sourceKey, held.saved = body.SourceKey, saved
			lifeSavedAnswer(w, http.StatusOK, held, true)
			return
		}
		a := &standInArticle{
			id: int64(len(l.articles) + 1), title: *body.Title, url: *body.URL, saveDate: saveDate.UTC().Format(time.RFC3339),
			sourceKey: body.SourceKey, saved: saved,
		}
		l.articles = append(l.articles, a)
		lifeSavedAnswer(w, http.StatusCreated, a, false)
	})
	l.Server = httptest.NewServer(mux)
	t.Cleanup(l.Close)
	return l
}

func (l *standInLife) find(match func(*standInArticle) bool) *standInArticle {
	for _, a := range l.articles {
		if match(a) {
			return a
		}
	}
	return nil
}

// hold saves an article as a person would, under key where it is set, and
// answers its id.
func (l *standInLife) hold(url string, key *string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := &standInArticle{id: int64(len(l.articles) + 1), title: "saved by hand", url: url, saveDate: "2026-01-02T03:04:05Z", sourceKey: key}
	l.articles = append(l.articles, a)
	return fmt.Sprint(a.id)
}

// counts is how many articles the stand-in holds, and how many creates it
// answered.
func (l *standInLife) counts() (articles, creates int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.articles), l.creates
}

func lifeError(w http.ResponseWriter, status int, message string) {
	writeLife(w, status, map[string]any{"error": http.StatusText(status), "message": message})
}

func lifeSavedAnswer(w http.ResponseWriter, status int, a *standInArticle, adopted bool) {
	writeLife(w, status, map[string]any{
		"id": a.id, "title": a.title, "url": a.url, "tags": []string{}, "summary": nil, "notes": nil, "save_date": a.saveDate,
		"last_read_date": nil, "read_count": 0, "is_favorite": false, "is_current": false, "is_archived": false, "review_days": nil,
		"source_key": a.sourceKey, "adopted": adopted,
	})
}

func lifeConflictAnswer(w http.ResponseWriter, conflict string, held *standInArticle) {
	writeLife(w, http.StatusConflict, map[string]any{
		"error": "Conflict", "message": "article " + fmt.Sprint(held.id) + " already holds " + held.url,
		"conflict": conflict, "article_id": held.id, "source_key": held.sourceKey,
	})
}

func writeLife(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// lifeDeliverer is the life-articles handler that a worker opens from a config
// whose [deliver.life] names the stand-in, with the client's secret in the
// environment.
func (p *pipeline) lifeDeliverer(t *testing.T, life *standInLife) worker.Handler {
	t.Helper()
	t.Setenv(lifeClientSecretEnv, lifeSecret)
	var cfg fileConfig
	cfg.Deliver.Life = serviceClient{API: life.URL, ClientID: lifeClient}
	deliverers, err := openDeliverers(t.Context(), cfg, p.provider.URL, quiet)
	if err != nil {
		t.Fatal(err)
	}
	handlers := workerHandlers(p.worker, deliverers, quiet)
	i := slices.IndexFunc(handlers, func(h worker.Handler) bool { return h.Capability() == store.DeliverCapability(deliver.LifeArticles) })
	if i < 0 {
		t.Fatalf("the worker holds no %s handler", deliver.LifeArticles)
	}
	return handlers[i]
}

// shown is the item as `items show --json` prints it, held to its schema.
func (p *pipeline) shown(t *testing.T, key string) server.ItemAnswer {
	t.Helper()
	out, err := runCLI(t, "items", "show", key, "--json", "--config", p.config)
	var shown server.ItemAnswer
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil {
		t.Fatalf("items show %s failed: %v", key, err)
	}
	holdsTo(t, []byte(out), "item.schema.json")
	return shown
}

// replayDeliveries replays the deliver stage over every item and answers how
// many items it queued.
func (p *pipeline) replayDeliveries(t *testing.T) int {
	t.Helper()
	out, err := runCLI(t, "items", "replay", "--stage", store.StageDeliver, "--json", "--config", p.config)
	var answer server.ReplayAnswer
	if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
		t.Fatalf("items replay --stage deliver printed %q, %v", out, err)
	}
	return answer.Items
}

// Saves that a rule places on reading-list, whose deliverer is life-articles,
// are each saved in a stand-in life under the item's key. A new one is
// delivered as a new article. One whose URL life holds under no key is
// adopted, delivered-to-existing. One whose URL life holds under another key
// is refused with that article, and never sent again. One that life answers
// 500 stays pending with its error and attempts, and its task waits out its
// backoff. A deliver replay queues none of them again, and a repeat of the
// same body answers the article it saved.
func TestASavePlacedOnLifesReadingListIsSavedThereOnceUnderItsKey(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(
		redditSave("t3_aa1", "Cooking"), redditSave("t3_bb2", "golang"), redditSave("t3_cc3", "Cooking"),
		redditSave("t3_dd4", "AskCulinary"), redditSave("t3_ee5", "Cooking"),
	)})
	p.parse(t, extract.Builtin())
	other := "url/0123456789abcdef"
	byHand := life.hold(text(p.shown(t, "reddit/t3_cc3").Item.URL), nil)
	held := life.hold(text(p.shown(t, "reddit/t3_dd4").Item.URL), &other)
	life.failing["reddit/t3_ee5"] = 1
	lifeArticles := p.lifeDeliverer(t, life)
	if ran := p.run(t, lifeArticles); ran != 4 {
		t.Fatalf("the worker ran %d deliveries, want one for each of the 4 saves on reading-list", ran)
	}

	want := map[string]server.DeliveryView{
		"reddit/t3_aa1": {Outcome: store.DeliveryDelivered, DestinationID: ptr("3")},
		"reddit/t3_cc3": {Outcome: store.DeliveryDeliveredToExisting, DestinationID: &byHand},
		"reddit/t3_dd4": {
			Outcome: store.DeliveryRefused, DestinationID: &held,
			LastError: ptr("life's article " + held + " holds this URL under source_key " + other),
		},
		"reddit/t3_ee5": {Outcome: store.DeliveryPending, LastError: ptr("life answered 500: Internal server error: OperationalError")},
	}
	for _, key := range slices.Sorted(maps.Keys(want)) {
		shown := p.shown(t, key)
		w := want[key]
		w.Destination, w.Attempts = "reading-list", 1
		if len(shown.Deliveries) != 1 || text(shown.Deliveries[0].DestinationID) != text(w.DestinationID) ||
			shown.Deliveries[0].Outcome != w.Outcome || shown.Deliveries[0].Attempts != 1 || text(shown.Deliveries[0].LastError) != text(w.LastError) {
			t.Errorf("%s holds deliveries %s, want %s", key, jsonOf(shown.Deliveries), jsonOf([]server.DeliveryView{w}))
		}
	}
	out, err := runCLI(t, "items", "show", "reddit/t3_dd4", "--config", p.config)
	if want := "delivery  reading-list  refused as " + held + "  attempts 1  life's article " + held; err != nil || !strings.Contains(out, want) {
		t.Errorf("items show printed %q, %v; want a line holding %q", out, err, want)
	}
	tasks := p.tasks(t)
	if i := slices.IndexFunc(tasks, func(v server.TaskView) bool {
		return text(v.ItemKey) == "reddit/t3_ee5" && v.Stage == store.StageDeliver
	}); i < 0 ||
		tasks[i].State != store.TaskQueued || tasks[i].NotBeforeTS == nil || text(tasks[i].Destination) != "reading-list" {
		t.Errorf("the delivery that life answered 500 holds tasks %s, want one queued for reading-list past its backoff", jsonOf(tasks))
	}

	articles, creates := life.counts()
	if queued := p.replayDeliveries(t); queued != 0 {
		t.Errorf("a deliver replay queued %d items, want none: three are settled and one waits out its backoff", queued)
	}
	p.landExports(t, map[string][]byte{"again": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_dd4", "AskCulinary"))})
	p.parse(t, extract.Builtin())
	if ran := p.run(t, lifeArticles); ran != 0 {
		t.Errorf("after a replay and a second landing the worker ran %d deliveries, want none", ran)
	}
	if a, c := life.counts(); a != articles || c != creates {
		t.Errorf("life went from %d articles and %d creates to %d and %d, want no change", articles, creates, a, c)
	}

	client := (&clientcredentials.Config{
		ClientID: lifeClient, ClientSecret: lifeSecret, TokenURL: p.provider.URL + "/token", Scopes: deliver.LifeArticlesScopes,
	}).Client(t.Context())
	item := p.shown(t, "reddit/t3_aa1").Item
	repeat := deliver.NewLifeArticles(life.URL, client).Deliver(t.Context(),
		deliver.Item{Key: item.Key, Zone: item.Zone, Title: item.Title, URL: item.URL, LandedTS: item.CreatedTS})
	if a, _ := life.counts(); repeat.Outcome != store.DeliveryDelivered || text(repeat.DestinationID) != "3" || a != articles {
		t.Errorf("the same body again answered %s as %s and left %d articles, want delivered as 3 and %d",
			repeat.Outcome, text(repeat.DestinationID), a, articles)
	}
}

// An item that the quality gate's guess places on a side names no
// destination, so it reaches the side's default destination. Under a set with
// no default for its side it waits, and `items show` says why. Once a set
// names a default, the apply queues its delivery.
func TestAnItemPlacedOnASideAloneWaitsForThatSidesDefaultDestination(t *testing.T) {
	ctx := t.Context()
	noDefault := t.TempDir()
	if err := os.CopyFS(noDefault, os.DirFS(mustAbs(t, "../examples/declarations"))); err != nil {
		t.Fatal(err)
	}
	destinations := "destinations:\n  - name: reading-list\n    side: personal\n    deliverer: life-articles\n" +
		"  - name: work-inbox\n    side: fleet\n  - name: dropped\n"
	if err := os.WriteFile(filepath.Join(noDefault, connectors.DestinationsFile), []byte(destinations), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newGatedPipeline(t, noDefault)
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_bb2", "golang"))})
	p.parse(t, extract.Builtin())
	key, guess, sure, run := "reddit/t3_bb2", connectors.SidePersonal, 0.9, "run-1"
	if err := p.st.Update(ctx, func(tx *store.Tx) error {
		if err := tx.PutOutput(ctx, store.Output{
			ItemKey: key, Stage: store.StageQuality, Producer: "quality-gate", ProducerVersion: "1", State: store.OutputDone,
			RunID: &run, SideGuess: &guess, SideConfidence: &sure,
		}); err != nil {
			return err
		}
		return route.AfterStage(quiet)(ctx, tx, store.Task{Stage: store.StageQuality, ItemKey: &key})
	}); err != nil {
		t.Fatal(err)
	}
	if u := p.shown(t, key).Undelivered; u == nil || u.Reason != connectors.UndeliveredNoSideDefault || u.Side != connectors.SidePersonal {
		t.Fatalf("the guessed save shows undelivered %s, want no-side-default on personal", jsonOf(u))
	}
	out, err := runCLI(t, "items", "show", key, "--config", p.config)
	if want := "delivery  none: no destination in the set in force is default_for_side personal"; err != nil || !strings.Contains(out, want) {
		t.Errorf("items show printed %q, %v; want a line holding %q", out, err, want)
	}

	set, err := connectors.Read(mustAbs(t, "../examples/declarations"), nil)
	if err != nil {
		t.Fatal(err)
	}
	first := "test"
	if _, err := connectors.Apply(ctx, p.st, set, "second", &first, "person:test", nil, route.AfterApply(quiet)); err != nil {
		t.Fatal(err)
	}
	if ran := p.run(t, p.lifeDeliverer(t, life)); ran != 1 {
		t.Fatalf("after a set named reading-list personal's default, the worker ran %d deliveries, want 1", ran)
	}
	if shown := p.shown(t, key); shown.Undelivered != nil || len(shown.Deliveries) != 1 || shown.Deliveries[0].Destination != "reading-list" ||
		shown.Deliveries[0].Outcome != store.DeliveryDelivered {
		t.Errorf("the guessed save holds deliveries %s, want delivered to reading-list", jsonOf(shown.Deliveries))
	}
}

// The personal saves of the stored Reddit exports, placed by the routing
// rules of the set in declarationsEnv, are each saved once in a stand-in
// life. Landing every export again and replaying the delivery saves none
// again. The exports are outsider text kept out of this public repo, so the
// test reads them only from redditExportsEnv, and reports counts alone.
func TestTheStoredRedditExportsSaveEachPersonalSaveInLifeOnce(t *testing.T) {
	exportsDir, declarations := os.Getenv(redditExportsEnv), os.Getenv(declarationsEnv)
	if exportsDir == "" || declarations == "" {
		t.Skip(redditExportsEnv + " and " + declarationsEnv + " name no stored Reddit exports and no declaration set")
	}
	exports := storedExports(t, exportsDir)
	p := newGatedPipeline(t, declarations)
	life := newStandInLife(t, p.provider)
	p.landExports(t, exports)
	p.parse(t, extract.Builtin())
	lifeArticles := p.lifeDeliverer(t, life)
	ran := p.run(t, lifeArticles)
	readAsIs := p.count(t, `SELECT count(*) FROM items i WHERE
		(SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = 'side' ORDER BY d.sequence DESC LIMIT 1) = 'personal'
		AND (SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = 'read' ORDER BY d.sequence DESC LIMIT 1) = 'as-is'`)
	outcomes := map[string]int{}
	for _, outcome := range []string{store.DeliveryDelivered, store.DeliveryDeliveredToExisting, store.DeliveryRefused, store.DeliveryPending} {
		outcomes[outcome] = p.count(t, `SELECT count(*) FROM deliveries WHERE outcome = ?`, outcome)
	}
	articles, creates := life.counts()

	p.landExports(t, exports)
	p.parse(t, extract.Builtin())
	queued := p.replayDeliveries(t)
	rerun := p.run(t, lifeArticles)
	again, createsAgain := life.counts()

	if outcomes[store.DeliveryDelivered] != articles || outcomes[store.DeliveryPending] != 0 {
		t.Errorf("%d deliveries are delivered and %d pending, and the stand-in holds %d articles; want one article each and none pending",
			outcomes[store.DeliveryDelivered], outcomes[store.DeliveryPending], articles)
	}
	if queued != 0 || rerun != 0 || again != articles || createsAgain != creates {
		t.Errorf("the second landing and the replay queued %d, ran %d, and added %d articles and %d creates, want none",
			queued, rerun, again-articles, createsAgain-creates)
	}
	t.Logf("%d exports: %d saves placed personal and read as is, %d deliveries run, %d articles delivered to the stand-in life, "+
		"%d delivered to an existing article, %d refused, %d pending; a second landing and a deliver replay queued %d and added %d articles",
		len(exports), readAsIs, ran, articles, outcomes[store.DeliveryDeliveredToExisting], outcomes[store.DeliveryRefused],
		outcomes[store.DeliveryPending], queued, again-articles)
}

// storedExports reads each stored export in dir, by its file's name.
func storedExports(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("%s holds no JSON files: %v", dir, err)
	}
	exports := map[string][]byte{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		exports[strings.TrimSuffix(filepath.Base(path), ".json")] = raw
	}
	return exports
}

// tasks is every task that is not done, as `tasks list --json` prints them.
func (p *pipeline) tasks(t *testing.T) []server.TaskView {
	t.Helper()
	out, err := runCLI(t, "tasks", "list", "--json", "--config", p.config)
	var answer server.TasksAnswer
	if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
		t.Fatalf("tasks list printed %q, %v", out, err)
	}
	holdsTo(t, []byte(out), "tasks-list.schema.json")
	return answer.Tasks
}

func ptr(s string) *string { return &s }

func text(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func jsonOf(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
