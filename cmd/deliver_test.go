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
// whose [deliver.life-articles] names the stand-in, with the client's secret in
// the environment.
func (p *pipeline) lifeDeliverer(t *testing.T, life *standInLife) worker.Handler {
	t.Helper()
	t.Setenv(deliverSecretEnv(deliver.LifeArticles), lifeSecret)
	cfg := fileConfig{Deliver: map[string]serviceClient{deliver.LifeArticles: {API: life.URL, ClientID: lifeClient}}}
	handlers := workerHandlers(p.worker, openDeliverers(t.Context(), cfg, p.provider.URL, quiet), quiet)
	i := slices.IndexFunc(handlers, func(h worker.Handler) bool { return h.Capability() == store.DeliverCapability(deliver.LifeArticles) })
	if i < 0 {
		t.Fatalf("the worker holds no %s handler", deliver.LifeArticles)
	}
	return handlers[i]
}

// lifeSet is the example set with destinations in place of its own.
func lifeSet(t *testing.T, destinations string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.CopyFS(dir, os.DirFS(mustAbs(t, "../examples/declarations"))); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, connectors.DestinationsFile), []byte(destinations), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// withLife is the example's destinations with life-articles as
// reading-list's deliverer.
const withLife = "destinations:\n  - name: reading-list\n    side: personal\n    deliverer: life-articles\n    default_for_side: true\n" +
	"  - name: work-inbox\n    side: fleet\n    default_for_side: true\n  - name: dropped\n"

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
// backoff. Until a worker that opens life-articles checks in, each delivery
// names the capability that no worker claims. A deliver replay queues none of
// them again, and a repeat of the same body answers the article it saved.
func TestASavePlacedOnLifesReadingListIsSavedThereOnceUnderItsKey(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
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
	if d := p.shown(t, "reddit/t3_aa1").Deliveries; len(d) != 1 || text(d[0].UnclaimedCapability) != "deliver:life-articles" {
		t.Errorf("before any worker opened life-articles, t3_aa1 holds deliveries %s, want one naming deliver:life-articles unclaimed", jsonOf(d))
	}
	lifeArticles := p.lifeDeliverer(t, life)
	if ran := p.run(t, lifeArticles); ran != 4 {
		t.Fatalf("the worker ran %d deliveries, want one for each of the 4 saves on reading-list", ran)
	}

	// quotes is what last_error carries of life's own answer: the key that
	// holds the URL, and life's message.
	want := map[string]struct {
		server.DeliveryView
		quotes string
	}{
		"reddit/t3_aa1": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryDelivered, DestinationID: ptr("3")}},
		"reddit/t3_cc3": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryDeliveredToExisting, DestinationID: &byHand}},
		"reddit/t3_dd4": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryRefused, DestinationID: &held}, quotes: other},
		"reddit/t3_ee5": {DeliveryView: server.DeliveryView{Outcome: store.DeliveryPending}, quotes: "OperationalError"},
	}
	for _, key := range slices.Sorted(maps.Keys(want)) {
		shown := p.shown(t, key)
		w := want[key]
		if len(shown.Deliveries) != 1 || shown.Deliveries[0].Destination != "reading-list" ||
			text(shown.Deliveries[0].DestinationID) != text(w.DestinationID) || shown.Deliveries[0].Outcome != w.Outcome ||
			shown.Deliveries[0].Attempts != 1 || !strings.Contains(text(shown.Deliveries[0].LastError), w.quotes) ||
			(w.quotes == "") != (shown.Deliveries[0].LastError == nil) {
			t.Errorf("%s holds deliveries %s, want one to reading-list %s, after 1 attempt, quoting %q",
				key, jsonOf(shown.Deliveries), w.Outcome, w.quotes)
		}
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

// A person runs review drop on one save and review place onto work-inbox on
// another, after a rule queued each one's delivery to life and before any
// worker ran it. Life gets neither: each queued task is withdrawn, and each
// delivery stays pending with no task left to run it.
func TestAPersonsDropOrMoveWithdrawsADeliveryThatHasNotRun(t *testing.T) {
	p := newGatedPipeline(t, lifeSet(t, withLife))
	life := newStandInLife(t, p.provider)
	p.landExports(t, map[string][]byte{"first": redditExport(redditSave("t3_aa1", "Cooking"), redditSave("t3_cc3", "Cooking"))})
	p.parse(t, extract.Builtin())
	for _, args := range [][]string{
		{"review", "drop", "reddit/t3_aa1", "--cause", "subreddit"},
		{"review", "place", "reddit/t3_cc3", "work-inbox", "--cause", "subreddit"},
	} {
		if out, err := runCLI(t, append(args, "--config", p.config)...); err != nil {
			t.Fatalf("%v printed %q, %v", args, out, err)
		}
	}
	if ran := p.run(t, p.lifeDeliverer(t, life)); ran != 0 {
		t.Errorf("after the drop and the move the worker ran %d deliveries, want none", ran)
	}
	if articles, creates := life.counts(); articles != 0 || creates != 0 {
		t.Errorf("life holds %d articles from %d creates, want none", articles, creates)
	}
	for _, key := range []string{"reddit/t3_aa1", "reddit/t3_cc3"} {
		if d := p.shown(t, key).Deliveries; len(d) != 1 || d[0].Outcome != store.DeliveryPending || d[0].LastError == nil {
			t.Errorf("%s holds deliveries %s, want its reading-list delivery pending, saying why it stopped", key, jsonOf(d))
		}
	}
	if open := slices.IndexFunc(p.tasks(t), func(v server.TaskView) bool { return v.Stage == store.StageDeliver }); open >= 0 {
		t.Errorf("a deliver task is still open after the drop and the move: %s", jsonOf(p.tasks(t)[open]))
	}
}

// An item that the quality gate's guess places on a side names no
// destination, so it reaches the side's default destination. Under a set with
// no default for its side it waits, and `items show` says why. Once a set
// names a default, the apply queues its delivery.
func TestAnItemPlacedOnASideAloneWaitsForThatSidesDefaultDestination(t *testing.T) {
	ctx := t.Context()
	p := newGatedPipeline(t, lifeSet(t, "destinations:\n  - name: reading-list\n    side: personal\n    deliverer: life-articles\n"+
		"  - name: work-inbox\n    side: fleet\n  - name: dropped\n"))
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

	set, err := connectors.Read(lifeSet(t, withLife), nil)
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
