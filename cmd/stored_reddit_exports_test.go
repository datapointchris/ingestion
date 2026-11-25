package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

// The stored Reddit exports drop through the reddit-exports connector of the
// set in declarationsEnv, whose format lands each save under its key. Every
// save parses and scans, and the set's rules place what they match. Each save
// that no rule places gets one dispatch from the stand-in dispatch service,
// whose answers cycle through a pass onto each side, a drop and an unsure
// answer. Each save read by a model gets one model read, whose answers
// alternate a suggestion and none, and the scanner reads each suggestion. The
// first fill holds every dispatch after the first until the connector is
// released. Each save read as is on the personal side is saved once in a
// stand-in life. Dropping the same files again lands nothing,
// and a copy of each under another name adds no item, task, dispatch or
// article. The exports are outsider text kept out of this public repo, so the
// test reads them only from redditExportsEnv, and it reports counts alone.
func TestTheStoredRedditExportsDropThroughTheirConnectorAndPassEveryStage(t *testing.T) {
	declarations := os.Getenv(declarationsEnv)
	if declarations == "" {
		t.Skip(declarationsEnv + " names no declaration set")
	}
	files := storedExportFiles(t)
	set, err := connectors.Read(declarations, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := set.Connector("reddit-exports"); !ok || c.Format != redditexport.Format {
		t.Fatalf("the set declares no reddit-exports connector of format %s, so a drop would land each export whole", redditexport.Format)
	}
	p := newGatedPipeline(t, declarations)
	stand := newStandInDispatch(t, p.provider)
	answers := []map[string]any{
		judged(qualitygate.AnswerPass, 0.9, connectors.SidePersonal, 0.9),
		judged(qualitygate.AnswerPass, 0.9, connectors.SideFleet, 0.9),
		judged(qualitygate.AnswerDrop, 0.9, connectors.SidePersonal, 0.9),
		judged(qualitygate.AnswerUnsure, 0.5, "unsure", 0.3),
	}
	answered := 0
	stand.answer = func(string) map[string]any {
		answered++
		return answers[answered%len(answers)]
	}
	read := 0
	stand.read = func(string) map[string]any {
		read++
		if read%2 == 0 {
			return readAnswered("A summary.", "")
		}
		return readAnswered("A summary.", "This item suggests a change.")
	}
	life := newStandInLife(t, p.provider)
	models := stand.models(t, "", "")
	workers := []worker.Handler{p.qualityGate(t, models), p.modelRead(t, models, sentenceScanner{}), p.lifeDeliverer(t, life)}

	created := p.dropEach(t, files)
	items := p.count(t, `SELECT count(*) FROM items`)
	parsed := p.parse(t, extract.Builtin())
	scanned := p.scanFindingNothing(t)
	p.run(t, workers...)
	out, err := runCLI(t, "connectors", "show", "reddit-exports", "--json", "--config", p.config)
	var shown server.ConnectorView
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil || shown.FirstFill == nil {
		t.Fatalf("connectors show reddit-exports failed: %v", err)
	}
	fill := *shown.FirstFill
	if fill.State != store.FirstFillHeld || fill.Held == 0 || fill.Tokens[store.StageQuality] == (store.Tokens{}) {
		t.Errorf("the first fill is %s holding %d, with tokens %s, want held behind one save's tokens at quality", fill.State, fill.Held, jsonOf(fill.Tokens))
	}
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, workers...)

	c := p.gated(t)
	unscanned := p.count(t, `SELECT count(*) FROM items i WHERE media_type = ?
		AND NOT EXISTS (SELECT 1 FROM outputs o WHERE o.item_key = i.key AND o.stage = 'scan' AND o.state = 'done')`, redditexport.MediaTypeSave)
	byGuess := p.count(t, `SELECT count(DISTINCT item_key) FROM decisions WHERE gate = 'side' AND decided_by LIKE 'model:%' AND verdict IN (?, ?)`,
		connectors.SidePersonal, connectors.SideFleet)
	dropped := p.count(t, `SELECT count(*) FROM items WHERE phase = 'dropped'`)
	modelRead := p.count(t, `SELECT count(*) FROM items i WHERE
		(SELECT d.verdict FROM decisions d WHERE d.item_key = i.key AND d.gate = 'read' ORDER BY d.sequence DESC LIMIT 1) = ?`, connectors.ReadModel)
	outcomes := map[string]int{}
	for _, outcome := range []string{store.DeliveryDelivered, store.DeliveryDeliveredToExisting, store.DeliveryRefused, store.DeliveryPending} {
		outcomes[outcome] = p.count(t, `SELECT count(*) FROM deliveries WHERE outcome = ?`, outcome)
	}
	reads := p.count(t, `SELECT count(*) FROM outputs WHERE stage = 'model-read' AND state = 'done'`)
	suggestions := p.count(t, `SELECT count(*) FROM outputs WHERE stage = 'model-read' AND state = 'done' AND findings IS NOT NULL`)
	dispatches := len(stand.starts())
	articles, creates := life.counts()
	if unscanned != 0 || c.byRule == 0 || c.waiting != 0 || dispatches != c.saves-c.byRule+modelRead || reads != modelRead ||
		suggestions != (modelRead+1)/2 {
		t.Errorf("%d saves: %d unscanned, %d placed by rule, %d waiting, %d dispatches, %d of %d model reads done with %d suggestions scanned; "+
			"want every save scanned, a rule placing some, none waiting, one quality dispatch for each save no rule placed "+
			"and one model read for each save read by a model, with every suggestion scanned",
			c.saves, unscanned, c.byRule, c.waiting, dispatches, reads, modelRead, suggestions)
	}
	if outcomes[store.DeliveryDelivered] != articles || outcomes[store.DeliveryPending] != 0 {
		t.Errorf("%d deliveries are delivered and %d pending, and the stand-in life holds %d articles; want one article each and none pending",
			outcomes[store.DeliveryDelivered], outcomes[store.DeliveryPending], articles)
	}

	replayed := 0
	for _, path := range files {
		out, err := runCLI(t, "items", "drop", path, "--connector", "reddit-exports", "--config", p.config)
		if err != nil {
			t.Fatalf("a second drop failed: %v", err)
		}
		if strings.Contains(out, "was already dropped") {
			replayed++
		}
	}
	copies := t.TempDir()
	var copied []string
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		copied = append(copied, filepath.Join(copies, "copy-"+filepath.Base(path)))
		if err := os.WriteFile(copied[len(copied)-1], raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	createdAgain := p.dropEach(t, copied)
	itemsAgain := p.count(t, `SELECT count(*) FROM items`)
	ranAgain := p.parse(t, extract.Builtin()) + p.scanFindingNothing(t) + p.run(t, workers...)
	queued := p.replayDeliveries(t)
	ranAgain += p.run(t, workers...)
	articlesAgain, createsAgain := life.counts()
	if replayed != len(files) || createdAgain != 0 || itemsAgain != items || ranAgain != 0 || queued != 0 ||
		len(stand.starts()) != dispatches || articlesAgain != articles || createsAgain != creates {
		t.Errorf("dropping the %d files again replayed %d; a copy of each created %d items, ran %d tasks, queued %d deliveries, "+
			"and started %d dispatches and %d articles; want every drop replayed and nothing more",
			len(files), replayed, createdAgain+itemsAgain-items, ranAgain, queued, len(stand.starts())-dispatches, articlesAgain-articles)
	}

	t.Logf("%d exports dropped: %d items landed (%d created), %d saves; %d parse tasks and %d scan tasks run",
		len(files), items, created, c.saves, parsed, scanned)
	t.Logf("first fill held %d behind one dispatch of tokens %s", fill.Held, jsonOf(fill.Tokens[store.StageQuality]))
	t.Logf("%d placed by rule, %d quality dispatches, %d placed on a side by the model's guess, %d dropped, review %v",
		c.byRule, dispatches-reads, byGuess, dropped, c.review)
	t.Logf("%d read by a model: %d model reads done, %d suggestions scanned", modelRead, reads, suggestions)
	t.Logf("deliveries: %d delivered, %d delivered to an existing article, %d refused, %d pending; the stand-in life holds %d articles",
		outcomes[store.DeliveryDelivered], outcomes[store.DeliveryDeliveredToExisting], outcomes[store.DeliveryRefused], outcomes[store.DeliveryPending], articles)
	t.Logf("a second drop of the %d files replayed %d and landed nothing; a copy of each under another name created %d items and ran %d tasks",
		len(files), replayed, createdAgain, ranAgain)
}

// dropEach drops each file into reddit-exports with `items drop --json`, and
// counts the items its answers created.
func (p *pipeline) dropEach(t *testing.T, files []string) int {
	t.Helper()
	created := 0
	for _, path := range files {
		out, err := runCLI(t, "items", "drop", path, "--connector", "reddit-exports", "--json", "--config", p.config)
		var answer server.PushAnswer
		if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
			t.Fatalf("a drop failed: %v", err)
		}
		for _, it := range answer.Items {
			if it.Created {
				created++
			}
		}
	}
	return created
}
