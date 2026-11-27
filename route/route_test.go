package route_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/schemas"
	"github.com/datapointchris/ingestion/store"
)

var set = connectors.Set{
	Connectors: []connectors.Connector{{Name: "reddit-exports"}, {Name: "feed-reader"}, {Name: "books-folder", ReadRule: connectors.ReadAsIs}},
	RoutingRules: []connectors.RoutingRule{{
		Name: "cooking", Match: connectors.Match{Connector: "reddit-exports", Field: "subreddit", Values: []string{"Cooking"}},
		Destination: "reading-list",
	}},
	Destinations: []connectors.Destination{
		{Name: "reading-list", Side: connectors.SidePersonal, Tags: []string{"read-later"}},
		{Name: "work-inbox", Side: connectors.SideFleet, Tags: []string{"work"}},
		{Name: "dropped"},
	},
}

func sighting(connector, subreddit string, tags ...string) store.Sighting {
	return store.Sighting{Connector: connector, Tags: tags, Fields: map[string]string{"subreddit": subreddit}}
}

func ptr(s string) *string { return &s }

// Two accepted rules on feed-reader. The block's name sorts first, and the
// keep still decides first where both hold.
var accepted = []store.RoutingRule{
	{Name: "aa-quiet-feed", Action: store.RuleBlock, MatchConnector: ptr("feed-reader")},
	{
		Name: "zz-golang-feed", Action: store.RuleKeep, MatchConnector: ptr("feed-reader"), MatchField: ptr("subreddit"), MatchValues: []string{"golang"},
		Destination: ptr("work-inbox"),
	},
	{Name: "aa-quiet-reddit", Action: store.RuleBlock, MatchConnector: ptr("reddit-exports"), MatchField: ptr("subreddit"), MatchValues: []string{"Cooking"}},
}

// The side gate places each item by the first rung that decides it, and says
// who decided: one side's tags, then the first rule that holds, then a guess
// at or above the floor. A declared rule decides before an accepted one, and
// an accepted keep before an accepted block. Tags naming two sides, a guess
// below the floor and no guess at all each send the item to review.
func TestTheSideGatePlacesEachItemByTheFirstRungThatDecidesIt(t *testing.T) {
	confident := &route.Guess{Side: connectors.SideFleet, Confidence: 0.9, Run: "run-1"}
	doubtful := &route.Guess{Side: connectors.SidePersonal, Confidence: 0.4, Run: "run-2"}
	sure := &route.Guess{Side: connectors.SidePersonal, Confidence: 0.9, Run: "run-3"}
	for _, c := range []struct {
		name      string
		sightings []store.Sighting
		guess     *route.Guess
		verdict   string
		by        string
		cause     string
	}{
		{"tags naming one side", []store.Sighting{sighting("reddit-exports", "golang", "read-later")}, confident, "personal", "tag", "tags"},
		{
			"tags naming both sides",
			[]store.Sighting{sighting("reddit-exports", "golang", "read-later"), sighting("feed-reader", "", "work")},
			confident,
			"review", "tag", "tags-disagree",
		},
		{"a declared rule before an accepted one", []store.Sighting{sighting("reddit-exports", "Cooking")}, confident, "personal", "rule:cooking", "rule"},
		{"an accepted keep before a block", []store.Sighting{sighting("feed-reader", "golang")}, confident, "fleet", "rule:zz-golang-feed", "rule"},
		{"an accepted block", []store.Sighting{sighting("feed-reader", "rust")}, confident, "drop", "rule:aa-quiet-feed", "rule"},
		{"a confident personal guess", []store.Sighting{sighting("reddit-exports", "golang")}, sure, "personal", "model:run-3", "guess"},
		{"a confident fleet guess", []store.Sighting{sighting("reddit-exports", "golang")}, confident, "review", "model:run-1", "below-floor"},
		{"a low-confidence guess", []store.Sighting{sighting("reddit-exports", "golang")}, doubtful, "review", "model:run-2", "below-floor"},
		{"no guess", []store.Sighting{sighting("reddit-exports", "golang")}, nil, "review", "gate", "no-guess"},
	} {
		d := route.Side(set, accepted, "url/0123456789abcdef", c.sightings, c.guess)
		if d.Verdict != c.verdict || d.DecidedBy != c.by || d.Cause != c.cause {
			t.Errorf("%s: the side gate answered %s by %s for %s, want %s by %s for %s", c.name, d.Verdict, d.DecidedBy, d.Cause, c.verdict, c.by, c.cause)
		}
	}
}

// A block's review share sends a seeded draw of its items to review as
// rule-would-hide, and drops the rest as rule:<name>. A replay over the same
// rule and item keys draws the same items. A later version at a higher share
// keeps every item that a lower one drew, so editing the rule to raise it
// never hides an item already shown.
func TestARulesReviewShareIsADrawThatAReplayRepeats(t *testing.T) {
	keys := make([]string, 400)
	for i := range keys {
		keys[i] = fmt.Sprintf("url/%016x", i)
	}
	versions := map[float64]int{0.1: 1, 0.2: 2, 0: 3}
	drawn := func(share float64) []string {
		rule := []store.RoutingRule{{
			Name: "quiet-feed", Sequence: versions[share], Action: store.RuleBlock, MatchConnector: ptr("feed-reader"), ReviewShare: share,
		}}
		var out []string
		for _, key := range keys {
			d := route.Side(set, rule, key, []store.Sighting{sighting("feed-reader", "")}, nil)
			switch {
			case d.DecidedBy != "rule:quiet-feed":
				t.Fatalf("%s is decided by %s, want the rule", key, d.DecidedBy)
			case d.Verdict == route.VerdictReview && d.Cause == route.CauseRuleWouldHide:
				out = append(out, key)
			case d.Verdict != route.VerdictDrop || d.Cause != route.CauseRule:
				t.Fatalf("%s is %s for %s, want a drop by the rule or its share's review", key, d.Verdict, d.Cause)
			}
		}
		return out
	}
	tenth := drawn(0.1)
	if n := len(tenth); n < 20 || n > 60 {
		t.Errorf("a share of 0.1 drew %d of %d items", n, len(keys))
	}
	if again := drawn(0.1); !slices.Equal(again, tenth) {
		t.Errorf("a replay over the same rule and keys drew %v, then %v", tenth, again)
	}
	fifth := drawn(0.2)
	for _, key := range tenth {
		if !slices.Contains(fifth, key) {
			t.Errorf("a share of 0.1 drew %s, and a share of 0.2 does not", key)
		}
	}
	if n := len(drawn(0)); n != 0 {
		t.Errorf("a share of 0 drew %d items, want none", n)
	}
}

// The read gate reads a placed item by its connector's read rule, else its
// side's default, and a model read carries its side's lens and the person's
// note.
func TestTheReadGateTakesTheConnectorsRuleElseTheSidesDefault(t *testing.T) {
	note := "what would this change for retries?"
	for _, c := range []struct {
		connector, side, verdict, by, lens string
	}{
		{"reddit-exports", connectors.SidePersonal, connectors.ReadAsIs, "rule:personal-default", ""},
		{"reddit-exports", connectors.SideFleet, connectors.ReadModel, "rule:fleet-default", "fleet"},
		{"books-folder", connectors.SideFleet, connectors.ReadAsIs, "rule:books-folder", ""},
	} {
		d := route.Read(set, []store.Sighting{sighting(c.connector, "")}, c.side, &note)
		lens := ""
		if d.Lens != nil {
			lens = *d.Lens
		}
		if d.Verdict != c.verdict || d.DecidedBy != c.by || lens != c.lens || (d.Verdict == connectors.ReadModel) != (d.Note != nil) {
			t.Errorf("%s on %s read %s by %s under lens %q with note %v, want %s by %s under lens %q",
				c.connector, c.side, d.Verdict, d.DecidedBy, lens, d.Note, c.verdict, c.by, c.lens)
		}
	}
}

// A connector that declares read_rule: model reads a personal item by a model
// under the personal lens, and no model read is due, because the build holds
// no prompt for that lens. The same item on the fleet side, placed with a
// note, is due one.
func TestAModelReadIsDueOnlyUnderALensWhosePromptTheBuildHolds(t *testing.T) {
	note := "what would this change?"
	for lens, want := range map[string]bool{route.LensFleet: true, route.LensPersonal: true, route.LensLearning: true, "unheld": false} {
		read := store.Decision{Gate: store.GateRead, Verdict: connectors.ReadModel, DecidedBy: route.ByPerson("ana"), Lens: &lens, Note: &note}
		if due := route.ModelReadDue(store.Decision{Verdict: connectors.SidePersonal}, read, nil, false); due != want {
			t.Errorf("a model read under lens %s is due: %v, want %v", lens, due, want)
		}
	}
}

// review list --json answers each reason that the help and the TUI name, and
// no other, so a reason added to one is added to all three.
func TestTheReviewReasonsAreTheReviewListSchemasReasons(t *testing.T) {
	raw, err := schemas.Files.ReadFile("review-list.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Items struct {
				Items struct {
					Properties struct {
						Reason struct {
							Enum []string `json:"enum"`
						} `json:"reason"`
					} `json:"properties"`
				} `json:"items"`
			} `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, r := range route.ReviewReasons {
		names = append(names, r.Name)
	}
	enum := schema.Properties.Items.Items.Properties.Reason.Enum
	if !slices.Equal(slices.Sorted(slices.Values(names)), slices.Sorted(slices.Values(enum))) {
		t.Errorf("route.ReviewReasons names %v, and the schema's reason enum %v", names, enum)
	}
}

// A verb that the table gains and a schema's enum lacks fails a reader that
// validates the answer. So each schema that names the verbs names
// route.Verbs, and the shelf-list names the shelf verbs.
func TestTheVerbSchemasNameTheVerbTable(t *testing.T) {
	enumAt := func(file string, path ...string) []string {
		raw, err := schemas.Files.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var node any
		if err := json.Unmarshal(raw, &node); err != nil {
			t.Fatal(err)
		}
		for _, step := range append(path, "enum") {
			node = node.(map[string]any)[step]
		}
		var enum []string
		for _, v := range node.([]any) {
			enum = append(enum, v.(string))
		}
		return slices.Sorted(slices.Values(enum))
	}
	for _, c := range []struct {
		file  string
		path  []string
		verbs []string
	}{
		{"review-list.schema.json", []string{"properties", "verbs", "items", "properties", "verb"}, route.VerbNames()},
		{"shelf-list.schema.json", []string{"$defs", "shelved", "properties", "verb"}, route.ShelfVerbs()},
	} {
		if enum, want := enumAt(c.file, c.path...), slices.Sorted(slices.Values(c.verbs)); !slices.Equal(enum, want) {
			t.Errorf("%s names the verbs %v, and the table %v", c.file, enum, want)
		}
	}
}
