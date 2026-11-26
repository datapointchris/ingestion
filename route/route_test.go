package route_test

import (
	"encoding/json"
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

// The side gate places each item by the first rung that decides it, and says
// who decided: one side's tags, then the first rule that holds, then a guess
// at or above the floor. Tags naming two sides, a guess below the floor and no
// guess at all each send the item to review.
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
		{"a rule", []store.Sighting{sighting("reddit-exports", "Cooking")}, confident, "personal", "rule:cooking", "rule"},
		{"a confident personal guess", []store.Sighting{sighting("reddit-exports", "golang")}, sure, "personal", "model:run-3", "guess"},
		{"a confident fleet guess", []store.Sighting{sighting("reddit-exports", "golang")}, confident, "review", "model:run-1", "below-floor"},
		{"a low-confidence guess", []store.Sighting{sighting("reddit-exports", "golang")}, doubtful, "review", "model:run-2", "below-floor"},
		{"no guess", []store.Sighting{sighting("reddit-exports", "golang")}, nil, "review", "gate", "no-guess"},
	} {
		d := route.Side(set, c.sightings, c.guess)
		if d.Verdict != c.verdict || d.DecidedBy != c.by || d.Cause != c.cause {
			t.Errorf("%s: the side gate answered %s by %s for %s, want %s by %s for %s", c.name, d.Verdict, d.DecidedBy, d.Cause, c.verdict, c.by, c.cause)
		}
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
// no prompt for that lens. The same item on the fleet side is due one.
func TestAModelReadIsDueOnlyUnderALensWhosePromptTheBuildHolds(t *testing.T) {
	modelRead := connectors.Set{Connectors: []connectors.Connector{{Name: "reddit-exports", ReadRule: connectors.ReadModel}}}
	for side, want := range map[string]bool{connectors.SidePersonal: false, connectors.SideFleet: true} {
		read := route.Read(modelRead, []store.Sighting{sighting("reddit-exports", "")}, side, nil)
		if due := route.ModelReadDue(store.Decision{Verdict: side}, read, nil, false); due != want {
			t.Errorf("a model read on the %s side under lens %v is due: %v, want %v", side, read.Lens, due, want)
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
