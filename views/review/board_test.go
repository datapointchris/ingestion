package review

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermstyle"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/ypl"
)

// fakeService answers the queue it holds, and keeps each placement it is sent.
type fakeService struct {
	answer server.ReviewAnswer
	placed []server.PlaceRequest
}

func (f *fakeService) Queue(context.Context) (server.ReviewAnswer, error) { return f.answer, nil }
func (f *fakeService) Parse(context.Context, string) (server.ParseAnswer, bool, error) {
	return server.ParseAnswer{}, false, nil
}

func (f *fakeService) Place(_ context.Context, key string, body server.PlaceRequest) (string, error) {
	f.placed = append(f.placed, body)
	return "placed " + key, nil
}

func (f *fakeService) Drop(_ context.Context, key string, _ server.DropRequest) (string, error) {
	return "dropped " + key, nil
}

func (f *fakeService) Mark(_ context.Context, key, mark string) (string, error) {
	return "marked " + key + " " + mark, nil
}

func (f *fakeService) Comments(context.Context, string) (server.ReviewComments, error) {
	return server.ReviewComments{State: server.CommentsReading}, nil
}

// video is a video waiting in review, as the queue answers it: made on day of November 2026,
// viewed views times, liked likes times and seconds long, with the gate's answer.
func video(id, title, channel, connector, reason string, day, views, likes, seconds int64, quality int, fit *int, verdict string) server.ReviewItem {
	answer, _ := json.Marshal(map[string]any{
		"genre": "explainer", "domains": []string{"energy"}, "themes": []string{"a theme"}, "quality": quality, "fit": fit,
		"idea": "An idea of " + id + ".", "verdict": verdict, "verdict_confidence": 0.7, "reason": "The opening of " + id + " decided it.",
		"side": connectors.SideFleet, "side_confidence": 0.8,
	})
	made := time.Date(2026, 11, int(day), 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	return server.ReviewItem{
		Key: "youtube/" + id, Zone: store.Outside, Title: &title, Reason: reason, QualityAnswer: answer,
		MediaType: ypl.MediaTypeVideo, SourceTS: &made, Connectors: []string{connector}, WaitingSinceTS: "2026-11-25T00:00:00.000Z",
		Sightings: []server.ReviewSighting{{
			Connector: connector, Fields: map[string]string{ypl.FieldChannelTitle: channel},
			Metrics: map[string]int64{ypl.MetricViews: views, ypl.MetricLikes: likes, ypl.MetricComments: 42, ypl.MetricDuration: seconds},
		}},
	}
}

func fit(n int) *int { return &n }

// deck is four items: three videos the gate answered, two from one channel, and a Reddit save
// with no answer, no metrics and no source time, which began to wait before the rest.
func deck() server.ReviewAnswer {
	save := "A post about sourdough"
	fleet, personal := connectors.SideFleet, connectors.SidePersonal
	return server.ReviewAnswer{
		SchemaVersion: 3,
		Destinations:  []server.ReviewDestination{{Name: "reading-list", Side: &personal}, {Name: "work-inbox", Side: &fleet}},
		Items: []server.ReviewItem{
			video("vida", "Gardening with robots", "Allotment Lab", "ypl-capture", store.ReasonQualityReview, 20, 10_000, 900, 300, 5, fit(9), "pass"),
			video("vidb", "Tidal power explained", "Sea Notes", "ypl-channel", store.ReasonQualityReview, 24, 50_000, 1000, 1800, 6, fit(3), "unsure"),
			video("vidc", "Robots in the greenhouse", "Allotment Lab", "ypl-capture", route.CauseGateWouldDrop, 10, 2000, 300, 600, 8, nil, "drop"),
			{
				Key: "reddit/t3_dd4", Zone: store.Outside, Title: &save, Reason: route.CausePersonSorts, QualityAnswer: json.RawMessage("null"),
				Connectors: []string{"reddit-exports"}, WaitingSinceTS: "2026-11-01T00:00:00.000Z",
				Sightings: []server.ReviewSighting{{Connector: "reddit-exports", Fields: map[string]string{redditexport.FieldSubreddit: "baking"}}},
			},
		},
	}
}

func readBoard(t *testing.T, service Service) *Board {
	t.Helper()
	b := NewBoard(t.Context(), service, Outside{})
	if err := b.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	return b
}

// drawn is each group the frame draws under grouping, as its key and its rows' keys.
func drawn(b *Board, grouping string) [][]string {
	var out [][]string
	for _, g := range b.Frame(time.Now(), grouping).Groups {
		out = append(out, append([]string{g.Key}, rowKeys(g.Rows)...))
	}
	return out
}

func rowKeys(rows []gotermboard.Row) []string {
	var keys []string
	for _, r := range rows {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(r.Key, "youtube/"), "reddit/"))
	}
	return keys
}

// The tab sorts the queue by when each item was made at its source, the gate's quality, likes per
// view, shortest first, and the gate's fit, each item with no value last. It groups by connector,
// channel or the gate's verdict, each group in the sort's order, and its filter reads the title
// and the channel.
func TestTheReviewTabSortsGroupsAndFiltersTheQueue(t *testing.T) {
	b := readBoard(t, &fakeService{answer: deck()})
	for i, want := range [][]string{
		{"vidb", "vida", "vidc", "t3_dd4"},
		{"vidc", "vidb", "vida", "t3_dd4"},
		{"vidc", "vida", "vidb", "t3_dd4"},
		{"vida", "vidc", "vidb", "t3_dd4"},
		{"vida", "vidb", "vidc", "t3_dd4"},
	} {
		b.sortAt = i
		var got []string
		for _, row := range b.ordered(byNone) {
			got = append(got, strings.TrimPrefix(strings.TrimPrefix(row.Key, "youtube/"), "reddit/"))
		}
		if !slices.Equal(got, want) {
			t.Errorf("under %s the queue runs %v, want %v", sorts[i], got, want)
		}
	}

	b.sortAt = slices.Index(sorts, sortQuality)
	for grouping, want := range map[string][][]string{
		byConnector: {{"connector/ypl-capture", "vidc", "vida"}, {"connector/reddit-exports", "t3_dd4"}, {"connector/ypl-channel", "vidb"}},
		byChannel:   {{"channel/Allotment Lab", "vidc", "vida"}, {"channel/Sea Notes", "vidb"}, {"channel/r/baking", "t3_dd4"}},
		byVerdict:   {{"verdict/pass", "vida"}, {"verdict/unsure", "vidb"}, {"verdict/drop", "vidc"}, {"verdict/none", "t3_dd4"}},
	} {
		if got := drawn(b, grouping); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("grouped by %s under quality the frame draws %v, want %v", grouping, got, want)
		}
	}

	m := gotermboard.NewModel(b, gotermstyle.PaletteFor(nil))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 40})
	for _, filter := range []struct{ typed, shows, hides string }{
		{"robots", "Robots in the greenhouse", "Tidal power"},
		{"sea notes", "Tidal power", "Gardening with robots"},
	} {
		m.Update(letter('/'))
		m.Update(tea.PasteMsg{Content: filter.typed})
		m.Update(tea.KeyPressMsg{Code: tea.KeyEnter})
		if view := m.View().Content; !strings.Contains(view, filter.shows) || strings.Contains(view, filter.hides) || strings.Contains(view, "sourdough") {
			t.Errorf("filtered by %q the tab draws:\n%s\nwant %q and neither %q nor the Reddit save", filter.typed, view, filter.shows, filter.hides)
		}
		m.Update(tea.KeyPressMsg{Code: tea.KeyEscape})
	}
}

// A video's card names its channel, length and upload day, its views, likes, comments and likes
// per view, and the gate's quality, fit, verdict, reason and side hint, and says where the gate
// would drop it.
func TestAVideosCardShowsItsNumbersAndTheGatesAnswer(t *testing.T) {
	b := readBoard(t, &fakeService{answer: deck()})
	card := b.document("youtube/vidc")
	for _, want := range []string{
		`**channel** Allotment Lab`, `**length** 10:00`, `**made** 2026\-11\-10`, `**views** 2000`, `**likes** 300`, `**comments** 42`,
		`**likes per view** 15\.0\%`, `**quality** 8 of 10`, `**fit** none`, `**verdict** drop at 0.70 confidence`,
		`**reason** The opening of vidc decided it\.`, `**side hint** fleet at 0.80 confidence`, `**gate\-would\-drop** the gate would drop it`,
		`**comments read** not read`,
	} {
		if !strings.Contains(card, want) {
			t.Errorf("the card lacks %q:\n%s", want, card)
		}
	}
}

// A decision keeps the card as the tab drew it: the sort and grouping in force, the item's rank
// under the sort and its place among the rows drawn, and the gate's numbers beside it.
func TestAPlacementKeepsTheCardAsTheTabPresentedIt(t *testing.T) {
	service := &fakeService{answer: deck()}
	b := readBoard(t, service)
	m := gotermboard.NewModel(b, gotermstyle.PaletteFor(nil))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 40})
	runKey(m, letter('s'))
	runKey(m, letter('v'))
	if m.Cursor() != "youtube/vidb" {
		t.Fatalf("sorted by quality and grouped by connector, the cursor rests on %q, want the row it rested on before", m.Cursor())
	}
	runKey(m, letter(KeyFleetRelate[0]))
	if len(service.placed) != 1 {
		t.Fatalf("F sent %d placements, want one", len(service.placed))
	}
	body := service.placed[0]
	p := body.Presented
	if body.Destination != "work-inbox" || p == nil || p.Sort != sortQuality || p.Grouping != byConnector || p.Rank != 2 || p.Position != 4 || p.Shown != 4 ||
		p.Quality == nil || *p.Quality != 6 || p.Fit == nil || *p.Fit != 3 || p.Verdict == nil || *p.Verdict != "unsure" ||
		p.Side == nil || *p.Side != connectors.SideFleet || body.DecidedAfterMS == nil || *body.DecidedAfterMS < 0 {
		t.Errorf("F placed on %s keeping %+v after %v ms, want work-inbox with the last row drawn, second under quality, grouped by connector",
			body.Destination, p, body.DecidedAfterMS)
	}
}

func letter(r byte) tea.KeyPressMsg { return tea.KeyPressMsg{Code: rune(r), Text: string(r)} }

// runKey presses key on m and runs each command it starts, as Bubble Tea would.
func runKey(m *gotermboard.Model, key tea.KeyPressMsg) {
	_, cmd := m.Update(key)
	for cmd != nil {
		msg := cmd()
		cmd = nil
		if batch, ok := msg.(tea.BatchMsg); ok {
			for _, c := range batch {
				if c != nil {
					c()
				}
			}
		}
	}
	m.Rebuild()
}
