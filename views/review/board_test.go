package review

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermstyle"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// fakeService answers the queue and the playlists it holds, and keeps each placement, archive and
// relate it is sent.
type fakeService struct {
	answer    server.ReviewAnswer
	playlists []Playlist
	placed    []server.PlaceRequest
	archived  []server.ArchiveRequest
	related   []string
	// refuse is the error that the next placement answers, once.
	refuse error
}

func (f *fakeService) Archive(_ context.Context, key string, body server.ArchiveRequest) (string, error) {
	f.archived = append(f.archived, body)
	return "archived " + key, nil
}

func (f *fakeService) Relate(ctx context.Context, key string) *exec.Cmd {
	f.related = append(f.related, key)
	return exec.CommandContext(ctx, "true")
}

func (f *fakeService) Playlists(context.Context) ([]Playlist, error) { return f.playlists, nil }

func (f *fakeService) Queue(context.Context) (server.ReviewAnswer, error) { return f.answer, nil }
func (f *fakeService) Parse(context.Context, string) (server.ParseAnswer, bool, error) {
	return server.ParseAnswer{}, false, nil
}

func (f *fakeService) Place(_ context.Context, key string, body server.PlaceRequest) (string, error) {
	if err := f.refuse; err != nil {
		f.refuse = nil
		return "", err
	}
	f.placed = append(f.placed, body)
	return "placed " + key, nil
}

func (f *fakeService) Drop(_ context.Context, key string, _ server.DropRequest) (string, error) {
	return "dropped " + key, nil
}

func (f *fakeService) Comments(context.Context, string) (server.ReviewComments, error) {
	return server.ReviewComments{State: server.CommentsReading}, nil
}

// video is a video waiting in review, as the queue answers it: made on day of November 2026,
// viewed views times, liked likes times and seconds long, with the gate's answer.
// Its sighting carries its channel's fields, which a drop decided by the channel stands for.
func video(id, title, channel, connector, reason string, day, views, likes, seconds int64, quality int, fit *int, verdict string) server.ReviewItem {
	answer, _ := json.Marshal(map[string]any{
		"genre": "explainer", "domains": []string{"energy"}, "themes": []string{"a theme"}, "quality": quality, "fit": fit,
		"idea": "An idea of " + id + ".", "verdict": verdict, "verdict_confidence": 0.7, "reason": "The opening of " + id + " decided it.",
		"side": connectors.SideFleet, "side_confidence": 0.8,
	})
	made := time.Date(2026, 11, int(day), 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	comments := int64(42)
	return server.ReviewItem{
		Key: "youtube/" + id, Zone: store.Outside, Title: &title, Reason: reason, QualityAnswer: answer,
		MediaType: "video", SourceTS: &made, Connectors: []string{connector}, WaitingSinceTS: "2026-11-25T00:00:00.000Z",
		Channel: &channel, ChannelFields: []string{"channel_title", "channel_id"},
		LengthS: &seconds, Views: &views, Likes: &likes, CommentCount: &comments,
		Comments: &server.ReviewComments{State: server.CommentsUnread},
		Sightings: []server.ReviewSighting{{
			Connector: connector, Fields: map[string]string{"channel_title": channel, "channel_id": "UC" + id},
		}},
	}
}

func fit(n int) *int { return &n }

// deck is four items: three videos the gate answered, two from one channel, and a Reddit save
// with no answer, no metrics and no source time, which began to wait before the rest.
func deck() server.ReviewAnswer {
	save, subreddit := "A post about sourdough", "r/baking"
	fleet, personal := connectors.SideFleet, connectors.SidePersonal
	inbox, watch, psyche, later := "work-inbox", "watch", "psyche", "Later"
	return server.ReviewAnswer{
		SchemaVersion: server.ReviewListVersion,
		Choices: []server.ReviewChoice{
			{Choice: route.ChoiceSuggestion, Side: &fleet, Destination: &inbox},
			{Choice: route.ChoiceWatch, Side: &personal, Destination: &watch, Playlist: true},
			{Choice: route.ChoicePsyche, Side: &personal, Destination: &psyche, RunsHere: true},
			{Choice: route.ChoiceArchive},
			{Choice: route.ChoiceDrop},
		},
		WatchDefaultPlaylist: &later,
		Items: []server.ReviewItem{
			video("vida", "Gardening with robots", "Allotment Lab", "ypl-capture", store.ReasonQualityReview, 20, 10_000, 900, 300, 5, fit(9), "pass"),
			video("vidb", "Tidal power explained", "Sea Notes", "ypl-channel", store.ReasonQualityReview, 24, 50_000, 1000, 1800, 6, fit(3), "unsure"),
			video("vidc", "Robots in the greenhouse", "Allotment Lab", "ypl-capture", route.CauseGateWouldDrop, 10, 2000, 300, 600, 8, nil, "drop"),
			{
				Key: "reddit/t3_dd4", Zone: store.Outside, Title: &save, Reason: route.CausePersonSorts, QualityAnswer: json.RawMessage("null"),
				Connectors: []string{"reddit-exports"}, WaitingSinceTS: "2026-11-01T00:00:00.000Z", Channel: &subreddit, ChannelFields: []string{},
				Sightings: []server.ReviewSighting{{Connector: "reddit-exports", Fields: map[string]string{"subreddit": "baking"}}},
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
// view, shortest first, and the gate's fit, each item with no value last. It groups by channel or
// connector, each group in the sort's order, or draws one list, and its filter reads the title and
// the channel.
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
		for _, row := range b.ordered(store.GroupByNone) {
			got = append(got, strings.TrimPrefix(strings.TrimPrefix(row.Key, "youtube/"), "reddit/"))
		}
		if !slices.Equal(got, want) {
			t.Errorf("under %s the queue runs %v, want %v", sorts[i], got, want)
		}
	}

	b.sortAt = slices.Index(sorts, store.SortQuality)
	for grouping, want := range map[string][][]string{
		store.GroupByConnector: {{"connector/ypl-capture", "vidc", "vida"}, {"connector/reddit-exports", "t3_dd4"}, {"connector/ypl-channel", "vidb"}},
		store.GroupByChannel:   {{"channel/Allotment Lab", "vidc", "vida"}, {"channel/Sea Notes", "vidb"}, {"channel/r/baking", "t3_dd4"}},
		store.GroupByNone:      {{"all", "vidc", "vidb", "vida", "t3_dd4"}},
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

// The card and the row show a video's length as m:ss or h:mm:ss and its likes per view as a
// percentage, and date an item by when its source made it, or by when it began to wait where its
// source gave no time.
func TestTheCardsNumbersAreTheItemsOwn(t *testing.T) {
	h := readBoard(t, &fakeService{answer: deck()}).queue.Value()
	vidc, _ := h.item("youtube/vidc")
	save, _ := h.item("reddit/t3_dd4")
	if got := length(*vidc.LengthS); got != "10:00" {
		t.Errorf("600 seconds read %q, want 10:00", got)
	}
	if got := length(3725); got != "1:02:05" {
		t.Errorf("3725 seconds read %q, want 1:02:05", got)
	}
	if ratio, ok := likesPerView(vidc); !ok || percent(ratio) != "15.0%" {
		t.Errorf("300 likes over 2000 views read %v, %v, want 15.0%%", ratio, ok)
	}
	if _, ok := likesPerView(save); ok {
		t.Error("a save with no views has likes per view")
	}
	if got := made(vidc); !got.Equal(time.Date(2026, 11, 10, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("the video was made %v, want its upload", got)
	}
	if got := made(save); !got.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("the save was made %v, want when it began to wait", got)
	}
}

// A person filters the tab to one row, then decides on it. The decision keeps the card as the tab
// showed it: the sort and grouping in force, the item's rank under the sort, its place among the
// rows that the filter left, and the gate's numbers beside it.
func TestAPlacementFromAFilteredTabKeepsTheCardAsTheTabShowedIt(t *testing.T) {
	service := &fakeService{answer: deck()}
	b := readBoard(t, service)
	m := gotermboard.NewModel(b, gotermstyle.PaletteFor(nil))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 40})
	runKey(m, letter('s'))
	runKey(m, letter('v'))
	runKey(m, letter('/'))
	m.Update(tea.PasteMsg{Content: "greenhouse"})
	runKey(m, tea.KeyPressMsg{Code: tea.KeyEnter})
	if m.Cursor() != "youtube/vidc" {
		t.Fatalf("filtered to the greenhouse video, the cursor rests on %q", m.Cursor())
	}
	suggestion := ChoiceBindings[route.ChoiceSuggestion].Key
	runKey(m, letter(suggestion[0]))
	if len(service.placed) != 1 {
		t.Fatalf("%s sent %d placements, want one", suggestion, len(service.placed))
	}
	body := service.placed[0]
	p := body.Presented
	if body.Choice != route.ChoiceSuggestion || body.Destination != "" || p == nil || p.Sort != store.SortQuality || p.Grouping != store.GroupByConnector || p.Rank != 1 || p.Position != 1 || p.Shown != 1 ||
		p.Quality == nil || *p.Quality != 8 || p.Fit != nil || p.Verdict == nil || *p.Verdict != "drop" ||
		p.Side == nil || *p.Side != connectors.SideFleet || body.DecidedAfterMS == nil || *body.DecidedAfterMS < 0 {
		t.Errorf("%s placed by %s keeping %+v after %v ms, want suggestion with the one row shown, first under quality, grouped by connector",
			suggestion, body.Choice, p, body.DecidedAfterMS)
	}
}

// W asks which playlist, the service's default first and then each playlist this machine lists.
// Enter takes the default, which the placement leaves to the service, and a number its playlist's
// id.
func TestWatchAsksForThePlaylistWithTheDefaultFirst(t *testing.T) {
	service := &fakeService{answer: deck(), playlists: []Playlist{{ID: "PLb", Title: "Bench"}, {ID: "PLl", Title: "Later"}, {ID: "PLa", Title: "Audio"}}}
	b := readBoard(t, service)
	b.choiceKeys.wantPlaylists.Store(true)
	b.choiceKeys.playlists.Refresh(t.Context())
	m := gotermboard.NewModel(b, gotermstyle.PaletteFor(nil))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 40})
	first := m.Cursor()
	watch := ChoiceBindings[route.ChoiceWatch].Key
	runKey(m, letter(watch[0]))
	runKey(m, tea.KeyPressMsg{Code: tea.KeyEnter})
	runKey(m, letter(watch[0]))
	runKey(m, letter('3'))
	runKey(m, tea.KeyPressMsg{Code: tea.KeyEnter})
	if len(service.placed) != 2 || service.placed[0].Choice != route.ChoiceWatch || service.placed[0].Playlist != "" || service.placed[1].Playlist != "PLb" {
		t.Fatalf("after W enter on %s, then W 3 enter, the placements are %+v, want the default, then Bench by its id", first, service.placed)
	}
}

// P places the item at psyche and hands the terminal to psyche relate on this machine once the
// placement lands, and relates nothing where the placement is refused. E asks what
// was liked, and archives it with the like picked.
func TestPsycheRelatesHereAndArchiveAsksWhatWasLiked(t *testing.T) {
	service := &fakeService{answer: deck()}
	b := NewBoard(t.Context(), service, Outside{HandOver: func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd {
		return func() tea.Msg { return then(m, cmd.Run(), nil) }
	}})
	if err := b.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	m := gotermboard.NewModel(b, gotermstyle.PaletteFor(nil))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 40})
	first := m.Cursor()
	service.refuse = errors.New("422 choice_refused: psyche is not declared")
	runKey(m, letter(ChoiceBindings[route.ChoicePsyche].Key[0]))
	if len(service.related) != 0 {
		t.Fatalf("P on %s related %v though its placement was refused, want nothing sent to psyche", first, service.related)
	}
	m.MoveCursorTo(first)
	runKey(m, letter(ChoiceBindings[route.ChoicePsyche].Key[0]))
	if len(service.placed) != 1 || service.placed[0].Choice != route.ChoicePsyche || !slices.Equal(service.related, []string{first}) {
		t.Fatalf("P on %s placed %+v and related %v, want it placed at psyche and related here", first, service.placed, service.related)
	}
	next := m.Cursor()
	runKey(m, letter(ChoiceBindings[route.ChoiceArchive].Key[0]))
	runKey(m, letter('h'))
	if len(service.archived) != 1 || service.archived[0].Liked != route.LikedTheme {
		t.Errorf("E h on %s archived %+v, want it archived for its theme", next, service.archived)
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

// Each choice has one key, and no two choices share one. A choice with no
// binding would never be offered.
func TestEachChoiceHasItsOwnKey(t *testing.T) {
	var keys []string
	for _, c := range route.Choices {
		bound, ok := ChoiceBindings[c.Name]
		if !ok || bound.Key == "" || bound.Footer == "" {
			t.Errorf("choice %s has the binding %+v", c.Name, bound)
		}
		keys = append(keys, bound.Key)
	}
	if len(ChoiceBindings) != len(route.Choices) || len(slices.Compact(slices.Sorted(slices.Values(keys)))) != len(keys) {
		t.Errorf("the bindings %v give the choices %v keys %v, want one key each", ChoiceBindings, route.ChoiceNames(), keys)
	}
}

// The watch's question lists what ypl playlists list --json prints, each an
// entry of ypl's own playlists answer. A field that ypl renames would list a
// blank row, so each field that Playlist reads is in ypl's document.
func TestAPlaylistReadsTheFieldsThatYPLPrints(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "ypl", "testdata", "wire", "playlists.json"))
	if err != nil {
		t.Fatal(err)
	}
	var answer struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil || len(answer.Data) == 0 {
		t.Fatalf("ypl's playlists answer holds %d playlists: %v", len(answer.Data), err)
	}
	fields := reflect.TypeFor[Playlist]()
	for i := range fields.NumField() {
		if name := fields.Field(i).Tag.Get("json"); answer.Data[0][name] == nil {
			t.Errorf("Playlist reads %q, and ypl's playlist holds only %v", name, slices.Sorted(maps.Keys(answer.Data[0])))
		}
	}
}
