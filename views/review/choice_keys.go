package review

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
)

// ChoiceService is the part of a tab's service that ChoiceKeys calls.
type ChoiceService interface {
	// Relate is the command that hands the item to psyche relate on this machine, which the psyche
	// choice hands the terminal to.
	Relate(ctx context.Context, key string) *exec.Cmd
	// Playlists lists the playlists that a watch can add a video to, as this machine reads them.
	Playlists(ctx context.Context) ([]Playlist, error)
}

// ChoiceKeys is what a choice's key does on each tab that places items, the review tab and the
// history tab alike: the playlist that a watch asks for, what an archive liked, the relate that a
// psyche placement hands the terminal to, and the URL that o opens. Each tab sends the write
// itself, so it keeps its own row once the write starts.
type ChoiceKeys struct {
	ctx     context.Context
	service ChoiceService
	outside Outside
	// playlists holds the playlists that a watch picks from, read once the first watch asks for
	// them, so a run that watches nothing reads none. playlistsErr is why the newest read failed.
	playlists     *gotermboard.Source[[]Playlist]
	wantPlaylists atomic.Bool
	playlistsErr  atomic.Pointer[error]
}

// NewChoiceKeys declares the playlists' read, reading nothing yet. Where outside.HandOver is nil,
// the relate takes the terminal as gotermboard.Model.HandOver does.
func NewChoiceKeys(ctx context.Context, service ChoiceService, outside Outside) *ChoiceKeys {
	if outside.HandOver == nil {
		outside.HandOver = func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd {
			return m.HandOver(cmd, then)
		}
	}
	k := &ChoiceKeys{ctx: ctx, service: service, outside: outside}
	k.playlists = gotermboard.NewSource(sourcePlaylists, time.Hour, k.readPlaylists)
	return k
}

// readPlaylists reads the playlists once a watch asked for them, and holds none before.
func (k *ChoiceKeys) readPlaylists(ctx context.Context) ([]Playlist, string, error) {
	if !k.wantPlaylists.Load() {
		return k.playlists.Value(), "", nil
	}
	lists, err := k.service.Playlists(ctx)
	if err != nil {
		k.playlistsErr.Store(&err)
		return k.playlists.Value(), "", fmt.Errorf("the playlists were not read: %w", err)
	}
	k.playlistsErr.Store(nil)
	return lists, "", nil
}

// Playlists is the playlists' read, which a tab lists among its sources.
func (k *ChoiceKeys) Playlists() gotermboard.Refresher { return k.playlists }

// PlaylistsStatus is how the playlists' read stands, for the strip, and false before a watch asked
// for them.
func (k *ChoiceKeys) PlaylistsStatus() (gotermboard.Status, bool) {
	return k.playlists.Status(), k.wantPlaylists.Load()
}

// ChoiceIn is the choice named name among choices, as the service answered them for the set in
// force, and why it places nothing, or empty where it places.
func ChoiceIn(choices []server.ReviewChoice, name string) (server.ReviewChoice, string) {
	if choices == nil {
		return server.ReviewChoice{}, "the service's choices were not read, so no choice's key places: the service could not read its set in force, or predates the choices"
	}
	i := slices.IndexFunc(choices, func(c server.ReviewChoice) bool { return c.Choice == name })
	switch {
	case i < 0:
		return server.ReviewChoice{}, "the service names no choice " + name + ", so " + ChoiceBindings[name].Key + " places nothing"
	case choices[i].Missing != nil:
		return choices[i], *choices[i].Missing
	}
	return choices[i], ""
}

// Pick places the item under key by choice through place, which sends body with the choice named,
// and a watch's playlist. A watch first asks which playlist, offering watchDefault. A choice that
// runs here hands the terminal to psyche relate on this machine once the placement lands, and
// relates nothing where it does not.
func (k *ChoiceKeys) Pick(m *gotermboard.Model, key string, choice server.ReviewChoice, watchDefault *string, body server.PlaceRequest,
	place func(body server.PlaceRequest, landed chan<- bool) tea.Cmd,
) tea.Cmd {
	body.Choice = choice.Choice
	if choice.Playlist {
		return k.watch(m, key, choice.Choice, watchDefault, func(playlist string) tea.Cmd {
			body.Playlist = playlist
			return place(body, nil)
		})
	}
	if !choice.RunsHere {
		return place(body, nil)
	}
	landed := make(chan bool, 1)
	write := place(body, landed)
	relate := func() tea.Msg {
		select {
		case ok := <-landed:
			if !ok {
				return nil
			}
		case <-k.ctx.Done():
			return nil
		}
		return k.outside.HandOver(m, k.service.Relate(k.ctx, key), func(m *gotermboard.Model, err error, stderr []byte) tea.Cmd {
			if err != nil {
				m.Say("the relate of " + Outsider(key) + " failed: " + gotermboard.LastLine(stderr, err) + ". `ingestion push " + key + " --to psyche --relate` runs it again")
			} else {
				m.Say("related " + Outsider(key) + " in psyche")
			}
			return nil
		})()
	}
	return tea.Batch(write, relate)
}

// Landed tells landed whether the placement landed, where landed is not nil, and says on a failure
// that nothing went to psyche.
func Landed(landed chan<- bool, err error) error {
	if landed == nil {
		return err
	}
	landed <- err == nil
	if err != nil {
		return fmt.Errorf("%w; nothing went to psyche", err)
	}
	return nil
}

// watch asks which playlist a pick by choice adds the video to. It lists the service's default
// first, then each playlist read on this machine, numbered. Enter takes the default, a number its
// playlist, and any other text names a playlist by its title or YouTube id. Enter with no default
// places nothing. The first watch begins the read, so its list shows on the next.
func (k *ChoiceKeys) watch(m *gotermboard.Model, key, choice string, fallback *string, place func(playlist string) tea.Cmd) tea.Cmd {
	if k.wantPlaylists.CompareAndSwap(false, true) {
		k.playlists.Force()
	}
	lists := playlistOrder(k.playlists.Value(), fallback)
	var listing []gotermboard.Line
	for i, p := range lists {
		label := fmt.Sprintf("%d  %s", i+1, Outsider(p.Title))
		if fallback != nil && (p.Title == *fallback || p.ID == *fallback) {
			label += "  (the default)"
		}
		listing = append(listing, gotermboard.Line{gotermboard.C(label, "")})
	}
	switch failed := k.playlistsErr.Load(); {
	case failed != nil:
		listing = append(listing, gotermboard.Line{gotermboard.C("the playlists were not read: "+(*failed).Error(), gotermboard.Yellow)})
	case len(lists) == 0 || (fallback != nil && len(lists) == 1):
		listing = append(listing, gotermboard.Line{gotermboard.C("reading the playlists; "+ChoiceBindings[choice].Key+" again lists them once read", "")})
	}
	text := choice + " " + Outsider(key) + " in which playlist? Type its number, title or id"
	if fallback != nil {
		text += ", or enter for " + Outsider(*fallback)
	}
	m.Ask(gotermboard.Question{
		Listing: listing, Text: text, Typed: true, Cancel: NothingWritten(m),
		Answer: func(typed string) tea.Cmd {
			playlist := typed
			if n, err := strconv.Atoi(typed); err == nil && n >= 1 && n <= len(lists) {
				playlist = lists[n-1].ID
			}
			if playlist == "" && fallback == nil {
				m.Say("no playlist named, and the service names no default, so nothing was placed")
				return nil
			}
			return place(playlist)
		},
	})
	return nil
}

// playlistOrder is the default first, then every other playlist by title. A default that no
// listed playlist names is listed by itself, so its number still picks it.
func playlistOrder(lists []Playlist, fallback *string) []Playlist {
	out := slices.Clone(lists)
	slices.SortStableFunc(out, func(a, b Playlist) int { return strings.Compare(strings.ToLower(a.Title), strings.ToLower(b.Title)) })
	if fallback == nil {
		return out
	}
	i := slices.IndexFunc(out, func(p Playlist) bool { return p.Title == *fallback || p.ID == *fallback })
	if i < 0 {
		return slices.Insert(out, 0, Playlist{ID: *fallback, Title: *fallback})
	}
	first := out[i]
	return slices.Insert(slices.Delete(out, i, i+1), 0, first)
}

// AskLike asks what an archive of the item under key liked, from a menu, then archives it through
// archive with the like picked, empty for none. The menu offers channel where it is not empty.
func AskLike(m *gotermboard.Model, key, channel string, archive func(liked string) tea.Cmd) tea.Cmd {
	likes := likesOf(channel)
	choices := make([]gotermboard.Choice, len(likes))
	for i, l := range likes {
		choices[i] = gotermboard.Choice{Key: l.key, Label: l.label}
	}
	m.Ask(gotermboard.Question{
		Text:    "archive " + Outsider(key) + ": done, its record kept and marked read. What did you like? The answer can propose a keep rule:",
		Choices: choices, Cancel: NothingWritten(m),
		Answer: func(picked string) tea.Cmd {
			i := slices.IndexFunc(likes, func(l cause) bool { return l.key == picked })
			return archive(likes[i].cause)
		},
	})
	return nil
}

// likesOf is what an archive may say was liked: the item's channel where its source names one, its
// theme, this item alone, and none.
func likesOf(channel string) []cause {
	var likes []cause
	if channel != "" {
		likes = append(likes, cause{"c", Outsider(route.LikedChannel + ", " + channel), route.LikedChannel})
	}
	return append(likes,
		cause{"h", route.LikedTheme, route.LikedTheme},
		cause{"i", "this item alone", route.LikedThisItem},
		cause{"n", "none, so it is kept as " + route.CauseUnstated, ""},
	)
}

// OpenURL hands the URL of the item under key to the browser. Only a web address opens, because
// the URL is the source's, and a stranger may have written it.
func (k *ChoiceKeys) OpenURL(m *gotermboard.Model, key string, rawURL *string) tea.Cmd {
	if rawURL == nil || *rawURL == "" {
		m.Say(Outsider(key) + " has no URL")
		return nil
	}
	u, err := url.Parse(*rawURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		m.Say("only an http or https address opens, and " + Outsider(key) + "'s is not one")
		return nil
	}
	if err := k.outside.Open(u.String()); err != nil {
		m.Say("the browser did not open: " + err.Error())
		return nil
	}
	m.Say("opened " + Outsider(u.String()))
	return nil
}

// NothingWritten is every question's esc.
func NothingWritten(m *gotermboard.Model) func() tea.Cmd {
	return func() tea.Cmd {
		m.Say("nothing written")
		return nil
	}
}
