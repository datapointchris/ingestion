package history

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/review"
)

// KeyOutcome cycles the outcome filter, as mutt's l limits its index to a pattern.
const KeyOutcome = "l"

// Actions is gotermboard.Keyed's: a key per placing choice and archive, each acting on the item
// under the cursor on the review tab's key for it, then the outcome filter and the URL.
func (b *Board) Actions() []gotermboard.Action {
	var actions []gotermboard.Action
	for _, c := range route.Choices {
		bound, ok := review.ChoiceBindings[c.Name]
		if !ok || (!c.Places() && c.Name != route.ChoiceArchive) {
			continue
		}
		run := b.onItem(b.pick(c))
		if c.Name == route.ChoiceArchive {
			run = b.onItem(b.archive)
		}
		actions = append(actions, gotermboard.Action{
			Keys: []string{bound.Key}, Label: c.Name + ", as your decision, which overrules a gate's drop: " + c.Help, Footer: bound.Key + " " + bound.Footer, Run: run,
		})
	}
	return append(actions,
		gotermboard.Action{Keys: []string{KeyOutcome}, Label: outcomeLabel(), Footer: KeyOutcome + " outcome", Run: func(m *gotermboard.Model) tea.Cmd {
			at := (int(b.filterAt.Load()) + 1) % len(filters)
			b.ShowOutcome(filters[at])
			m.Say("showing " + filterWords(filters[at]) + ", reading them")
			m.Rebuild()
			return nil
		}},
		gotermboard.Action{Keys: []string{"o"}, Label: "open the item's URL in the browser", Footer: "o open", Run: b.onItem(b.openURL)},
	)
}

// outcomeLabel is the l key's label: each filter in the order l cycles them.
func outcomeLabel() string {
	words := make([]string, len(filters))
	for i, f := range filters {
		words[i] = filterWords(f)
	}
	return "show the next outcome, reading the items again under it, in turn: " + strings.Join(words, ", then ")
}

// onItem runs act on the item under the cursor. A key on a group or on nothing does nothing. A key
// on an item whose write is queued, running, or landed and not yet read again refuses, because
// the item drawn is what the read held before that write.
func (b *Board) onItem(act func(*gotermboard.Model, server.ItemListed) tea.Cmd) func(*gotermboard.Model) tea.Cmd {
	return func(m *gotermboard.Model) tea.Cmd {
		it, listed := b.items.Value().item(m.Cursor())
		if !listed {
			return nil
		}
		if pending, held := b.writes.Pending(it.Key); held && pending.Refusal() != "" {
			m.Say(pending.Refusal())
			return nil
		}
		return act(m, it)
	}
}

// pick places it by c. A watch first asks which playlist. A psyche pick hands the terminal to
// psyche relate on this machine once the placement lands, and relates nothing where it does not.
func (b *Board) pick(c route.Choice) func(*gotermboard.Model, server.ItemListed) tea.Cmd {
	return func(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
		if c.TakesPlaylist() {
			return b.watch(m, it, c.Name)
		}
		if !c.RunsHere {
			return b.placeWrite(m, it, server.PlaceRequest{Choice: c.Name}, nil)
		}
		landed := make(chan bool, 1)
		write := b.placeWrite(m, it, server.PlaceRequest{Choice: c.Name}, landed)
		key := it.Key
		relate := func() tea.Msg {
			select {
			case ok := <-landed:
				if !ok {
					return nil
				}
			case <-b.ctx.Done():
				return nil
			}
			return b.outside.HandOver(m, b.service.Relate(b.ctx, key), func(m *gotermboard.Model, err error, stderr []byte) tea.Cmd {
				if err != nil {
					m.Say("the relate of " + outsider(key) + " failed: " + gotermboard.LastLine(stderr, err) + ". `ingestion push " + key + " --to psyche --relate` runs it again")
				} else {
					m.Say("related " + outsider(key) + " in psyche")
				}
				return nil
			})()
		}
		return tea.Batch(write, relate)
	}
}

// placeWrite sends body, the placement of it. Where landed is not nil, it carries whether the
// placement landed, and a failure says that nothing went to psyche.
func (b *Board) placeWrite(m *gotermboard.Model, it server.ItemListed, body server.PlaceRequest, landed chan<- bool) tea.Cmd {
	key := it.Key
	doing := "placing " + key + " by " + body.Choice
	if body.Playlist != "" {
		doing += " into " + body.Playlist
	}
	return b.writeRow(m, key, doing, func() (string, error) {
		said, err := b.service.Place(b.ctx, key, body)
		if landed != nil {
			landed <- err == nil
			if err != nil {
				err = fmt.Errorf("%w; nothing went to psyche", err)
			}
		}
		return said, err
	})
}

// watch asks which playlist a pick by choice adds the video to, listing each playlist that this
// machine reads, numbered. Enter takes the service's default, a number its playlist, and any other
// text names a playlist by its title or YouTube id. The first watch begins the read, so its list
// shows on the next.
func (b *Board) watch(m *gotermboard.Model, it server.ItemListed, choice string) tea.Cmd {
	if b.wantPlaylists.CompareAndSwap(false, true) {
		b.playlists.Force()
	}
	lists := slices.Clone(b.playlists.Value())
	slices.SortStableFunc(lists, func(a, z review.Playlist) int {
		return strings.Compare(strings.ToLower(a.Title), strings.ToLower(z.Title))
	})
	var listing []gotermboard.Line
	for i, p := range lists {
		listing = append(listing, gotermboard.Line{gotermboard.C(fmt.Sprintf("%d  %s", i+1, outsider(p.Title)), "")})
	}
	switch failed := b.playlistsErr.Load(); {
	case failed != nil:
		listing = append(listing, gotermboard.Line{gotermboard.C("the playlists were not read: "+(*failed).Error(), gotermboard.Yellow)})
	case len(lists) == 0:
		listing = append(listing, gotermboard.Line{gotermboard.C("reading the playlists; "+review.ChoiceBindings[choice].Key+" again lists them once read", "")})
	}
	m.Ask(gotermboard.Question{
		Listing: listing, Text: choice + " " + outsider(it.Key) + " in which playlist? Type its number, title or id, or enter for the service's default",
		Typed: true, Cancel: nothingWritten(m),
		Answer: func(typed string) tea.Cmd {
			playlist := typed
			if n, err := strconv.Atoi(typed); err == nil && n >= 1 && n <= len(lists) {
				playlist = lists[n-1].ID
			}
			return b.placeWrite(m, it, server.PlaceRequest{Choice: choice, Playlist: playlist}, nil)
		},
	})
	return nil
}

// like is one answer to what an archive liked: the key that picks it, what the menu says, and the
// like the archive records, empty for none.
type like struct{ key, label, liked string }

// archive asks what was liked, from a menu, then archives it under that like. Its channel is
// offered where its sightings name one.
func (b *Board) archive(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
	var likes []like
	if it.Channel != nil {
		likes = append(likes, like{"c", outsider(route.LikedChannel + ", " + *it.Channel), route.LikedChannel})
	}
	likes = append(likes,
		like{"h", route.LikedTheme, route.LikedTheme},
		like{"i", "this item alone", route.LikedThisItem},
		like{"n", "none, so it is kept as " + route.CauseUnstated, ""},
	)
	choices := make([]gotermboard.Choice, len(likes))
	for i, l := range likes {
		choices[i] = gotermboard.Choice{Key: l.key, Label: l.label}
	}
	key := it.Key
	m.Ask(gotermboard.Question{
		Text:    "archive " + outsider(key) + ": done, its record kept and marked read. What did you like?",
		Choices: choices, Cancel: nothingWritten(m),
		Answer: func(picked string) tea.Cmd {
			i := slices.IndexFunc(likes, func(l like) bool { return l.key == picked })
			body := server.ArchiveRequest{Liked: likes[i].liked}
			return b.writeRow(m, key, "archiving "+key, func() (string, error) { return b.service.Archive(b.ctx, key, body) })
		},
	})
	return nil
}

// openURL hands the item's URL to the browser. Only a web address opens, because the URL is the
// source's, and a stranger may have written it.
func (b *Board) openURL(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
	if it.URL == nil || *it.URL == "" {
		m.Say(outsider(it.Key) + " has no URL")
		return nil
	}
	u, err := url.Parse(*it.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		m.Say("only an http or https address opens, and " + outsider(it.Key) + "'s is not one")
		return nil
	}
	if err := b.outside.Open(u.String()); err != nil {
		m.Say("the browser did not open: " + err.Error())
		return nil
	}
	m.Say("opened " + outsider(u.String()))
	return nil
}

// nothingWritten is every question's esc.
func nothingWritten(m *gotermboard.Model) func() tea.Cmd {
	return func() tea.Cmd {
		m.Say("nothing written")
		return nil
	}
}

// writeRow queues run on the item key names, confirmed on the screen's loop. The row stays where
// it is with the write named on it, and the read after the write lands draws its new outcome. A
// key on the item refuses until that read lands, unless the write failed, which puts the error on
// the row.
func (b *Board) writeRow(m *gotermboard.Model, key, doing string, run func() (string, error)) tea.Cmd {
	cmd := b.writes.Start(m, keywrites.Write{Doing: doing, Run: run, Row: key}, b.reread)
	m.Rebuild()
	return cmd
}
