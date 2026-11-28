package history

import (
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermboard/keywrites"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/views/review"
)

// KeyOutcome steps the outcome filter on, and KeyOutcomeBack steps it back, as h and l step
// through a view's modes in vi-keyed terminal tools.
const (
	KeyOutcome     = "l"
	KeyOutcomeBack = "h"
)

// Actions is gotermboard.Keyed's: a key per placing choice and archive, each acting on the item
// under the cursor on the review tab's key for it, then the outcome filter and the URL.
func (b *Board) Actions() []gotermboard.Action {
	var actions []gotermboard.Action
	for _, c := range route.Choices {
		bound, ok := review.ChoiceBindings[c.Name]
		if !ok || (!c.Places() && c.Name != route.ChoiceArchive) {
			continue
		}
		run := b.onItem(b.chooseBy(c.Name))
		if c.Name == route.ChoiceArchive {
			run = b.onItem(b.archive)
		}
		actions = append(actions, gotermboard.Action{
			Keys: []string{bound.Key}, Label: c.Name + ", as your decision, which overrules a gate's drop: " + c.Help, Footer: bound.Key + " " + bound.Footer, Run: run,
		})
	}
	return append(actions,
		gotermboard.Action{Keys: []string{KeyOutcome}, Label: outcomeLabel(), Footer: KeyOutcomeBack + "/" + KeyOutcome + " outcome", Run: b.stepOutcome(1)},
		gotermboard.Action{Keys: []string{KeyOutcomeBack}, Label: "show the outcome before, reading the items again under it", Run: b.stepOutcome(-1)},
		gotermboard.Action{Keys: []string{"o"}, Label: "open the item's URL in the browser", Footer: "o open", Run: b.onItem(func(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
			return b.choiceKeys.OpenURL(m, it.Key, it.URL)
		})},
	)
}

// stepOutcome puts the filter by places from the one in force in force, wrapping at either end.
func (b *Board) stepOutcome(by int) func(*gotermboard.Model) tea.Cmd {
	return func(m *gotermboard.Model) tea.Cmd {
		at := (int(b.filterAt.Load()) + by + len(filters)) % len(filters)
		b.ShowOutcome(filters[at])
		m.Say("showing " + filterWords(filters[at]) + ", reading them")
		m.Rebuild()
		return nil
	}
}

// outcomeLabel is the l key's label: each filter in the order l steps through them.
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

// chooseBy places it by the choice named name, once the service's choices say that the set in
// force takes it, as review.ChoiceKeys.Pick asks and relates.
func (b *Board) chooseBy(name string) func(*gotermboard.Model, server.ItemListed) tea.Cmd {
	return func(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
		offer := b.choices.Value()
		choice, why := review.ChoiceIn(offer.choices, name)
		if why != "" {
			m.Say(why)
			return nil
		}
		return b.choiceKeys.Pick(m, it.Key, choice, offer.watchDefault, server.PlaceRequest{}, func(body server.PlaceRequest, landed chan<- bool) tea.Cmd {
			doing := "placing " + it.Key + " by " + body.Choice
			if body.Playlist != "" {
				doing += " into " + body.Playlist
			}
			return b.writeRow(m, it.Key, doing, func() (string, error) {
				said, err := b.service.Place(b.ctx, it.Key, body)
				return said, review.Landed(landed, err)
			})
		})
	}
}

// archive asks what was liked, then archives it under that like.
func (b *Board) archive(m *gotermboard.Model, it server.ItemListed) tea.Cmd {
	channel := ""
	if it.Channel != nil {
		channel = *it.Channel
	}
	return review.AskLike(m, it.Key, channel, func(liked string) tea.Cmd {
		body := server.ArchiveRequest{Liked: liked}
		return b.writeRow(m, it.Key, "archiving "+it.Key, func() (string, error) { return b.service.Archive(b.ctx, it.Key, body) })
	})
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
