// Package promptproposal drafts a change to the quality gate's prompt from
// the gate's misses. A miss is one done answer of the gate on an item that a
// person's word went against. The word is a placement from review that
// accuracy counts as disagreeing with the gate's verdict or side. It is also
// a mark of low-quality on an item that the gate passed, or of read on one
// that it dropped.
//
// A projected miss holds keys, enums, numbers and the person's own words. It
// never holds a title, a URL, the item's text, the gate's domains, themes,
// idea or reason, or a sighting's fields, so the draft reads no outsider's
// text. Its proposal is therefore owner.
//
// serve's Scheduler drafts at each time of its schedule. It projects the
// misses labeled since the newest done proposal, so a failed draft's misses
// roll into the next. Where it finds any, it writes a prompt_proposals row
// that holds them and the prompt's text, and queues one prompt-proposal task.
// Where it finds none, it drafts nothing. A worker holding a model port runs
// the task under modelstage's dispatch-once protocol, and serve keeps the
// run's answer on the row. Nothing here changes a prompt. A person who
// accepts a proposal changes the prompt's file, and the build that carries it
// is a new version.
package promptproposal

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/datapointchris/ingestion/accuracy"
	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/gatetrust"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	// Capability is the capability of every prompt-proposal task.
	Capability = store.StagePromptProposal
	// Producer names a proposal's drafts, whose version is the
	// prompt-proposal prompt's.
	Producer = "prompt-proposal"
	// DefaultSchedule is the schedule where config names none.
	DefaultSchedule = "0 6 * * 1"
)

// How the person gave the word that a miss goes against.
const (
	// LabelPlacement is a placement of an item that the gates sent to review.
	LabelPlacement = "placement"
	// LabelMark is a mark on an item that the gate had decided.
	LabelMark = "mark"
)

// What a miss went against.
const (
	MissedVerdict = "verdict"
	MissedSide    = "side"
)

// Miss is one done answer of the quality gate that a person's word went
// against. Every field is a key, an enum, a number or the person's words.
type Miss struct {
	// N numbers the miss within its proposal, from 1, so a draft cites it.
	N          int      `json:"n"`
	ItemKey    string   `json:"item_key"`
	MediaType  string   `json:"media_type"`
	Connectors []string `json:"connectors"`
	// LabelKind is LabelPlacement or LabelMark, and Label the person's word:
	// personal, fleet or drop for a placement, and low-quality or read for a
	// mark.
	LabelKind string `json:"label_kind"`
	Label     string `json:"label"`
	// Destination, Cause and Note are a placement's: where the person placed
	// the item, what they said decided it, and the note they wrote. Each is
	// nil where they gave none, and on a mark.
	Destination *string `json:"destination"`
	Cause       *string `json:"cause"`
	Note        *string `json:"note"`
	// ReviewCause is why the item waited in review before the person placed
	// it, and nil on a mark.
	ReviewCause *string `json:"review_cause"`
	LabeledTS   string  `json:"labeled_ts"`
	// Missed names what the word went against: MissedVerdict, MissedSide or
	// both.
	Missed []string `json:"missed"`
	// PromptVersion, Model and Effort are what answered, as its gate family
	// names them.
	PromptVersion     string  `json:"prompt_version"`
	Model             string  `json:"model"`
	Effort            string  `json:"effort"`
	Verdict           string  `json:"verdict"`
	VerdictConfidence float64 `json:"verdict_confidence"`
	Side              string  `json:"side"`
	SideConfidence    float64 `json:"side_confidence"`
	Quality           int     `json:"quality"`
	Genre             string  `json:"genre"`
}

// Project reads each miss whose word was given after after, through through,
// the earliest word first, numbered from 1. An empty after reads from the
// first word.
func Project(ctx context.Context, st *store.Store, after, through string) ([]Miss, error) {
	placed, err := st.ReviewPlacedItems(ctx)
	if err != nil {
		return nil, err
	}
	var out []Miss
	for _, r := range placed {
		out = append(out, placementMisses(r, after, through)...)
	}
	marks, err := st.MarksBetween(ctx, after, through)
	if err != nil {
		return nil, err
	}
	records := map[string]store.ItemRecords{}
	for _, m := range marks {
		if m.Agreement != gatetrust.Disagreed || m.GateDecidedBy == nil {
			continue
		}
		r, ok := records[m.ItemKey]
		if !ok {
			if r, err = st.Item(ctx, m.ItemKey); err != nil {
				return nil, err
			}
			records[m.ItemKey] = r
		}
		if miss, ok := markMiss(r, m); ok {
			out = append(out, miss)
		}
	}
	slices.SortStableFunc(out, func(a, b Miss) int {
		return cmp.Or(cmp.Compare(a.LabeledTS, b.LabeledTS), cmp.Compare(a.ItemKey, b.ItemKey), cmp.Compare(a.LabelKind, b.LabelKind),
			cmp.Compare(a.PromptVersion, b.PromptVersion))
	})
	for i := range out {
		out[i].N = i + 1
	}
	return out, nil
}

// placementMisses is each of the gate's answers on r that its placement from
// review went against, where the placement fell after after, through through.
func placementMisses(r store.ItemRecords, after, through string) []Miss {
	placement, ok := r.ReviewPlacement()
	if !ok || placement.DecidedTS <= after || placement.DecidedTS > through {
		return nil
	}
	label := placement.Verdict
	var out []Miss
	for _, o := range accuracy.AnswersAt(r.Outputs, store.StageQuality) {
		a, err := qualitygate.DecodeAnswer(o.Answer)
		if err != nil {
			continue
		}
		var missed []string
		verdict, side := accuracy.Outcomes(a, label)
		if verdict == accuracy.Disagreed {
			missed = append(missed, MissedVerdict)
		}
		if side == accuracy.Disagreed {
			missed = append(missed, MissedSide)
		}
		if len(missed) == 0 {
			continue
		}
		miss := missOf(r, o, a, LabelPlacement, label, placement.DecidedTS, missed)
		miss.Destination, miss.Note = placement.Destination, placement.Note
		if review, ok := store.ReviewBefore(r.Decisions, placement); ok {
			miss.ReviewCause = &review.Cause
		}
		if placement.Cause != "" {
			miss.Cause = &placement.Cause
		}
		out = append(out, miss)
	}
	return out
}

// markMiss is the gate's answer that m disagreed with, where r holds it.
func markMiss(r store.ItemRecords, m store.Mark) (Miss, bool) {
	for _, o := range accuracy.AnswersAt(r.Outputs, store.StageQuality) {
		if store.DecidedByModel(o.Run()) != *m.GateDecidedBy {
			continue
		}
		a, err := qualitygate.DecodeAnswer(o.Answer)
		if err != nil {
			return Miss{}, false
		}
		return missOf(r, o, a, LabelMark, m.Mark, m.MarkedTS, []string{MissedVerdict}), true
	}
	return Miss{}, false
}

// missOf is the miss of answer a, in output o on r, that the word label went
// against.
func missOf(r store.ItemRecords, o store.Output, a qualitygate.Answer, kind, label, labeled string, missed []string) Miss {
	answerer := gatetrust.AnswererOf(o)
	return Miss{
		ItemKey: r.Item.Key, MediaType: r.Item.MediaType, Connectors: gatetrust.Connectors(r.Sightings),
		LabelKind: kind, Label: label, LabeledTS: labeled, Missed: missed,
		PromptVersion: answerer.PromptVersion, Model: answerer.Model, Effort: answerer.Effort,
		Verdict: a.Verdict, VerdictConfidence: a.VerdictConfidence, Side: a.Side, SideConfidence: a.SideConfidence,
		Quality: a.Quality, Genre: a.Genre,
	}
}

// Draft writes a proposal over the misses labeled since the newest done
// proposal, through through, and queues the task that drafts it. A failed or
// unfinished draft's misses therefore roll into the next. It answers the
// proposal, or why it wrote none: a draft is outstanding, a proposal already
// covers through, or no miss was labeled.
func Draft(ctx context.Context, st *store.Store, through time.Time) (store.PromptProposal, string, error) {
	end := store.Stamp(through)
	if newest, ok, err := st.NewestPromptProposal(ctx); err != nil {
		return store.PromptProposal{}, "", err
	} else if ok && newest.LabeledThrough >= end {
		return store.PromptProposal{}, "a proposal already covers the misses labeled through " + end, nil
	}
	after := ""
	if done, ok, err := st.NewestDonePromptProposal(ctx); err != nil {
		return store.PromptProposal{}, "", err
	} else if ok {
		after = done.LabeledThrough
	}
	found, err := Project(ctx, st, after, end)
	if err != nil {
		return store.PromptProposal{}, "", err
	}
	if len(found) == 0 {
		return store.PromptProposal{}, "no miss was labeled", nil
	}
	misses, err := json.Marshal(found)
	if err != nil {
		return store.PromptProposal{}, "", err
	}
	var written store.PromptProposal
	why := ""
	err = st.Update(ctx, func(tx *store.Tx) error {
		if task, ok, err := tx.DraftOutstanding(ctx); err != nil {
			return err
		} else if ok {
			why = "the draft of proposal " + *task.ProposalID + " is still " + task.State
			return nil
		}
		if newest, ok, err := tx.NewestPromptProposal(ctx); err != nil {
			return err
		} else if ok && newest.LabeledThrough >= end {
			why = "another write drafted the misses labeled through " + newest.LabeledThrough
			return nil
		}
		p := store.PromptProposal{
			Prompt: store.StageQuality, PromptVersion: prompts.Quality.Version, PromptText: prompts.Quality.Text(),
			LabeledThrough: end, MissCount: len(found), Misses: misses, Zone: store.Owner,
			StageRun: store.StageRun{Producer: Producer, ProducerVersion: prompts.PromptProposal.Version},
		}
		if after != "" {
			p.LabeledAfter = &after
		}
		if written, err = tx.AddPromptProposal(ctx, p); err != nil {
			return err
		}
		_, err = tx.Enqueue(ctx, store.Task{ProposalID: &written.ID, Stage: store.StagePromptProposal, Capability: Capability})
		return err
	})
	if err != nil || why != "" {
		return store.PromptProposal{}, why, err
	}
	return written, "", nil
}

const (
	// maxWait bounds how long the scheduler sleeps between passes, so a clock
	// that moved is met within the hour.
	maxWait = time.Hour
	// retryAfter is how long the scheduler waits after a pass failed.
	retryAfter = 5 * time.Minute
	// maxCatchUp bounds the schedule's times that one pass steps over to
	// find the latest, so a schedule of every minute after a long gap ends.
	maxCatchUp = 100000
)

// Scheduler drafts a proposal at each time that its schedule names.
type Scheduler struct {
	store    *store.Store
	schedule cron.Schedule
	log      *slog.Logger
	// started anchors the first time where the store holds no proposal, and
	// checked is the latest time that a pass drafted for.
	started, checked time.Time
}

// NewScheduler is the scheduler over st that drafts at each time of expr, a
// cron expression as a connector's schedule reads it.
func NewScheduler(st *store.Store, expr string, log *slog.Logger) (*Scheduler, error) {
	schedule, err := connectors.ParseSchedule(expr)
	if err != nil {
		return nil, fmt.Errorf("schedule %q is not a cron expression: %w", expr, err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: st, schedule: schedule, log: log, started: st.Now()}, nil
}

// Run passes until ctx ends, sleeping until the schedule's next time.
func (s *Scheduler) Run(ctx context.Context) {
	for {
		next, err := s.Pass(ctx)
		if err != nil && ctx.Err() == nil {
			s.log.ErrorContext(ctx, "a prompt proposal's pass failed", "error", err)
		}
		timer := time.NewTimer(min(max(next.Sub(s.store.Now()), time.Second), maxWait))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Pass drafts for the latest time of the schedule that has come since the
// newest proposal, once, and answers when to pass again. Once the store holds
// a proposal, a time that came while the service was down is drafted for on
// the first pass after. Before the first proposal, the first time after serve
// started is the first that drafts, and its window holds every earlier label.
func (s *Scheduler) Pass(ctx context.Context) (time.Time, error) {
	now := s.store.Now()
	next := s.schedule.Next(now)
	anchor := s.started
	newest, held, err := s.store.NewestPromptProposal(ctx)
	if err != nil {
		return now.Add(retryAfter), err
	}
	if held {
		if anchor, err = time.Parse(store.StampLayout, newest.LabeledThrough); err != nil {
			return now.Add(retryAfter), fmt.Errorf("read proposal %s's labeled_through: %w", newest.ID, err)
		}
	}
	var due time.Time
	for t, i := s.schedule.Next(anchor), 0; !t.IsZero() && !t.After(now); t, i = s.schedule.Next(t), i+1 {
		if due = t; i == maxCatchUp {
			due = now
			break
		}
	}
	if due.IsZero() || !due.After(s.checked) {
		return next, nil
	}
	p, why, err := Draft(ctx, s.store, due)
	if err != nil {
		return now.Add(retryAfter), err
	}
	s.checked = due
	if why != "" {
		s.log.InfoContext(ctx, "drafted no prompt proposal", "labeled_through", store.Stamp(due), "why", why)
		return next, nil
	}
	s.log.InfoContext(ctx, "queued a prompt proposal's draft", "proposal", p.ID, "prompt", p.Prompt, "prompt_version", p.PromptVersion,
		"labeled_after", p.LabeledAfter, "labeled_through", p.LabeledThrough, "misses", p.MissCount)
	return next, nil
}

// SchemaVersion is the version of schemas/prompt-proposals-list.schema.json
// that a list holds to.
const SchemaVersion = 1

// List is what GET /api/v1/gates/proposals answers:
// schemas/prompt-proposals-list.schema.json.
type List struct {
	SchemaVersion int                    `json:"schema_version"`
	Proposals     []store.PromptProposal `json:"proposals"`
}

// Routes is the route that lists every prompt proposal.
func Routes(st *store.Store, log *slog.Logger) server.Routes {
	if log == nil {
		log = slog.Default()
	}
	return func(r *server.Router) {
		r.Handle(server.RouteGateProposals, func(w http.ResponseWriter, req *http.Request) {
			proposals, err := st.PromptProposals(req.Context())
			if err != nil {
				log.ErrorContext(req.Context(), "read the prompt proposals", "error", err)
				server.Refuse(w, http.StatusInternalServerError, server.CodeInternal, "the server could not read the prompt proposals")
				return
			}
			server.JSON(w, http.StatusOK, List{SchemaVersion: SchemaVersion, Proposals: proposals})
		})
	}
}
