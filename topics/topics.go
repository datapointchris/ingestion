// Package topics is the model stage that splits a memo, the owner's own
// words, into one item per topic. Its prompt and answer schema are
// prompts.Topics.
//
// The service queues a memo's topics task in the write that ends its scan
// done, once per memo. A connector's first fill holds every memo after the
// first to reach the stage, as at every model stage. A worker holding a model
// port runs the task under modelstage's dispatch-once protocol. Its prompt
// quotes each unit of the memo's reading parse, numbered from 1, with its
// span of the recording. The model answers each topic as a run of those
// units, with a title and an idea. The worker keeps an answer only where its
// topics hold every unit once, in order, and fails it otherwise, so `tasks
// retry` asks again.
//
// When the task ends done, serve writes the memo's topics rows and lands each
// topic as an item in the owner's zone, keyed <memo key>#<n>, with the memo as
// its parent and a sighting from each connector that sighted the memo. Each
// topic's parse is its run of the memo's units, and a scan task follows it.
// The side gate then splits the memo, and places each topic on its own.
package topics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	// Capability is the capability of every topics task.
	Capability = store.StageTopics
	// Producer names the topics stage's outputs, whose version is the
	// prompt's, and each topic's parse, whose version ParseVersion names.
	Producer = "topic-splitter"
)

// Answer is the model's answer, as prompts/topics/answer.schema.json holds
// it. ReadAnswer decodes it.
type Answer struct {
	Topics []AnswerTopic `json:"topics"`
}

// AnswerTopic is one topic of a memo: the run of units it holds, from 1, its
// title and its idea.
type AnswerTopic struct {
	FirstUnit int    `json:"first_unit"`
	LastUnit  int    `json:"last_unit"`
	Title     string `json:"title"`
	Idea      string `json:"idea"`
}

// ReadAnswer checks raw against the answer's schema and decodes it.
func ReadAnswer(raw json.RawMessage) (Answer, error) {
	if err := prompts.Topics.Check(raw); err != nil {
		return Answer{}, err
	}
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the topics answer: %w", err)
	}
	return a, nil
}

// Covers answers why a's topics do not hold each of a parse's units units
// once, in order, and nil where they do: the first starts at unit 1, each
// later one starts right after the one before it ends, and the last ends at
// the last unit.
func (a Answer) Covers(units int) error {
	if len(a.Topics) == 0 {
		return errors.New("the answer names no topic")
	}
	next := 1
	for i, t := range a.Topics {
		switch {
		case t.LastUnit < t.FirstUnit:
			return fmt.Errorf("topic %d ends at unit %d, before it starts at unit %d", i+1, t.LastUnit, t.FirstUnit)
		case t.FirstUnit > next:
			return fmt.Errorf("a gap: topic %d starts at unit %d, so units %d to %d belong to no topic", i+1, t.FirstUnit, next, t.FirstUnit-1)
		case t.FirstUnit < next:
			return fmt.Errorf("an overlap: topic %d starts at unit %d, inside the topic before it, which ends at unit %d", i+1, t.FirstUnit, next-1)
		}
		next = t.LastUnit + 1
	}
	switch {
	case next <= units:
		return fmt.Errorf("a gap: the topics end at unit %d, so units %d to %d belong to no topic", next-1, next, units)
	case next > units+1:
		return fmt.Errorf("the topics end at unit %d, past the parse's last unit, %d", next-1, units)
	}
	return nil
}

// ParseVersion is the producer version of each topic's parse, cut from parse
// by answer, each of which is part of it.
func ParseVersion(answer, parse store.Output) string {
	return answer.ProducerVersion + "/" + parse.Producer + "@" + parse.ProducerVersion
}

// QueueAfterScan queues a memo's topics task in the write that ends its scan
// done, where the scan read the memo's reading parse and the memo never held a
// topics task. A later scan queues none, so a replay of a model-free stage
// starts no model call.
func QueueAfterScan(ctx context.Context, tx *store.Tx, task store.Task) error {
	key := *task.ItemKey
	item, _, err := tx.Item(ctx, key)
	if err != nil || !item.SplitsIntoTopics() {
		return err
	}
	outputs, err := tx.Outputs(ctx, key)
	if err != nil || !store.ReadingParseScanned(outputs) {
		return err
	}
	if held, err := tx.HasTask(ctx, key, store.StageTopics); err != nil || held {
		return err
	}
	_, err = tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageTopics, Capability: Capability})
	return err
}

// AfterAnswer is the hook that runs in the write that ends a memo's topics
// task done. It writes the memo's topics and lands each as an item, where the
// memo holds none yet.
func AfterAnswer(log *slog.Logger) server.AfterStage {
	return func(ctx context.Context, tx *store.Tx, task store.Task) error {
		if task.ItemKey == nil {
			return nil
		}
		r, err := tx.Records(ctx, *task.ItemKey)
		if err != nil || !r.Item.SplitsIntoTopics() {
			return err
		}
		return split(ctx, tx, r, log)
	}
}

// split writes the topics of the memo's newest done answer, over the parse
// that the answer read, and lands each topic as an item in the owner's zone
// with the memo as its parent. Each topic takes a sighting from each
// connector that sighted the memo, with that sighting's tags, so the
// connector's person_sorts and tags reach it. Its parse is its run of the
// memo's units, numbered from 1 with their labels, and a scan task is queued
// for it. Where the memo's recording time is known, each topic's source time
// is when its first unit starts. split writes nothing where the answer is
// stale, as store.ReadsQuotedParse decides, or where its topics do not cover
// the parse, and logs the second.
func split(ctx context.Context, tx *store.Tx, r store.ItemRecords, log *slog.Logger) error {
	key := r.Item.Key
	newest := store.NewestDone(r.Outputs, store.StageTopics)
	if newest == nil || !store.ReadsQuotedParse(*newest, r.Outputs) {
		return nil
	}
	if held, err := tx.Topics(ctx, key); err != nil || len(held) > 0 {
		return err
	}
	answer, err := ReadAnswer(newest.Answer)
	if err != nil {
		return fmt.Errorf("read the topics answer on %s: %w", key, err)
	}
	pick := store.ParsePick{}
	if read := newest.Input(); read != nil {
		pick = store.ParsePick{Extractor: read.Extractor, Version: read.Version}
	}
	parse, err := tx.Parse(ctx, key, pick)
	if err != nil {
		return fmt.Errorf("read the parse that %s's topics split: %w", key, err)
	}
	if err := answer.Covers(len(parse.Units)); err != nil {
		log.WarnContext(ctx, "the topics answer does not hold each unit of the memo once, so the memo lands no topic", "key", key, "error", err)
		return nil
	}
	sightings, err := tx.Sightings(ctx, key)
	if err != nil {
		return err
	}
	rows := make([]store.Topic, len(answer.Topics))
	for i, t := range answer.Topics {
		child, err := itemkey.Child(key, i+1)
		if err != nil {
			return err
		}
		rows[i] = store.Topic{
			ItemKey: key, N: i + 1, ChildKey: child, FirstUnit: t.FirstUnit, LastUnit: t.LastUnit, Title: t.Title, Idea: t.Idea,
			Producer: newest.Producer, ProducerVersion: newest.ProducerVersion,
			InputProducer: parse.Output.Producer, InputVersion: parse.Output.ProducerVersion,
		}
	}
	if wrote, err := tx.PutTopics(ctx, rows); err != nil || !wrote {
		return err
	}
	lander, err := landing.New(landing.Config{})
	if err != nil {
		return err
	}
	recorded, starts := recordedAt(r.Item), startsOf(parse.Output)
	version := ParseVersion(*newest, parse.Output)
	for i, t := range answer.Topics {
		child := rows[i].ChildKey
		for _, s := range sightings {
			a := landing.Arrival{
				Connector: s.Connector, Zone: store.Owner, Key: child, MediaType: store.MediaTypeMemoTopic, Tags: s.Tags,
				Title: t.Title, Parent: key, SourceTime: startOf(recorded, starts, t.FirstUnit),
			}
			if _, err := lander.LandIn(ctx, tx, a); err != nil {
				return fmt.Errorf("land topic %d of %s: %w", i+1, key, err)
			}
		}
		units := make([]store.Unit, 0, t.LastUnit-t.FirstUnit+1)
		for _, u := range parse.Units[t.FirstUnit-1 : t.LastUnit] {
			units = append(units, store.Unit{N: len(units) + 1, Label: u.Label, Text: u.Text})
		}
		out := store.Output{ItemKey: child, Stage: store.StageParse, StageRun: store.StageRun{Producer: Producer, ProducerVersion: version, State: store.OutputDone}}
		if err := tx.PutOutputWithUnits(ctx, out, units); err != nil {
			return fmt.Errorf("write the parse of topic %d of %s: %w", i+1, key, err)
		}
		if _, err := tx.Enqueue(ctx, store.Task{ItemKey: &child, Stage: store.StageScan, Capability: scan.Capability}); err != nil {
			return err
		}
	}
	log.InfoContext(ctx, "split the memo into its topics", "key", key, "topics", len(rows), "units", len(parse.Units))
	return nil
}

// recordedAt is when the memo was recorded, its source time, and nil where
// the push named none.
func recordedAt(memo store.Item) *time.Time {
	if memo.SourceTS == nil {
		return nil
	}
	at, err := time.Parse(time.RFC3339Nano, *memo.SourceTS)
	if err != nil {
		return nil
	}
	return &at
}

// startsOf is where each unit of an audio-whisper parse starts in the
// recording, in milliseconds, and nil where the parse keeps none.
func startsOf(parse store.Output) []int64 {
	var a extract.AudioAnswer
	if len(parse.Answer) == 0 || json.Unmarshal(parse.Answer, &a) != nil {
		return nil
	}
	return a.StartsMs
}

// startOf is when the unit numbered first starts, recorded plus its start in
// the recording, and zero where either is unknown.
func startOf(recorded *time.Time, starts []int64, first int) time.Time {
	if recorded == nil || first < 1 || first > len(starts) {
		return time.Time{}
	}
	return recorded.Add(time.Duration(starts[first-1]) * time.Millisecond)
}
