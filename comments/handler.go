package comments

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// DefaultModel and DefaultEffort are what the read's run is asked for
	// where config names neither. It reads short comments and answers a short
	// schema, so the smallest model does it.
	DefaultModel  = "haiku"
	DefaultEffort = "medium"
	// clipRunes bounds each comment that the prompt quotes, so the prompt stays
	// a size that a small model reads whole however many comments ypl answers.
	clipRunes = 400
)

// NewHandler is the worker handler that reads each task's item's comments
// through the reader that readers opens for its media type, and asks port
// what they say under modelstage's dispatch-once protocol. A worker opens it
// only where it holds a model port and a reader.
func NewHandler(port modelport.Port, readers Readers, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Comments, nil },
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			return quoted(ctx, readers, task)
		},
		Keep: keep,
	}, settings, log)
}

// promptData is what the prompt quotes: the item's title and URL, and each
// comment numbered from 1.
type promptData struct {
	Title, URL string
	Comments   []quotedThread
	Clipped    bool
	ClipRunes  int
}

type quotedThread struct {
	N              int
	Text           string
	Likes, Replies int64
}

// quoted reads the task's item's comments and quotes them. An item whose
// comments are off or empty fails its task, marked modelport.ErrRefused, so
// no model is asked about nothing, and the card says why. A failed read of
// the source tries again after a backoff.
func quoted(ctx context.Context, readers Readers, task worker.Task) (promptData, error) {
	reader, ok := readers[task.MediaType]
	if !ok {
		return promptData{}, fmt.Errorf("%w: this worker reads the comments of no %s", modelport.ErrRefused, task.MediaType)
	}
	read, err := reader.Comments(ctx, deref(task.ItemKey))
	if err != nil {
		return promptData{}, fmt.Errorf("read the item's comments: %w", err)
	}
	switch {
	case read.Disabled:
		return promptData{}, fmt.Errorf("%w: the item's owner turned its comments off", modelport.ErrRefused)
	case len(read.Threads) == 0:
		return promptData{}, fmt.Errorf("%w: the item holds no comments yet", modelport.ErrRefused)
	}
	data := promptData{Title: deref(task.Title), URL: deref(task.URL), ClipRunes: clipRunes}
	for i, t := range read.Threads {
		text := []rune(strings.Join(strings.Fields(t.Text), " "))
		if len(text) > clipRunes {
			text, data.Clipped = text[:clipRunes], true
		}
		data.Comments = append(data.Comments, quotedThread{N: i + 1, Text: string(text), Likes: t.Likes, Replies: t.Replies})
	}
	return data, nil
}

// keep checks the answer against its schema, and logs its tone and counts
// alone, because a model wrote every other field over strangers' text.
func keep(_ context.Context, _ worker.Task, _ prompts.Prompt, raw json.RawMessage, _ *server.ReportOutput) ([]any, error) {
	a, err := ReadAnswer(raw)
	if err != nil {
		return nil, err
	}
	return []any{"tone", a.Tone, "praise", len(a.Praise), "criticism", len(a.Criticism)}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
