// Package comments is the model stage that reads what a video's top comments
// say about it, when a person asks on the video's review card. It runs once
// per item and never as a batch: the route queues one task, a worker reads up
// to the source's top comments through the source's own reader and asks one
// model call through modelstage's dispatch-once protocol, and the answer is
// kept as the output's answer. A model wrote that answer over strangers'
// comments, so it ships as outsider text alone, and the comments themselves
// are not kept.
package comments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

const (
	// Capability is the capability of every comments task.
	Capability = store.StageComments
	// Producer names the stage's outputs, whose version is the prompt's.
	Producer = "comments"
	// SchemaVersion is the route's answer, schemas/item-comments.schema.json.
	SchemaVersion = 1
)

// Thread is one top-level comment as a source answers it: its text, and how
// many likes and replies it drew.
type Thread struct {
	Text    string
	Likes   int64
	Replies int64
}

// Read is an item's top comments, most relevant first, and Disabled where
// the item's owner turned its comments off.
type Read struct {
	Disabled bool
	Threads  []Thread
}

// Reader reads the top comments of an item that holds no bytes, by its key.
type Reader interface {
	Comments(ctx context.Context, key string) (Read, error)
}

// Readers holds the reader opened for each media type, by the type.
type Readers map[string]Reader

// Source is how a source reads comments: the media type of the items it
// reads them for, and how a worker opens its reader on the source's client.
type Source struct {
	MediaType string
	Open      func(api string, client *http.Client) Reader
}

var (
	mu         sync.Mutex
	mediaTypes []string
)

// Register records that this build reads the comments of items of
// mediaType through one of its sources, so the route queues a read for one.
// sources.Register calls it.
func Register(mediaType string) {
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(mediaTypes, mediaType) {
		mediaTypes = append(mediaTypes, mediaType)
	}
}

// Reads says whether a registered source reads the comments of items of
// mediaType.
func Reads(mediaType string) bool {
	mu.Lock()
	defer mu.Unlock()
	return slices.Contains(mediaTypes, mediaType)
}

// Answer is the model's answer, as prompts/comments/answer.schema.json holds
// it.
type Answer struct {
	Summary   string   `json:"summary"`
	Praise    []string `json:"praise"`
	Criticism []string `json:"criticism"`
	Tone      string   `json:"tone"`
}

// ReadAnswer checks raw against the schema of the prompt in force and
// decodes it.
func ReadAnswer(raw json.RawMessage) (Answer, error) {
	if err := prompts.Comments.Check(raw); err != nil {
		return Answer{}, err
	}
	return DecodeAnswer(raw)
}

// DecodeAnswer decodes a stored answer without checking it against this
// build's schema, which its run's schema checked when it arrived.
func DecodeAnswer(raw json.RawMessage) (Answer, error) {
	var a Answer
	if err := json.Unmarshal(raw, &a); err != nil {
		return Answer{}, fmt.Errorf("decode the comments' answer: %w", err)
	}
	return a, nil
}

// Routes is the route that asks for the read of an item's comments.
func Routes(st *store.Store, log *slog.Logger) server.Routes {
	if log == nil {
		log = slog.Default()
	}
	return func(r *server.Router) {
		r.Handle(server.RouteItemComments, func(w http.ResponseWriter, req *http.Request) {
			request(w, req, st, log)
		})
	}
}

var errNotRead = errors.New("no source reads the comments")

// request queues the item's one comments task where it holds none, and
// answers how its read stands. An item with a done answer, an open task or a
// failed one queues nothing, so the model is asked once per item, and again
// only through `tasks retry`.
func request(w http.ResponseWriter, r *http.Request, st *store.Store, log *slog.Logger) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		server.Refuse(w, http.StatusBadRequest, server.CodeInvalidKey, "%v", err)
		return
	}
	var mediaType string
	var answer *server.ReviewComments
	err = st.Update(r.Context(), func(tx *store.Tx) error {
		records, err := tx.Records(r.Context(), key)
		if err != nil {
			return err
		}
		if mediaType = records.Item.MediaType; !Reads(mediaType) {
			return errNotRead
		}
		if answer = server.CommentsOf(records); answer != nil {
			return nil
		}
		task, err := tx.Enqueue(r.Context(), store.Task{ItemKey: &key, Stage: store.StageComments, Capability: Capability})
		if err != nil {
			return err
		}
		answer = &server.ReviewComments{State: server.CommentsReading, TaskID: &task.ID}
		log.Info("a read of an item's comments was asked for", "item", key, "task", task.ID)
		return nil
	})
	switch {
	case errors.Is(err, store.ErrNoItem):
		server.Refuse(w, http.StatusNotFound, server.CodeItemNotFound, "no item %s", key)
		return
	case errors.Is(err, errNotRead):
		server.Refuse(w, http.StatusUnprocessableEntity, server.CodeCommentsNotRead,
			"%s is a %s, and this build reads the comments of no item of that type", key, mediaType)
		return
	case err != nil:
		log.ErrorContext(r.Context(), "ask for the read of an item's comments", "error", err, "item", key)
		server.Refuse(w, http.StatusInternalServerError, server.CodeInternal, "the server could not ask for the item's comments")
		return
	}
	server.JSON(w, http.StatusOK, server.ItemCommentsAnswer{SchemaVersion: SchemaVersion, Key: key, Comments: *answer})
}
