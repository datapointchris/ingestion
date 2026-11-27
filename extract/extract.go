// Package extract is the parse stage: one extractor per media type, each
// turning an item's bytes into units, such as a thread's post and comments or
// a book's pages. A registry keyed by media type picks the extractor, so an
// item parses the same whatever source sent it.
//
// An extractor registers by joining builtin. One written in Go sets Extract.
// One whose library is not Go is a Program from the worker image, run through
// isolatedprocess. One whose item holds no bytes, such as a video, reads the
// item at its source, and the source registers it through Register. Each
// output is keyed by item, extractor and version, so a changed extractor
// takes a new version and its outputs write beside the old. A media type
// holds one primary extractor, which landing queues, and may hold secondary
// ones, whose parse another stage's write queues and writes beside the
// primary's.
package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// builtin is every extractor that ingestion holds of its own.
var builtin = []Extractor{ThreadText, PDFText, PDFDocling}

// registered is every extractor that a source registered.
var registered []Extractor

// Register adds extractors to the builtin registry, as a source registers the
// ones that read its items, from its own init.
func Register(extractors ...Extractor) { registered = append(registered, extractors...) }

// Named is the builtin or registered extractor called name, and false where
// none is.
func Named(name string) (Extractor, bool) {
	for _, e := range append(slices.Clone(builtin), registered...) {
		if e.Name == name {
			return e, true
		}
	}
	return Extractor{}, false
}

// SlotGroupHeavy is the slot group of each parse whose run holds gigabytes,
// such as Docling's and Whisper's.
const SlotGroupHeavy = "heavy"

// SlotGroups holds each slot group's default limit, which a
// [worker.slot_limits] key naming the group overrides. heavy runs one parse
// at a time, because a worker's memory holds one.
var SlotGroups = map[string]int{SlotGroupHeavy: 1}

// Item is what an extractor that ReadsURL reads: the item's key, its URL and
// its media type.
type Item struct {
	Key       string
	URL       *string
	MediaType string
}

// Reader reads one item at its source, as an extractor that ReadsURL does.
type Reader func(ctx context.Context, item Item) (Read, error)

// Read is what a Reader found.
type Read struct {
	Units []Unit
	// Deferred says the source holds no text of its own for the item, so its
	// parse hands the item to the extractor that DefersTo names, such as a
	// video with no caption track.
	Deferred bool
	// Answer is kept as the output's answer, such as the caption track that
	// a read used.
	Answer json.RawMessage
}

// RetryError is a read that may succeed if made again, and its parse queues
// again after After. A bounded one fails once its task's claims reach
// ReadAttempts, such as when the source gave no answer. Where the source
// asked the read to wait, as while it is busy, the parse waits: it spends no
// claim, and fails once it has waited ReadWaitCeiling.
type RetryError struct {
	Err     error
	After   time.Duration
	Bounded bool
}

func (e *RetryError) Error() string { return e.Err.Error() }
func (e *RetryError) Unwrap() error { return e.Err }

// ReadAttempts is how many claims a parse makes of a read that fails boundedly
// before it fails. A claim that ended waiting is not among them.
const ReadAttempts = 4

// ReadWaitCeiling is how long a parse waits on its source in all before it
// fails, counted from the source's first answer to wait. It outlasts the
// longest pause that ypl declares, a day, so only a source that never frees
// fails a parse this way.
const ReadWaitCeiling = 48 * time.Hour

// Extractor turns the bytes of one media type into a parse's units.
type Extractor struct {
	// Name names the extractor in each output it writes, such as thread-text.
	Name string
	// Version is written beside each output, so a changed extractor takes a
	// new one.
	Version   string
	MediaType string
	// Program is the absolute path of the extractor's program, and empty for
	// one written in Go. A worker runs it only where the program is installed.
	Program string
	// Extract reads body and answers its units in order.
	Extract func(ctx context.Context, body io.Reader) ([]Unit, error)
	// Timeout bounds one run of Extract, and none where it is zero. A run
	// past it fails its task, so a program that hangs on one item's bytes
	// frees its worker slot and the item waits in review.
	Timeout time.Duration
	// Positional keeps a unit with no text in its place, so unit n is the
	// artifact's own part n, such as a book's PDF page n. Otherwise such a
	// unit is dropped and the rest are numbered again.
	Positional bool
	// Secondary marks an extractor that landing never queues and a parse
	// replay never picks, because another stage's write queues its parse, as
	// the structure's queues PDFDocling's.
	Secondary bool
	// SlotLimit caps the extractor's parses that one worker runs at once,
	// where [worker.slot_limits] names no limit for it. Zero leaves them
	// uncapped. An extractor in a slot group sets none.
	SlotLimit int
	// SlotGroup names the slot group whose limit the extractor's parses share
	// with every other extractor in it, such as SlotGroupHeavy. SlotGroups
	// holds the group's limit.
	SlotGroup string
	// ReadsURL marks an extractor whose item holds no bytes, so it reads the
	// item at its source through the reader that Open opens on that source's
	// client. A source registers it, and a worker runs its parses only where
	// it opened that source. It sets no Extract.
	ReadsURL bool
	// Open opens the reader of an extractor that ReadsURL, which reaches its
	// source at api through client, carrying the source client's token.
	Open func(api string, client *http.Client) Reader
	// DefersTo names the secondary extractor whose parse the write that ends a
	// deferred parse of this one queues, such as Whisper for a video whose
	// captions hold no track.
	DefersTo string
	// LinkKind is the kind of item key, as itemkey names it, whose link this
	// extractor reads at its source, such as youtube. A pushed link of that
	// kind lands as the extractor's media type, under the URL that LinkURL
	// makes. Only a primary extractor that ReadsURL sets it.
	LinkKind string
	// LinkURL is the URL that the source names an item under, from the id in
	// the item's key, as its poll lands it. An extractor with a LinkKind sets
	// it, so an item's URL is the same whichever connector landed it first.
	LinkURL func(id string) string
}

// Unit is one part of a parse, as an extractor writes it. The parse numbers
// its units from 1 in their order.
type Unit struct {
	// Label is the unit's own heading, such as a comment's author or a
	// chapter's title.
	Label *string
	Text  string
}

// Capability is the capability of e's parse tasks, so a worker claims only
// the tasks of extractors it runs.
func Capability(e Extractor) string { return store.StageParse + ":" + e.Name }

// Registry picks the primary extractor for an item by its media type, and
// for a pushed link by its key's kind. It holds every extractor that a worker
// runs.
type Registry struct {
	byType map[string]Extractor
	byLink map[string]Extractor
	all    []Extractor
}

// NewRegistry registers extractors. It refuses one with no name, version,
// media type or way to run, two of one name, two primary ones of one media
// type or one link kind, and one that defers to an extractor it does not hold
// as secondary. It refuses a link kind on an extractor that is secondary or
// reads bytes. It refuses a slot group that SlotGroups does not hold, and a
// SlotLimit beside a slot group, whose limit is the group's.
func NewRegistry(extractors ...Extractor) (*Registry, error) {
	r := &Registry{byType: map[string]Extractor{}, byLink: map[string]Extractor{}}
	names := map[string]bool{}
	for _, e := range extractors {
		switch {
		case e.Name == "" || e.Version == "" || e.MediaType == "":
			return nil, fmt.Errorf("an extractor names its name, version and media type, not %q, %q and %q", e.Name, e.Version, e.MediaType)
		case e.ReadsURL && (e.Open == nil || e.Extract != nil):
			return nil, fmt.Errorf("extractor %s reads its item at its source, so it sets Open and no Extract", e.Name)
		case !e.ReadsURL && e.Extract == nil:
			return nil, fmt.Errorf("extractor %s sets no Extract", e.Name)
		case names[e.Name]:
			return nil, fmt.Errorf("two extractors are named %s", e.Name)
		case e.SlotGroup != "" && SlotGroups[e.SlotGroup] < 1:
			return nil, fmt.Errorf("extractor %s runs in slot group %s, which SlotGroups gives no limit", e.Name, e.SlotGroup)
		case e.SlotGroup != "" && e.SlotLimit != 0:
			return nil, fmt.Errorf("extractor %s runs in slot group %s, so it sets no SlotLimit of its own", e.Name, e.SlotGroup)
		case e.LinkKind != "" && (e.Secondary || !e.ReadsURL || e.LinkURL == nil):
			return nil, fmt.Errorf("extractor %s reads %s links, so it is primary, reads its item at its source and sets LinkURL", e.Name, e.LinkKind)
		}
		names[e.Name] = true
		r.all = append(r.all, e)
		if e.Secondary {
			continue
		}
		if held, taken := r.byType[e.MediaType]; taken {
			return nil, fmt.Errorf("extractors %s and %s are both primary for %s", held.Name, e.Name, e.MediaType)
		}
		r.byType[e.MediaType] = e
		if e.LinkKind == "" {
			continue
		}
		if held, taken := r.byLink[e.LinkKind]; taken {
			return nil, fmt.Errorf("extractors %s and %s both read %s links", held.Name, e.Name, e.LinkKind)
		}
		r.byLink[e.LinkKind] = e
	}
	for _, e := range r.all {
		if e.DefersTo != "" && !slices.ContainsFunc(r.all, func(o Extractor) bool { return o.Name == e.DefersTo && o.Secondary }) {
			return nil, fmt.Errorf("extractor %s defers to %s, which the registry holds as no secondary extractor", e.Name, e.DefersTo)
		}
	}
	return r, nil
}

// Builtin is the registry of every extractor that ingestion holds, and every
// one that a source registered.
func Builtin() *Registry {
	r, err := NewRegistry(append(slices.Clone(builtin), registered...)...)
	if err != nil {
		panic("extract: " + err.Error())
	}
	return r
}

// For is the primary extractor that reads mediaType, and false where none
// does.
func (r *Registry) For(mediaType string) (Extractor, bool) {
	e, ok := r.byType[mediaType]
	return e, ok
}

// LinkArrival is the media type and URL that a pushed link keyed key lands
// with, as the extractor that reads the key's kind names them, and false
// where no extractor reads a link of that kind. A child's key names no link.
func (r *Registry) LinkArrival(key string) (mediaType, link string, ok bool) {
	kind, id, ok := strings.Cut(key, "/")
	if !ok || strings.Contains(key, "#") {
		return "", "", false
	}
	e, ok := r.byLink[kind]
	if !ok {
		return "", "", false
	}
	return e.MediaType, e.LinkURL(id), true
}

// LinkKinds names, in order, each kind of key whose link an extractor reads.
func (r *Registry) LinkKinds() []string { return slices.Sorted(maps.Keys(r.byLink)) }

// QueueParse queues a parse task for item where an extractor reads its media
// type. Landing calls it in the write that adds the item, so every item lands
// with its parse queued.
func (r *Registry) QueueParse(ctx context.Context, tx *store.Tx, item store.Item) error {
	e, ok := r.For(item.MediaType)
	if !ok {
		return nil
	}
	_, err := tx.Enqueue(ctx, store.Task{ItemKey: &item.Key, Stage: store.StageParse, Capability: Capability(e)})
	return err
}

// Replayable is the parse stage as a replay runs it again: a task for each
// item whose media type an extractor reads.
func (r *Registry) Replayable() server.Replayable {
	return server.Replayable{Capability: func(item store.Item) (string, bool) {
		e, ok := r.For(item.MediaType)
		return Capability(e), ok
	}}
}
