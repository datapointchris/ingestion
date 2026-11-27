// Package lens holds each lens's context: the owner's current work, which the
// note stage drafts beside and the model read reads beside, so both bear on
// what the owner works on now. A context reader is registered once under its
// name, as a deliverer is, and config opens one for a lens from its
// [lens.<lens>.context] table. With no reader opened for a lens, each stage
// under it runs without a context.
package lens

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Context is the owner's current work, as a context reader answers it.
type Context struct {
	// Focus is where the owner's work leans for a while, and nil where none
	// is in force.
	Focus       *Focus
	Initiatives []Initiative
}

// Focus is the statement of where the owner's work leans, the last day it
// holds, and the share of the work each pursuit it names takes.
type Focus struct {
	Statement string
	Until     string
	Shares    []Share
}

// Share is one pursuit's share of the work, from 0 to 1.
type Share struct {
	Pursuit string
	Share   float64
}

// Initiative is one body of the owner's work under way.
type Initiative struct {
	Number   int
	Name     string
	Summary  string
	Pursuits []string
}

const (
	// mostInitiatives and summaryRunes bound what Text quotes, so a long
	// record keeps the prompt to a size that a small model reads whole.
	mostInitiatives = 60
	summaryRunes    = 300
)

// Text is the context as a prompt quotes it, and "" where it holds nothing.
func (c Context) Text() string {
	var b strings.Builder
	if f := c.Focus; f != nil {
		fmt.Fprintf(&b, "Focus until %s: %s\n", f.Until, oneLine(f.Statement))
		for _, s := range f.Shares {
			fmt.Fprintf(&b, "- %s, %.0f%% of the work\n", oneLine(s.Pursuit), s.Share*100)
		}
	}
	if len(c.Initiatives) > 0 {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("Work under way:\n")
		for _, i := range c.Initiatives[:min(len(c.Initiatives), mostInitiatives)] {
			fmt.Fprintf(&b, "- %d %s: %s", i.Number, oneLine(i.Name), clip(oneLine(i.Summary), summaryRunes))
			if len(i.Pursuits) > 0 {
				fmt.Fprintf(&b, " (in %s)", strings.Join(i.Pursuits, ", "))
			}
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func clip(s string, runes int) string {
	if utf8.RuneCountInString(s) <= runes {
		return s
	}
	return string([]rune(s)[:runes]) + "…"
}

// Reader reads a lens's context. Beside an error it may answer an older
// context it read, which Text then quotes.
type Reader interface {
	Read(ctx context.Context) (Context, error)
}

// Readers holds the reader opened for each lens, by the lens's name.
type Readers map[string]Reader

// readFor bounds one read of a lens's context, so a source that hangs holds
// a stage's task no longer than this.
const readFor = 10 * time.Second

// Text is the context of lens as a prompt quotes it, and "" where no reader
// is opened for lens. A read that fails costs the stage only the context: Text
// logs why on log and answers what the reader answered beside the failure,
// the last context it read or none, so the stage runs on.
func (r Readers) Text(ctx context.Context, lens string, log *slog.Logger) string {
	reader, ok := r[lens]
	if !ok {
		return ""
	}
	bounded, cancel := context.WithTimeout(ctx, readFor)
	defer cancel()
	c, err := reader.Read(bounded)
	if err != nil {
		log.WarnContext(ctx, "the lens's context could not be read, so the stage runs beside the last context read, or none",
			"lens", lens, "error", err, "last_read", c.Text() != "")
	}
	return c.Text()
}

// Registration is one context reader as this build carries it: the scopes
// its service client needs, and how a worker opens it on the reader's API.
type Registration struct {
	Scopes []string
	Open   func(api string, client *http.Client) Reader
}

var registered = map[string]Registration{}

// Register records that this build carries the context reader called name.
// Registering a name twice panics, as a duplicate is a build error.
func Register(name string, r Registration) {
	if _, ok := registered[name]; ok {
		panic("lens: context reader " + name + " is registered twice")
	}
	registered[name] = r
}

// Lookup is the context reader registered as name.
func Lookup(name string) (Registration, bool) {
	r, ok := registered[name]
	return r, ok
}

// Names is each registered context reader's name, in order.
func Names() []string { return slices.Sorted(maps.Keys(registered)) }

// cacheFor is how long a read context answers again, so a batch of items
// drafted together reads the owner's work once. retryAfter is how long a
// failed read answers its failure again before the next read is tried, so a
// source that is down is not asked once per task.
const (
	cacheFor   = 5 * time.Minute
	retryAfter = time.Minute
)

// Cached is reader answering each read context again for cacheFor. A failed
// read answers the last context read beside its error, and answers both again
// for retryAfter.
func Cached(reader Reader) Reader { return &cached{reader: reader} }

type cached struct {
	reader   Reader
	mu       sync.Mutex
	held     Context
	readAt   time.Time
	failure  error
	failedAt time.Time
}

func (c *cached) Read(ctx context.Context) (Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !c.readAt.IsZero() && now.Sub(c.readAt) < cacheFor {
		return c.held, nil
	}
	if c.failure != nil && now.Sub(c.failedAt) < retryAfter {
		return c.held, c.failure
	}
	read, err := c.reader.Read(ctx)
	if err != nil {
		c.failure, c.failedAt = err, now
		return c.held, err
	}
	c.held, c.readAt, c.failure = read, now, nil
	return read, nil
}
