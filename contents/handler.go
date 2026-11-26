package contents

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/modelstage"
	"github.com/datapointchris/ingestion/prompts"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// DefaultModel and DefaultEffort are what the stage's run is asked for
	// where config names neither. Sonnet read two outline-less books'
	// contents pages and placed 46 of their 48 chapters. Medium runs on a
	// dispatch service that refuses a lower effort.
	DefaultModel  = "sonnet"
	DefaultEffort = "medium"
	// frontPages is how many PDF pages at a book's front are read for its
	// contents pages, and quoted where none looks like one.
	frontPages = 30
	// numberedShare and numberedLeast are the least share and count of a
	// page's lines ending in a number that make it look like a contents
	// page.
	numberedShare = 0.3
	numberedLeast = 3
)

// endsInNumber is a line that ends in a digit, as a contents entry's page
// number does.
var endsInNumber = regexp.MustCompile(`\d\s*$`)

// NewHandler is the worker handler that runs the stage through port under
// modelstage's dispatch-once protocol, reading each task's parse through
// client.
func NewHandler(port modelport.Port, client *worker.Client, settings modelstage.Settings, log *slog.Logger) worker.Handler {
	settings.Model, settings.Effort = cmp.Or(settings.Model, DefaultModel), cmp.Or(settings.Effort, DefaultEffort)
	return modelstage.NewHandler(port, modelstage.Stage{
		Name: Capability, Producer: Producer,
		Prompt: func(worker.Task) (prompts.Prompt, error) { return prompts.Contents, nil },
		Data: func(ctx context.Context, task worker.Task) (any, error) {
			parse, err := client.TaskParse(ctx, task.ID, task.LeaseToken)
			if err != nil {
				return nil, fmt.Errorf("read the parse: %w", err)
			}
			return Quote(deref(task.Title), parse.Units), nil
		},
		Keep: func(_ context.Context, _ prompts.Prompt, raw json.RawMessage, _ *server.ReportOutput) ([]any, error) {
			answer, err := ReadAnswer(raw)
			if err != nil {
				return nil, err
			}
			return []any{"entries", len(answer.Entries)}, nil
		},
	}, settings, log)
}

// PromptData is what the prompt quotes of a book.
type PromptData struct {
	Title string
	Pages int
	// ContentsPages says the quoted pages look like contents pages, and
	// false that they are the book's first Shown pages.
	ContentsPages bool
	Shown         int
	Quoted        []QuotedPage
}

// QuotedPage is one page that the prompt quotes, by its PDF page.
type QuotedPage struct {
	Page int
	Text string
}

// Quote is what the prompt quotes of the book called title whose positional
// parse is units: its contents pages, or its first frontPages pages where
// none looks like one.
func Quote(title string, units []store.Unit) PromptData {
	data := PromptData{Title: title, Pages: len(units)}
	pages := ContentsPages(units)
	data.ContentsPages = len(pages) > 0
	if !data.ContentsPages {
		for n := 1; n <= min(frontPages, len(units)); n++ {
			pages = append(pages, n)
		}
	}
	for _, n := range pages {
		data.Quoted = append(data.Quoted, QuotedPage{Page: n, Text: units[n-1].Text})
	}
	data.Shown = len(pages)
	return data
}

// ContentsPages are the PDF pages among the first frontPages of units that
// look like contents pages: at least numberedLeast of their lines, and
// numberedShare of them, end in a number.
func ContentsPages(units []store.Unit) []int {
	var pages []int
	for i := range min(frontPages, len(units)) {
		lines, numbered := 0, 0
		for line := range strings.SplitSeq(units[i].Text, "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			lines++
			if endsInNumber.MatchString(line) {
				numbered++
			}
		}
		if numbered >= numberedLeast && float64(numbered) >= numberedShare*float64(lines) {
			pages = append(pages, i+1)
		}
	}
	return pages
}
