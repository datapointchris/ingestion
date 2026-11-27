package lens

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

// downReader fails every read, as a context source does while it is down,
// beside the context it answers with the failure.
type downReader struct {
	beside Context
	reads  int
}

func (d *downReader) Read(context.Context) (Context, error) {
	d.reads++
	return d.beside, errors.New("503 Service Unavailable")
}

// While a context source is down, a stage reads the last context the reader
// answered beside its failure, or none, and the cache asks the source once
// rather than once per task.
func TestAContextSourceThatIsDownCostsTheStageOnlyItsContext(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	down := &downReader{}
	never := Readers{"work": Cached(down)}
	for range 3 {
		if text := never.Text(t.Context(), "work", log); text != "" {
			t.Errorf("a reader that never read answered %q, want no context", text)
		}
	}
	if down.reads != 1 {
		t.Errorf("three tasks asked the source %d times while it was down, want once", down.reads)
	}

	last := Context{Focus: &Focus{Statement: "The greenhouse vents.", Until: "2030-04-12"}}
	stale := Readers{"work": &downReader{beside: last}}
	if text := stale.Text(t.Context(), "work", log); text != last.Text() {
		t.Errorf("a failed read answered %q, want the last context read, %q", text, last.Text())
	}
}
