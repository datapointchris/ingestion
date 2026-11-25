// Package modelport is the one port that carries every model call. A stage
// hands it a prompt, the JSON Schema that holds the answer and the prompt's
// version, and reads back the structured answer.
//
// Two adapters carry a call. queue posts it to an HTTP dispatch service and
// follows the dispatch until its run answers, so the call waits on that
// service's queue and budget. claude-cli runs claude -p on this host, with no
// tools, none of the host's settings and no permission prompts. The port
// refuses with neither named, so a model stage never reaches a model some
// other way. This is the ports and adapters pattern: the stage depends on the
// port, and config chooses the adapter.
package modelport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/datapointchris/ingestion/store"
)

// Adapter names, as config's [model] adapter spells them.
const (
	AdapterQueue     = "queue"
	AdapterClaudeCLI = "claude-cli"
)

// Adapters lists every adapter name.
var Adapters = []string{AdapterQueue, AdapterClaudeCLI}

// ErrNoAdapter is a config that names no adapter, so no model stage runs.
var ErrNoAdapter = errors.New(`[model] adapter names no adapter, so no model stage runs: set it to "queue" or "claude-cli"`)

// ErrRefused marks a call that sending again would meet the same way, such as
// one the dispatch service refuses. Any other error may pass on a later try.
var ErrRefused = errors.New("refused")

// Request is one model call.
type Request struct {
	// ItemKey, Stage and PromptVersion name the call, so a call sent again
	// for one item, stage and prompt is one dispatch.
	ItemKey       string
	Stage         string
	PromptVersion string
	// Replaces is the dispatch whose run failed, where this call sends it
	// again, so the retry is a dispatch of its own.
	Replaces string
	Prompt   string
	// Schema holds the answer, a JSON Schema whose root is an object.
	Schema json.RawMessage
	// Model and Effort are what the run is asked to run on. Empty leaves
	// each to the adapter's own default.
	Model  string
	Effort string
}

// IdempotencyKey names r across every retry, by its item, stage, prompt
// version and the failed dispatch it replaces.
func (r Request) IdempotencyKey() string {
	sum := sha256.Sum256([]byte(r.ItemKey + "\x00" + r.Stage + "\x00" + r.PromptVersion + "\x00" + r.Replaces))
	return "ingestion-" + r.Stage + "-" + hex.EncodeToString(sum[:16])
}

// How a run ended.
const (
	Succeeded = "succeeded"
	Failed    = "failed"
)

// NotRun is the failure of a dispatch that ended with no run, as one the
// dispatch service dropped or refused.
const NotRun = "not_run"

// Answer is a finished run.
type Answer struct {
	// Run names the run that answered, which a decision by the model cites.
	Run   string
	State string
	// Output is the structured answer where the run succeeded.
	Output json.RawMessage
	// Failure is a code where the run failed, such as timed_out, and Detail
	// the run's own words, which may quote the prompt.
	Failure string
	Detail  string
	// Model and Effort are what the run ran on, where it says.
	Model  string
	Effort string
	Tokens *store.Tokens
}

// Progress is where one call stands.
type Progress struct {
	// Dispatch names the call for Follow, and is empty where the adapter ran
	// it at once.
	Dispatch string
	// Answer is the finished run, and nil while the run has not ended.
	Answer *Answer
	// Waiting is why the run has not started, in the dispatch service's
	// words, where it gives any.
	Waiting string
}

// Port carries model calls.
type Port interface {
	// Adapter is the adapter's name.
	Adapter() string
	// Start sends r and answers where it stands. An adapter that runs the
	// call itself answers it finished.
	Start(ctx context.Context, r Request) (Progress, error)
	// Follow reads where a call that Start dispatched stands.
	Follow(ctx context.Context, dispatch string) (Progress, error)
}

// refused is an error marked ErrRefused.
func refused(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrRefused, fmt.Sprintf(format, args...))
}
