package modelport_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/modelport"
	"github.com/datapointchris/ingestion/store"
)

// standInClaude writes the argv, stdin and environment it ran with under
// $HOME, then prints an init frame and a result frame as claude -p
// --output-format stream-json does.
const standInClaude = `#!/bin/sh
printf '%s\0' "$@" > "$HOME/argv"
cat > "$HOME/stdin"
env > "$HOME/env"
printf '%s\n' '{"type":"system","subtype":"init","model":"claude-haiku-4-5","tools":["StructuredOutput"]}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"","session_id":"session-1","structured_output":{"verdict":"pass"},"usage":{"input_tokens":9,"cache_creation_input_tokens":2900,"cache_read_input_tokens":0,"output_tokens":40},"modelUsage":{"claude-haiku-4-5":{"inputTokens":10,"cacheCreationInputTokens":3000,"cacheReadInputTokens":0,"outputTokens":50}}}'
`

// The claude-cli adapter runs claude -p with no tools, no settings, no
// session kept and the stage's schema less its $schema and $id, the prompt on
// stdin and none of the worker's secrets in its environment. It answers the
// result frame's structured output, its session as the run, the init frame's
// model and the tokens of every model the session used.
func TestTheClaudeCLIAdapterRunsClaudeWithNoToolsAndReadsItsAnswer(t *testing.T) {
	home, bin := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(standInClaude), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", home)
	t.Setenv("INGESTION_CLIENT_SECRET", "worker-secret")
	port, err := modelport.NewClaudeCLI("")
	if err != nil {
		t.Fatal(err)
	}
	r := modelport.Request{
		ItemKey: "reddit/t3_aa1", Stage: store.StageQuality, PromptVersion: "1-test", Prompt: "Judge this opening.",
		Schema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://example.com/a.json","type":"object"}`),
		Model:  "haiku", Effort: "medium",
	}
	progress, err := port.Start(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}

	argv, _ := os.ReadFile(filepath.Join(home, "argv"))
	got := strings.Split(strings.TrimSuffix(string(argv), "\x00"), "\x00")
	if want := modelport.ClaudeArgs(r, `{"type":"object"}`); !slices.Equal(got, want) {
		t.Errorf("claude ran with %q, want %q", got, want)
	}
	for _, flag := range []string{"--safe-mode", "--no-session-persistence"} {
		if !slices.Contains(got, flag) {
			t.Errorf("claude ran without %s", flag)
		}
	}
	if i := slices.Index(got, "--tools"); i < 0 || got[i+1] != "" {
		t.Errorf("claude ran with tools %q, want none", got)
	}
	if stdin, _ := os.ReadFile(filepath.Join(home, "stdin")); string(stdin) != r.Prompt {
		t.Errorf("claude read %q on stdin, want the prompt", stdin)
	}
	env, _ := os.ReadFile(filepath.Join(home, "env"))
	if bytes.Contains(env, []byte("worker-secret")) || !bytes.Contains(env, []byte("CLAUDE_CODE_ENABLE_TELEMETRY=0")) {
		t.Errorf("claude's environment holds the worker's secret or leaves telemetry on:\n%s", env)
	}

	a := progress.Answer
	if a == nil || a.State != modelport.Succeeded || string(a.Output) != `{"verdict":"pass"}` || a.Run != "session-1" || a.Model != "claude-haiku-4-5" ||
		a.Tokens == nil || *a.Tokens != (store.Tokens{UncachedInput: 10, CacheWrite: 3000, Output: 50}) {
		t.Errorf("the adapter answered %+v, want the structured output of session-1 on claude-haiku-4-5 with its model's tokens", a)
	}
}

// A call that the account's usage limit refused exits 1 with the cause only on
// stdout. The adapter answers it as a limit that resets, not a failed run, so
// the stage waits for the reset rather than failing the item.
func TestTheClaudeCLIAdapterAnswersAUsageLimitAsAWaitForItsReset(t *testing.T) {
	bin := t.TempDir()
	script := "#!/bin/sh\ncat > /dev/null\n" +
		`printf '%s\n' '{"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":1900000000,"rateLimitType":"five_hour"}}'` + "\n" +
		`printf '%s\n' '{"type":"result","subtype":"success","is_error":true,"result":"You have hit your limit","session_id":"s"}'` + "\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	port, err := modelport.NewClaudeCLI("")
	if err != nil {
		t.Fatal(err)
	}
	_, err = port.Start(t.Context(), modelport.Request{Stage: store.StageQuality, Schema: json.RawMessage(`{"type":"object"}`)})
	var limit *modelport.LimitError
	if !errors.As(err, &limit) || limit.Window != "five_hour" || !limit.ResetsAt.Equal(time.Unix(1900000000, 0)) || errors.Is(err, modelport.ErrRefused) {
		t.Errorf("the adapter answered %v, want the five_hour limit until its reset", err)
	}
}
