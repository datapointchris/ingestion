package extract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// ProgramDir is where the worker image installs each extractor program.
const ProgramDir = "/usr/local/libexec/ingestion/extractors"

const (
	// stderrKept bounds the stderr that a failed run keeps, from its end.
	stderrKept = 64 << 10
	// lineMax bounds one unit's line on stdout.
	lineMax = 64 << 20
	// exitWait is how long a run's pipes stay open once it exits or is
	// killed, so a child process that holds them never hangs the worker.
	exitWait = 10 * time.Second
)

// Program is an extractor that runs the program at path with args, such as
// one whose library is Python. The program reads the item's bytes on stdin,
// and writes one JSON line per unit on stdout, in order:
//
//	{"label": "Chapter 1", "text": "It was a bright cold day…"}
//
// label may be null or left out, and any other field is refused. A run that
// exits non-zero fails its task, with the end of its stderr kept.
func Program(name, version, mediaType, path string, args ...string) Extractor {
	return Extractor{
		Name: name, Version: version, MediaType: mediaType, Program: path,
		Extract: func(ctx context.Context, body io.Reader) ([]Unit, error) { return runProgram(ctx, path, args, body) },
	}
}

// ProgramError is a program that exited non-zero, with the end of its stderr.
type ProgramError struct {
	Program  string
	ExitCode int
	Stderr   string
}

func (e *ProgramError) Error() string {
	return fmt.Sprintf("%s exited %d: %s", filepath.Base(e.Program), e.ExitCode, e.Stderr)
}

// runProgram is the one place ingestion starts a process. The program runs in
// an empty directory of its own, with an environment of PATH, HOME, TMPDIR
// and LANG alone. So no credential of the worker, such as its client secret,
// reaches a program that reads outsider bytes.
func runProgram(ctx context.Context, path string, args []string, body io.Reader) ([]Unit, error) {
	dir, err := os.MkdirTemp("", "ingestion-extract-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir, cmd.Stdin, cmd.WaitDelay = dir, body, exitWait
	cmd.Env = []string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + dir, "TMPDIR=" + dir, "LANG=C.UTF-8"}
	stderr := &tail{max: stderrKept}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", path, err)
	}
	units, readErr := readUnits(stdout)
	if readErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	var exit *exec.ExitError
	switch {
	case readErr != nil:
		return nil, fmt.Errorf("%s wrote %w", filepath.Base(path), readErr)
	case errors.As(waitErr, &exit):
		return nil, &ProgramError{Program: path, ExitCode: exit.ExitCode(), Stderr: strings.TrimSpace(stderr.String())}
	case waitErr != nil:
		return nil, fmt.Errorf("run %s: %w", path, waitErr)
	}
	return units, nil
}

// readUnits reads one unit from each line of r, skipping blank lines.
func readUnits(r io.Reader) ([]Unit, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), lineMax)
	var units []Unit
	for line := 1; scanner.Scan(); line++ {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		var u struct {
			Label *string `json:"label"`
			Text  *string `json:"text"`
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&u); err != nil {
			return nil, fmt.Errorf("line %d, which is not a unit: %w", line, err)
		}
		if u.Text == nil {
			return nil, fmt.Errorf("line %d, which names no text", line)
		}
		units = append(units, Unit{Label: u.Label, Text: *u.Text})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("a stdout it could not read: %w", err)
	}
	return units, nil
}

// tail keeps the last max bytes written to it.
type tail struct {
	max int
	buf []byte
}

func (t *tail) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - t.max; over > 0 {
		t.buf = t.buf[over:]
	}
	return len(p), nil
}

func (t *tail) String() string { return string(t.buf) }
