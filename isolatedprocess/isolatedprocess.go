// Package isolatedprocess starts every program that reads outsider bytes.
// Each such program runs in an empty directory of its own, with an
// environment of PATH, HOME, TMPDIR and LANG alone, and inherits no
// credential of the worker. A program that needs a login of its own, as
// claude does, names the variables that carry it, and inherits those alone.
// A worker that runs one has called worker.MarkUndumpable, so the program
// cannot read the worker's own environment through /proc either.
package isolatedprocess

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// stderrKept bounds the stderr that a run keeps, from its end.
	stderrKept = 64 << 10
	// exitWait is how long a run's pipes stay open once it exits or is
	// killed, so a child process that holds them never hangs the worker.
	exitWait = 10 * time.Second
)

// Command is one run of a program.
type Command struct {
	Path string
	Args []string
	// Files are written into the run's directory before it starts, each
	// under its name, so Args can name them by a relative path.
	Files map[string]string
	Stdin io.Reader
	// Stdout reads the program's stdout while it runs. A read that fails
	// kills the run, and Run answers the read's error. Nil discards stdout.
	Stdout func(io.Reader) error
	// Inherit names the variables of this process's environment that the
	// run keeps, each a name or a prefix ending in *, such as ANTHROPIC_*.
	// Each replaces the run's own value, so inheriting HOME runs the program
	// with this process's home rather than the run's directory.
	Inherit []string
	// Env holds entries set last, each NAME=value, over every other.
	Env []string
}

// ExitError is a program that exited non-zero, with the end of its stderr.
type ExitError struct {
	Program  string
	ExitCode int
	Stderr   string
}

func (e *ExitError) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("%s exited %d and wrote nothing to stderr", filepath.Base(e.Program), e.ExitCode)
	}
	return fmt.Sprintf("%s exited %d: %s", filepath.Base(e.Program), e.ExitCode, e.Stderr)
}

// writeFile writes body to a new file at path without copying it.
func writeFile(path, body string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(body)
	return errors.Join(err, f.Close())
}

// Run runs c until it exits or ctx ends, and answers the end of its stderr.
// A program that exits non-zero answers an *ExitError.
func Run(ctx context.Context, c Command) (string, error) {
	dir, err := os.MkdirTemp("", "ingestion-run-")
	if err != nil {
		return "", err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	for name, body := range c.Files {
		if name != filepath.Base(name) || name == "." || name == ".." {
			return "", fmt.Errorf("a run's file is one filename, not %q", name)
		}
		if err := writeFile(filepath.Join(dir, name), body); err != nil {
			return "", err
		}
	}
	cmd := exec.CommandContext(ctx, c.Path, c.Args...)
	cmd.Dir, cmd.Stdin, cmd.WaitDelay = dir, c.Stdin, exitWait
	cmd.Env = append(append([]string{"PATH=/usr/local/bin:/usr/bin:/bin", "HOME=" + dir, "TMPDIR=" + dir, "LANG=C.UTF-8"},
		inherited(os.Environ(), c.Inherit)...), c.Env...)
	stderr := &tail{max: stderrKept}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start %s: %w", c.Path, err)
	}
	var readErr error
	if c.Stdout != nil {
		readErr = c.Stdout(stdout)
	} else {
		_, readErr = io.Copy(io.Discard, stdout)
	}
	if readErr != nil {
		_ = cmd.Process.Kill()
	}
	waitErr := cmd.Wait()
	kept := strings.TrimSpace(stderr.String())
	var exit *exec.ExitError
	switch {
	case readErr != nil:
		return kept, fmt.Errorf("%s wrote %w", filepath.Base(c.Path), readErr)
	case errors.As(waitErr, &exit):
		return kept, &ExitError{Program: c.Path, ExitCode: exit.ExitCode(), Stderr: kept}
	case waitErr != nil:
		return kept, fmt.Errorf("run %s: %w", c.Path, waitErr)
	}
	return kept, nil
}

// inherited is each entry of environ whose name one of names matches, as a
// name or a prefix ending in *.
func inherited(environ, names []string) []string {
	var out []string
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		for _, n := range names {
			if prefix, ok := strings.CutSuffix(n, "*"); (ok && strings.HasPrefix(name, prefix)) || name == n {
				out = append(out, entry)
				break
			}
		}
	}
	return out
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
