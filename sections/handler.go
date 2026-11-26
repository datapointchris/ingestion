package sections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/isolatedprocess"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/worker"
)

const (
	// Producer is the program that writes each structure output.
	Producer = "pdf-structure"
	// Version names the program's number and PyMuPDF's, so a changed program
	// or a library upgrade writes its profile beside the old one.
	Version = "2+pymupdf-" + extract.PyMuPDFVersion
	// Capability is the capability of every structure task.
	Capability = store.StageStructure + ":" + Producer
	// Program is where the worker image installs pdf-structure.
	Program = extract.ProgramDir + "/" + Producer
	// Timeout bounds one run of pdf-structure, and a run past it fails its
	// task. It sits far above any book's run, so it stops only a program
	// that hangs.
	Timeout = 10 * time.Minute
	// blobRetry is how long a structure task waits to read its bytes again
	// after the service gave no answer.
	blobRetry = 30 * time.Second
)

// NewHandler is the worker handler that profiles each structure task's PDF
// with the program at path, reading its bytes through client. It is nil
// where the program is not installed, so the worker claims no structure task.
func NewHandler(path string, client *worker.Client, log *slog.Logger) worker.Handler {
	if log == nil {
		log = slog.Default()
	}
	if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		log.Warn("pdf-structure is not installed here, so this worker claims no structure task", "program", path)
		return nil
	}
	return handler{program: path, client: client}
}

type handler struct {
	program string
	client  *worker.Client
}

func (h handler) Capability() string { return Capability }

// Run profiles the task's PDF and reports the profile as the structure
// output's answer. A PDF that the program cannot open, or a profile it cannot
// read, fails the task with the reason kept. A read of the bytes that the
// service gave no answer to runs again.
func (h handler) Run(ctx context.Context, task worker.Task) worker.Result {
	if task.BlobSHA256 == nil {
		return failed("the item holds no bytes to profile")
	}
	body, _, err := h.client.Blob(ctx, *task.BlobSHA256)
	if err != nil {
		reason := fmt.Sprintf("read the item's bytes: %v", err)
		var refused *worker.StatusError
		if errors.As(err, &refused) && refused.Status != http.StatusTooManyRequests && refused.Status < 500 {
			return failed(reason)
		}
		return worker.Result{Outcome: server.OutcomeRetry, Error: &reason, RetryAfter: blobRetry}
	}
	defer func() { _ = body.Close() }()
	runCtx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	var profile Profile
	_, err = isolatedprocess.Run(runCtx, isolatedprocess.Command{
		Path: h.program, Stdin: body,
		Stdout: func(r io.Reader) (err error) { profile, err = readProfile(r); return err },
	})
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return failed(fmt.Sprintf("%s ran past its %s limit", Producer, Timeout))
	}
	if err != nil {
		return failed(err.Error())
	}
	answer, err := json.Marshal(profile)
	if err != nil {
		return failed(fmt.Sprintf("encode the profile: %v", err))
	}
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: Producer, ProducerVersion: Version, State: store.OutputDone, Answer: answer,
	}}}
}

// failed fails the task with reason, and records the structure output as
// failed with it.
func failed(reason string) worker.Result {
	return worker.Result{Outcome: server.OutcomeFailed, Error: &reason, Outputs: []server.ReportOutput{{
		Producer: Producer, ProducerVersion: Version, State: store.OutputFailed, Error: &reason,
	}}}
}
