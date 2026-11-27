package extract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/isolatedprocess"
	"github.com/datapointchris/ingestion/store"
)

// WhisperProgram is where the worker image installs audio-whisper, which
// transcribes the audio it reads on stdin with faster-whisper.
const WhisperProgram = ProgramDir + "/audio-whisper"

// FasterWhisperVersion is the faster-whisper that release/python-requirements.txt
// pins, and that the worker image installs for audio-whisper.
const FasterWhisperVersion = "1.2.1"

// WhisperModel is the model that audio-whisper runs, whose files
// release/download-whisper-model pins.
const WhisperModel = "large-v3-turbo"

// WhisperPins is the first 12 hex digits of the SHA-256 of the pin of each
// library in whisperLibraries' closure in release/python-requirements.txt, as
// DoclingPins reads its own, then every model pin in
// release/download-whisper-model, then ffmpeg's version and SHA-256 in
// release/install-tools, one a line. Each shapes a transcript, and a test fails
// until this equals them.
const WhisperPins = "a764b748ef26"

// whisperLibraries are the libraries that audio-whisper imports.
var whisperLibraries = []string{"faster-whisper", "numpy"}

// WhisperVersion names the transcription that audio-whisper makes, so a
// transcript by other libraries, weights or decoding writes beside the old. A
// change to the program's own settings raises the leading number.
const WhisperVersion = "1+faster-whisper-" + FasterWhisperVersion + "-" + WhisperModel + "-" + WhisperPins

// AudioWhisper is the primary extractor of a memo's audio, in each container
// that the push route lands a memo as. It runs audio-whisper over the item's
// bytes in SlotGroupHeavy, so a memo, a video's audio and a book's Docling
// parse never run at once on one worker.
var AudioWhisper = AudioWhisperAt(WhisperProgram)

// AudioWhisperTimeout bounds one parse by AudioWhisper. The longest memo that
// the push route takes is the recording that server.MaxMemoBytes holds. A run
// measures well under the audio's length with `/usr/bin/time -v audio-whisper
// < <the audio>`, so the bound leaves room past it.
const AudioWhisperTimeout = 12 * time.Hour

// AudioWhisperAt is AudioWhisper, run by the audio-whisper program at
// program. Each segment that holds speech becomes one unit, labeled by its
// span as SpanLabel writes it, and the parse keeps an AudioAnswer.
func AudioWhisperAt(program string) Extractor {
	return Extractor{
		Name: "audio-whisper", Version: WhisperVersion, MediaType: store.MemoMediaTypes[0], AlsoReads: store.MemoMediaTypes[1:],
		Program: program, SlotGroup: SlotGroupHeavy, Timeout: AudioWhisperTimeout,
		Extract: func(ctx context.Context, body io.Reader) (Read, error) {
			transcript, err := Transcribe(ctx, program, body)
			if err != nil {
				return Read{}, err
			}
			return memoRead(transcript)
		},
	}
}

// AudioAnswer is what an AudioWhisper parse keeps as its answer: the model,
// what audio-whisper reported of the audio, how many segments it heard, and
// where each unit starts in the audio, in milliseconds, by the unit's order.
type AudioAnswer struct {
	Model string `json:"model"`
	TranscriptInfo
	Segments int     `json:"segments"`
	StartsMs []int64 `json:"starts_ms"`
}

// memoRead is a memo's parse of transcript: one unit for each segment that
// holds speech, its whitespace folded, in order.
func memoRead(transcript Transcript) (Read, error) {
	answer := AudioAnswer{Model: WhisperModel, TranscriptInfo: transcript.Info, Segments: len(transcript.Segments), StartsMs: []int64{}}
	var units []Unit
	for _, s := range transcript.Segments {
		text := strings.Join(strings.Fields(s.Text), " ")
		if text == "" {
			continue
		}
		label := SpanLabel(s.StartMs, s.StartMs+s.DurationMs)
		units = append(units, Unit{Label: &label, Text: text})
		answer.StartsMs = append(answer.StartsMs, s.StartMs)
	}
	raw, err := json.Marshal(answer)
	if err != nil {
		return Read{}, err
	}
	return Read{Units: units, Answer: raw}, nil
}

// SpanLabel is the label of a stretch of audio from startMs to endMs: each
// end as mm:ss, or h:mm:ss from an hour on, joined by an en dash, such as
// 01:05–01:12.
func SpanLabel(startMs, endMs int64) string {
	return clock(startMs) + "–" + clock(endMs)
}

// clock is ms into the audio as mm:ss, or h:mm:ss from an hour on.
func clock(ms int64) string {
	seconds := ms / 1000
	if seconds >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
	}
	return fmt.Sprintf("%02d:%02d", seconds/60, seconds%60)
}

// Segment is one stretch of timed text, such as a caption or a run of speech:
// when it starts and how long it runs, in milliseconds, and its text on one
// line.
type Segment struct {
	StartMs    int64  `json:"start_ms"`
	DurationMs int64  `json:"duration_ms"`
	Text       string `json:"text"`
}

// TranscriptInfo is what audio-whisper reports of the audio before its
// segments: the language it heard and how sure it is, how long the audio
// runs, and how much of it the voice-activity filter kept.
type TranscriptInfo struct {
	Language            string  `json:"language"`
	LanguageProbability float64 `json:"language_probability"`
	DurationMs          int64   `json:"duration_ms"`
	VoicedMs            int64   `json:"voiced_ms"`
}

// Transcript is what audio-whisper heard.
type Transcript struct {
	Info     TranscriptInfo
	Segments []Segment
}

// Transcribe runs the program at program, as audio-whisper, over audio. The
// program reads the audio on stdin in any container that ffmpeg reads. It
// writes its TranscriptInfo as its first line on stdout, then one Segment a
// line in order. It runs through isolatedprocess, so it holds none of the
// worker's credentials. A run that exits non-zero answers an error with the
// end of its stderr, and one that exits zero with no info line answers an
// error of its own.
func Transcribe(ctx context.Context, program string, audio io.Reader) (Transcript, error) {
	var t Transcript
	var seenInfo bool
	_, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
		Path: program, Stdin: audio,
		Stdout: func(r io.Reader) (err error) { t, seenInfo, err = readTranscript(r); return err },
	})
	if err == nil && !seenInfo {
		return t, errors.New("audio-whisper exited 0 and wrote no transcript info")
	}
	return t, err
}

// readTranscript reads a TranscriptInfo from r's first line and a Segment from
// each line after, refusing any field that neither holds. It says whether r
// held the info line, and a stdout with none is no error of its own, so a run
// that failed before writing keeps its exit status and stderr.
func readTranscript(r io.Reader) (Transcript, bool, error) {
	var t Transcript
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64<<10), lineMax)
	seenInfo := false
	for line := 1; scanner.Scan(); line++ {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(scanner.Bytes()))
		decoder.DisallowUnknownFields()
		if !seenInfo {
			if err := decoder.Decode(&t.Info); err != nil {
				return t, false, fmt.Errorf("line %d, which is not the transcript's info: %w", line, err)
			}
			seenInfo = true
			continue
		}
		var s Segment
		if err := decoder.Decode(&s); err != nil {
			return t, true, fmt.Errorf("line %d, which is not a segment: %w", line, err)
		}
		t.Segments = append(t.Segments, s)
	}
	if err := scanner.Err(); err != nil {
		return t, seenInfo, fmt.Errorf("a stdout it could not read: %w", err)
	}
	return t, seenInfo, nil
}
