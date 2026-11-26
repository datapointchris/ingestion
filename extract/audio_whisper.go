package extract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/datapointchris/ingestion/isolatedprocess"
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

// WhisperPins is the first 12 hex digits of the SHA-256 of every pin in
// release/python-requirements.txt, then every model pin in
// release/download-whisper-model, then ffmpeg's version and SHA-256 in
// release/install-tools, one a line. Each shapes a transcript, and a test fails
// until this equals them.
const WhisperPins = "f25db1625bcd"

// WhisperVersion names the transcription that audio-whisper makes, so a
// transcript by other libraries, weights or decoding writes beside the old. A
// change to the program's own settings raises the leading number.
const WhisperVersion = "1+faster-whisper-" + FasterWhisperVersion + "-" + WhisperModel + "-" + WhisperPins

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
// end of its stderr.
func Transcribe(ctx context.Context, program string, audio io.Reader) (Transcript, error) {
	var t Transcript
	_, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
		Path: program, Stdin: audio,
		Stdout: func(r io.Reader) (err error) { t, err = readTranscript(r); return err },
	})
	return t, err
}

// readTranscript reads a TranscriptInfo from r's first line and a Segment from
// each line after, refusing any field that neither holds.
func readTranscript(r io.Reader) (Transcript, error) {
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
				return t, fmt.Errorf("line %d, which is not the transcript's info: %w", line, err)
			}
			seenInfo = true
			continue
		}
		var s Segment
		if err := decoder.Decode(&s); err != nil {
			return t, fmt.Errorf("line %d, which is not a segment: %w", line, err)
		}
		t.Segments = append(t.Segments, s)
	}
	if err := scanner.Err(); err != nil {
		return t, fmt.Errorf("a stdout it could not read: %w", err)
	}
	if !seenInfo {
		return t, errors.New("audio-whisper wrote no transcript info")
	}
	return t, nil
}
