package ypl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
)

// CaptionsExtractor and WhisperExtractor are the extractors of a video. Each
// reads it through ypl's door, because the item holds no bytes.
// CaptionsExtractor reads the caption track that ypl picks. Where the video
// has none, its parse defers to WhisperExtractor, which transcribes the
// video's audio. A worker runs one captions parse at a time, because ypl reads
// one video at a time, so a second would only wait at ypl and hold a slot.
// CaptionsExtractor reads a pushed YouTube link, so such a link lands under
// the watch URL that the poll lands a video under.
var (
	CaptionsExtractor = extract.Extractor{
		Name: "youtube-captions", Version: "1", MediaType: MediaTypeVideo,
		ReadsURL: true, Open: openCaptions, DefersTo: whisperName, SlotLimit: 1, LinkKind: itemkey.YouTube, LinkURL: WatchURL,
	}
	WhisperExtractor = WhisperExtractorAt(extract.WhisperProgram)
)

const whisperName = "youtube-whisper"

// WhisperExtractorAt is WhisperExtractor, run by the audio-whisper program at
// program. It runs in extract.SlotGroupHeavy with Docling, so a worker runs
// one of their parses at a time, because each holds gigabytes.
func WhisperExtractorAt(program string) extract.Extractor {
	return extract.Extractor{
		Name: whisperName, Version: extract.WhisperVersion, MediaType: MediaTypeVideo, Program: program,
		Secondary: true, ReadsURL: true, SlotGroup: extract.SlotGroupHeavy, Timeout: WhisperTimeout,
		Open: func(api string, client *http.Client) extract.Reader {
			return whisperReader{client: NewClient(api, client), program: program}.read
		},
	}
}

// WhisperTimeout bounds one parse by WhisperExtractor, however long the video. A video
// of known length is bounded sooner, by transcribeBound.
const WhisperTimeout = 12 * time.Hour

// transcribeBound is how long a Whisper parse may take over audio that runs
// seconds, from the audio's request to its transcript: audioWait for ypl to
// begin its answer, half an hour for the audio's arrival and the model's load,
// then the audio's own length again. A four-thread run measures well under the
// audio's length with `/usr/bin/time -v audio-whisper < <the audio>`, so the
// bound leaves room past it.
func transcribeBound(seconds int64) time.Duration {
	return audioWait + 30*time.Minute + time.Duration(seconds)*time.Second
}

// shortTranscript says whether a transcript of decodedMs falls well short of
// a video of seconds, as audio cut off partway does, which ffmpeg may decode
// with no error.
func shortTranscript(decodedMs, seconds int64) bool {
	return decodedMs*10 < seconds*1000*9
}

// blockSeconds is how long each block of a video with no chapters runs.
const blockSeconds = 5 * 60

// How long a parse waits before reading again: where the video has not
// begun, and where the read failed in a way a later read may not.
const (
	notStartedRetry = time.Hour
	failedRetry     = 5 * time.Minute
)

// captionsAnswer is what a captions parse keeps as its answer: the track it
// read, or null, the video's language and length, and how many chapters and
// segments it read.
type captionsAnswer struct {
	Track           *Track  `json:"track"`
	Language        *string `json:"language"`
	DurationSeconds *int64  `json:"duration_seconds"`
	Chapters        int     `json:"chapters"`
	Segments        int     `json:"segments"`
}

// whisperAnswer is what a Whisper parse keeps as its answer: the model, what
// audio-whisper reported of the audio, and how many chapters and segments it
// made units of.
type whisperAnswer struct {
	Model string `json:"model"`
	extract.TranscriptInfo
	Chapters int `json:"chapters"`
	Segments int `json:"segments"`
}

// openCaptions is CaptionsExtractor's reader, which reads through ypl at api.
func openCaptions(api string, client *http.Client) extract.Reader {
	c := NewClient(api, client)
	return func(ctx context.Context, item extract.Item) (extract.Read, error) {
		id, err := videoID(item.Key)
		if err != nil {
			return extract.Read{}, err
		}
		captions, err := c.Captions(ctx, id)
		if err != nil {
			return extract.Read{}, readError(err)
		}
		answer, err := json.Marshal(captionsAnswer{
			Track: captions.Track, Language: captions.Language, DurationSeconds: captions.DurationSeconds,
			Chapters: len(captions.Chapters), Segments: len(captions.Segments),
		})
		if err != nil {
			return extract.Read{}, err
		}
		if captions.Track == nil || !slices.ContainsFunc(captions.Segments, spoken) {
			return extract.Read{Deferred: true, Answer: answer}, nil
		}
		return extract.Read{Units: videoUnits(captions.Description, captions.Chapters, captions.Segments), Answer: answer}, nil
	}
}

// whisperReader reads a video's description and chapters through ypl, then
// streams its audio through audio-whisper at program.
type whisperReader struct {
	client  *Client
	program string
}

func (w whisperReader) read(ctx context.Context, item extract.Item) (extract.Read, error) {
	id, err := videoID(item.Key)
	if err != nil {
		return extract.Read{}, err
	}
	captions, err := w.client.Captions(ctx, id)
	if err != nil {
		return extract.Read{}, readError(err)
	}
	runCtx, cancel := ctx, context.CancelFunc(func() {})
	if captions.DurationSeconds != nil {
		runCtx, cancel = context.WithTimeout(ctx, transcribeBound(*captions.DurationSeconds))
	}
	defer cancel()
	ranPast := func() error {
		return fmt.Errorf("the Whisper parse ran past %s over a video of %d seconds", transcribeBound(*captions.DurationSeconds), *captions.DurationSeconds)
	}
	audio, err := w.client.Audio(runCtx, id)
	if err != nil && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return extract.Read{}, ranPast()
	}
	if err != nil {
		return extract.Read{}, readError(err)
	}
	defer func() { _ = audio.Body.Close() }()
	transcript, err := extract.Transcribe(runCtx, w.program, audio.Body)
	broken := audio.Broken()
	switch {
	case (err != nil || broken != nil) && ctx.Err() == nil && errors.Is(runCtx.Err(), context.DeadlineExceeded):
		return extract.Read{}, ranPast()
	case broken != nil:
		return extract.Read{}, &extract.RetryError{Err: fmt.Errorf("the audio's stream from ypl broke: %w", broken), After: failedRetry, Bounded: true}
	case err != nil:
		return extract.Read{}, err
	case captions.DurationSeconds != nil && shortTranscript(transcript.Info.DurationMs, *captions.DurationSeconds):
		return extract.Read{}, &extract.RetryError{
			Err: fmt.Errorf("audio-whisper decoded %d s of a video of %d s, so the audio was cut short",
				transcript.Info.DurationMs/1000, *captions.DurationSeconds),
			After: failedRetry, Bounded: true,
		}
	}
	answer, err := json.Marshal(whisperAnswer{
		Model: extract.WhisperModel, TranscriptInfo: transcript.Info,
		Chapters: len(captions.Chapters), Segments: len(transcript.Segments),
	})
	if err != nil {
		return extract.Read{}, err
	}
	return extract.Read{Units: videoUnits(captions.Description, captions.Chapters, transcript.Segments), Answer: answer}, nil
}

// videoID is the video id that key, youtube/<id>, names.
func videoID(key string) (string, error) {
	id, ok := strings.CutPrefix(key, itemkey.YouTube+"/")
	if !ok || id == "" {
		return "", fmt.Errorf("a video's key is %s/<video id>, not %s", itemkey.YouTube, key)
	}
	return id, nil
}

// readError is how a parse treats err from a read through ypl, as classify
// reads it. Where ypl asked for a wait, or gave no answer, the parse waits,
// because nothing about the video failed. Where the video is a premiere or a
// stream that has not begun, it waits an hour. A refusal that no wait clears,
// such as a video that no signed-out read reaches or a ypl that holds no
// yt-dlp, fails it with ypl's refusal kept. Any other failure, such as ypl
// failing to read YouTube, queues it again boundedly.
func readError(err error) error {
	var refusal *Error
	if errors.As(err, &refusal) && refusal.Code == "video_not_started" {
		return &extract.RetryError{Err: err, After: notStartedRetry}
	}
	f := classify(err)
	switch {
	case f.Wait > 0:
		return &extract.RetryError{Err: err, After: f.Wait}
	case f.Final:
		return err
	}
	return &extract.RetryError{Err: err, After: failedRetry, Bounded: true}
}

// spoken says whether s holds any text.
func spoken(s extract.Segment) bool { return strings.TrimSpace(s.Text) != "" }

// videoUnits are a video's parse: its description, then the text of each
// chapter labeled with its start and title, or of each five-minute block
// labeled with its start where the video has no chapters. A segment joins the
// last chapter that starts at or before it, or the first. A segment's runs of
// whitespace read as one space, and a unit's segments join by one space.
func videoUnits(description string, chapters []Chapter, segments []extract.Segment) []extract.Unit {
	described := "Description"
	units := []extract.Unit{{Label: &described, Text: description}}
	texts := map[int][]string{}
	for _, s := range segments {
		text := strings.Join(strings.Fields(s.Text), " ")
		if text == "" {
			continue
		}
		at := int(s.StartMs / 1000 / blockSeconds)
		if len(chapters) > 0 {
			at = 0
			for i, c := range chapters {
				if c.StartSeconds*1000 <= s.StartMs {
					at = i
				}
			}
		}
		texts[at] = append(texts[at], text)
	}
	if len(chapters) > 0 {
		for i, c := range chapters {
			label := clock(c.StartSeconds) + " " + c.Title
			units = append(units, extract.Unit{Label: &label, Text: strings.Join(texts[i], " ")})
		}
		return units
	}
	for _, block := range slices.Sorted(maps.Keys(texts)) {
		label := clock(int64(block) * blockSeconds)
		units = append(units, extract.Unit{Label: &label, Text: strings.Join(texts[block], " ")})
	}
	return units
}

// clock writes seconds as YouTube writes a time in a video: m:ss, or h:mm:ss
// from an hour.
func clock(seconds int64) string {
	if seconds >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
	}
	return fmt.Sprintf("%d:%02d", seconds/60, seconds%60)
}
