package extract

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"testing"
)

// Each library version and model pin that shapes a parse is in its
// extractor's version, so a lock recompile or a model re-pin that moves the
// text writes a new parse beside the old one.
func TestTheExtractorVersionsNameTheLockAndTheModelPins(t *testing.T) {
	lock, err := os.ReadFile("../release/python-requirements.txt")
	if err != nil {
		t.Fatal(err)
	}
	models, err := os.ReadFile("../release/download-docling-models")
	if err != nil {
		t.Fatal(err)
	}
	whisper, err := os.ReadFile("../release/download-whisper-model")
	if err != nil {
		t.Fatal(err)
	}
	tools, err := os.ReadFile("../release/install-tools")
	if err != nil {
		t.Fatal(err)
	}
	var locked []string
	for line := range strings.Lines(string(lock)) {
		if line != "" && line[0] != ' ' && line[0] != '#' && strings.TrimSpace(line) != "" {
			locked = append(locked, strings.TrimSuffix(strings.TrimSpace(line), " \\"))
		}
	}
	_, ffmpeg, _ := strings.Cut(string(tools), "\n    ffmpeg)\n")
	ffmpeg, _, _ = strings.Cut(ffmpeg, ";;")
	var ffmpegPins []string
	for line := range strings.Lines(ffmpeg) {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "version=") || strings.HasPrefix(line, "sha256=") {
			ffmpegPins = append(ffmpegPins, line)
		}
	}
	for _, c := range []struct {
		name, pinned string
		pins         []string
	}{
		{"DoclingPins", DoclingPins, append(slices.Clone(locked), scriptPins(string(models))...)},
		{"WhisperPins", WhisperPins, append(append(slices.Clone(locked), scriptPins(string(whisper))...), ffmpegPins...)},
	} {
		sum := sha256.Sum256([]byte(strings.Join(c.pins, "\n")))
		if got := hex.EncodeToString(sum[:])[:12]; got != c.pinned {
			t.Errorf("%s is %s, and the %d pins digest to %s", c.name, c.pinned, len(c.pins), got)
		}
	}
	if len(ffmpegPins) != 2 {
		t.Errorf("install-tools pins ffmpeg by %v, not by a version and a SHA-256", ffmpegPins)
	}
	for _, want := range []string{"docling-slim==" + DoclingVersion, "pymupdf==" + PyMuPDFVersion, "faster-whisper==" + FasterWhisperVersion} {
		if !strings.Contains(strings.Join(locked, "\n"), want) {
			t.Errorf("the lock pins no %s", want)
		}
	}
}

// scriptPins are the lines of a download script's pins block.
func scriptPins(script string) []string {
	_, block, _ := strings.Cut(script, "pins='\n")
	block, _, _ = strings.Cut(block, "'")
	var pins []string
	for line := range strings.Lines(strings.TrimSpace(block)) {
		pins = append(pins, strings.TrimSpace(line))
	}
	return pins
}
