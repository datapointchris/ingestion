package extract

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
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
	var pins []string
	for line := range strings.Lines(string(lock)) {
		if line != "" && line[0] != ' ' && line[0] != '#' && strings.TrimSpace(line) != "" {
			pins = append(pins, strings.TrimSuffix(strings.TrimSpace(line), " \\"))
		}
	}
	_, block, _ := strings.Cut(string(models), "pins='\n")
	block, _, _ = strings.Cut(block, "'")
	for line := range strings.Lines(strings.TrimSpace(block)) {
		pins = append(pins, strings.TrimSpace(line))
	}
	sum := sha256.Sum256([]byte(strings.Join(pins, "\n")))
	if got := hex.EncodeToString(sum[:])[:12]; got != DoclingPins {
		t.Errorf("DoclingPins is %s, and the %d pins digest to %s", DoclingPins, len(pins), got)
	}
	for _, want := range []string{"docling-slim==" + DoclingVersion, "pymupdf==" + PyMuPDFVersion} {
		if !strings.Contains(strings.Join(pins, "\n"), want) {
			t.Errorf("the lock pins no %s", want)
		}
	}
}
