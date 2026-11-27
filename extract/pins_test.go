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
// extractor's version, and no other, so a lock recompile or a model re-pin
// that moves one program's text writes a new parse beside the old one and
// leaves the other program's parses as they are.
func TestTheExtractorVersionsNameTheirLibrariesAndModelPins(t *testing.T) {
	lock := lockedLibraries(t, read(t, "../release/python-requirements.txt"))
	_, ffmpeg, _ := strings.Cut(read(t, "../release/install-tools"), "\n    ffmpeg)\n")
	ffmpeg, _, _ = strings.Cut(ffmpeg, ";;")
	var ffmpegPins []string
	for line := range strings.Lines(ffmpeg) {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "version=") || strings.HasPrefix(line, "sha256=") {
			ffmpegPins = append(ffmpegPins, line)
		}
	}
	for _, c := range []struct {
		name, pinned string
		libraries    []string
		pins         []string
	}{
		{"DoclingPins", DoclingPins, doclingLibraries, scriptPins(read(t, "../release/download-docling-models"))},
		{"WhisperPins", WhisperPins, whisperLibraries, append(scriptPins(read(t, "../release/download-whisper-model")), ffmpegPins...)},
		{"TrafilaturaPins", TrafilaturaPins, trafilaturaLibraries, nil},
	} {
		pins := append(lock.closure(t, c.libraries), c.pins...)
		sum := sha256.Sum256([]byte(strings.Join(pins, "\n")))
		if got := hex.EncodeToString(sum[:])[:12]; got != c.pinned {
			t.Errorf("%s is %s, and its %d pins digest to %s", c.name, c.pinned, len(pins), got)
		}
	}
	all := strings.Join(lock.pins, "\n")
	for _, want := range []string{"docling-slim==" + DoclingVersion, "pymupdf==" + PyMuPDFVersion, "faster-whisper==" + FasterWhisperVersion, "trafilatura==" + TrafilaturaVersion} {
		if !strings.Contains(all, want) {
			t.Errorf("the lock pins no %s", want)
		}
	}
}

// lock is a uv lock's libraries in its order: each one's name and pin line,
// and the libraries that each one needs, read from its via lines.
type lock struct {
	names []string
	pins  []string
	needs map[string][]string
}

// lockedLibraries reads the lock that uv pip compile writes: a pin line per
// library, then its hashes, then `# via` and the libraries that need it, on
// that line or one a line after it.
func lockedLibraries(t *testing.T, text string) lock {
	t.Helper()
	l := lock{needs: map[string][]string{}}
	current := ""
	via := false
	for line := range strings.Lines(text) {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
		case line[0] != ' ' && line[0] != '#':
			current = libraryName(trimmed)
			l.names = append(l.names, current)
			l.pins = append(l.pins, strings.TrimSuffix(trimmed, " \\"))
			via = false
		case trimmed == "# via":
			via = true
		case strings.HasPrefix(trimmed, "# via "):
			l.need(strings.TrimPrefix(trimmed, "# via "), current)
			via = false
		case via && strings.HasPrefix(trimmed, "#   "):
			l.need(strings.TrimPrefix(trimmed, "#   "), current)
		default:
			via = false
		}
	}
	if len(l.names) == 0 {
		t.Fatal("the lock pins no library")
	}
	return l
}

// need records that by needs library, unless by is the lock's input file.
func (l lock) need(by, library string) {
	if !strings.HasPrefix(by, "-r ") {
		l.needs[libraryName(by)] = append(l.needs[libraryName(by)], library)
	}
}

// closure is the pin line of each library in roots and of every library that
// one of them needs, in the lock's order. It fails where the lock pins no
// root.
func (l lock) closure(t *testing.T, roots []string) []string {
	t.Helper()
	in := map[string]bool{}
	queue := slices.Clone(roots)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		if in[name] {
			continue
		}
		in[name] = true
		queue = append(queue, l.needs[name]...)
	}
	var pins []string
	for i, name := range l.names {
		if in[name] {
			pins = append(pins, l.pins[i])
		}
	}
	for _, root := range roots {
		if !slices.Contains(l.names, root) {
			t.Errorf("the lock pins no %s", root)
		}
	}
	return pins
}

// libraryName is the library that a pin line or via entry names, without its
// version, its URL or its extras.
func libraryName(s string) string {
	end := strings.IndexAny(s, "=@[ ;")
	if end < 0 {
		end = len(s)
	}
	return strings.ToLower(s[:end])
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
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
