package scan

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// normalizedMax bounds the bytes of a text that its normalized view reads.
// The raw view reads the whole text. ATR's own engine reads no field past
// 100,000 characters.
const normalizedMax = 8 << 20

// suppressMeta is the meta that marks a rule whose matches inside code do not
// count, as ATR's tags.suppress_in_code_blocks marks it.
const suppressMeta = "suppress_in_code"

// The views of one text that yr scans, each a file named for the text and
// suffixed with the view's name.
const (
	viewRaw        = ".raw"
	viewNormalized = ".normalized"
)

// view is one file that yr scans for a text, and how its bytes map back to
// the text's own. ATR's engine matches each pattern against the text after
// NFKC, a strip of zero-width and bidirectional controls and a fold of
// homoglyphs to Latin, then against the text as written, each without regard
// to case. YARA-X folds the case of ASCII only, so each view also lowers the
// case of every other letter, and the converter lowers each pattern's.
type view struct {
	suffix string
	text   string
	// shifts are the view's runs whose length differs from the bytes of the
	// text they came from, in order. A byte past a run sits as far past it
	// in the text as in the view.
	shifts []shift
	codes  []span
	coded  bool
}

// shift is one run of a view and the text's bytes it came from.
type shift struct{ at, length, rawAt, rawLength int }

// span is a half-open run of bytes.
type span struct{ start, end int }

// views prepares text as ATR's engine reads it: the raw view, and the
// normalized view where it differs.
func views(text string) []*view {
	if isASCII(text) {
		return []*view{{suffix: viewRaw, text: text}}
	}
	raw := build(text, viewRaw, lowerSegment)
	cut := min(len(text), normalizedMax)
	for cut < len(text) && !utf8.RuneStart(text[cut]) {
		cut--
	}
	normalized := build(text[:cut], viewNormalized, normalizeSegment)
	if cut == len(text) && normalized.text == raw.text {
		return []*view{raw}
	}
	return []*view{raw, normalized}
}

func isASCII(s string) bool {
	for i := range len(s) {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// build writes each segment of text through prepare. A raw view's segment is
// one rune, and a normalized view's is one that NFKC composes alone.
func build(text, suffix string, prepare func(out *strings.Builder, segment []byte)) *view {
	v := &view{suffix: suffix}
	var b strings.Builder
	b.Grow(len(text))
	write := func(start, end int, segment []byte) {
		at := b.Len()
		prepare(&b, segment)
		if b.Len()-at != end-start {
			v.shifts = append(v.shifts, shift{at: at, length: b.Len() - at, rawAt: start, rawLength: end - start})
		}
	}
	if suffix == viewRaw {
		for i := 0; i < len(text); {
			if text[i] < utf8.RuneSelf {
				b.WriteByte(text[i])
				i++
				continue
			}
			_, size := utf8.DecodeRuneInString(text[i:])
			write(i, i+size, []byte(text[i:i+size]))
			i += size
		}
	} else {
		var it norm.Iter
		it.InitString(norm.NFKC, text)
		for !it.Done() {
			start := it.Pos()
			segment := it.Next()
			write(start, it.Pos(), segment)
		}
	}
	v.text = b.String()
	return v
}

// lowerSegment writes segment with each letter outside ASCII in lower case,
// and any byte that is not UTF-8 as it is.
func lowerSegment(out *strings.Builder, segment []byte) {
	for len(segment) > 0 {
		r, size := utf8.DecodeRune(segment)
		if r == utf8.RuneError && size <= 1 {
			out.WriteByte(segment[0])
			segment = segment[1:]
			continue
		}
		if r >= utf8.RuneSelf {
			r = unicode.ToLower(r)
		}
		out.WriteRune(r)
		segment = segment[size:]
	}
}

// normalizeSegment writes an NFKC segment as ATR's engine prepares it, less
// a zero-width or bidirectional control, with each homoglyph folded to Latin
// and each other letter outside ASCII in lower case.
func normalizeSegment(out *strings.Builder, segment []byte) {
	for len(segment) > 0 {
		r, size := utf8.DecodeRune(segment)
		if r == utf8.RuneError && size <= 1 {
			out.WriteByte(segment[0])
			segment = segment[1:]
			continue
		}
		segment = segment[size:]
		if stripped(r) {
			continue
		}
		if latin, ok := confusables[r]; ok {
			r = latin
		} else if r >= utf8.RuneSelf {
			r = unicode.ToLower(r)
		}
		out.WriteRune(r)
	}
}

// stripped is whether ATR's normalizeUnicode removes r.
func stripped(r rune) bool {
	switch {
	case r == 0x200B, r == 0x200C, r == 0x200D, r == 0xFEFF, r == 0x2060, r == 0x180E, r == 0x200E, r == 0x200F:
		return true
	case 0x202A <= r && r <= 0x202E, 0x2066 <= r && r <= 0x2069:
		return true
	}
	return false
}

// confusables is ATR's CONFUSABLE_TO_LATIN: each Cyrillic, Greek or other
// homoglyph that its engine folds to the Latin letter it looks like.
var confusables = map[rune]rune{
	0x0430: 'a', 0x0435: 'e', 0x043E: 'o', 0x0440: 'p', 0x0441: 'c',
	0x0445: 'x', 0x0443: 'y', 0x0456: 'i', 0x0455: 's', 0x0458: 'j',
	0x04BB: 'h', 0x0501: 'd', 0x051B: 'q', 0x0261: 'g', 0x043D: 'h',
	0x043A: 'k', 0x043C: 'm', 0x0442: 't', 0x0432: 'b',
	0x0410: 'A', 0x0412: 'B', 0x0415: 'E', 0x041A: 'K', 0x041C: 'M',
	0x041D: 'H', 0x041E: 'O', 0x0420: 'P', 0x0421: 'C', 0x0422: 'T',
	0x0425: 'X', 0x0423: 'Y', 0x0406: 'I', 0x0408: 'J', 0x0405: 'S',
	0x03BF: 'o', 0x03C1: 'p', 0x03B1: 'a', 0x03B5: 'e', 0x03BD: 'v',
	0x03BA: 'k', 0x0391: 'A', 0x0392: 'B', 0x0395: 'E', 0x0396: 'Z',
	0x0397: 'H', 0x0399: 'I', 0x039A: 'K', 0x039C: 'M', 0x039D: 'N',
	0x039F: 'O', 0x03A1: 'P', 0x03A4: 'T', 0x03A7: 'X', 0x03A5: 'Y',
	0x0131: 'i', 0x01C0: 'l', 0x0578: 'n',
}

// rawStart is where the text holds the byte that starts at at in the view. A
// match that starts inside a run starts at the run's start in the text.
func (v *view) rawStart(at int) int {
	i := sort.Search(len(v.shifts), func(i int) bool { return v.shifts[i].at > at }) - 1
	if i < 0 {
		return at
	}
	s := v.shifts[i]
	if at < s.at+s.length {
		return s.rawAt
	}
	return s.rawAt + s.rawLength + at - (s.at + s.length)
}

// rawEnd is where the text ends a match that ends at at in the view. A match
// that ends inside a run ends at the run's end in the text.
func (v *view) rawEnd(at int) int {
	i := sort.Search(len(v.shifts), func(i int) bool { return v.shifts[i].at >= at }) - 1
	if i < 0 {
		return at
	}
	s := v.shifts[i]
	if at <= s.at+s.length {
		return s.rawAt + s.rawLength
	}
	return s.rawAt + s.rawLength + at - (s.at + s.length)
}

// inCode is whether the view's byte at at sits inside code, as ATR's
// buildCodeBlockRanges reads markdown.
func (v *view) inCode(at int) bool {
	if !v.coded {
		v.codes, v.coded = codeRanges(v.text), true
	}
	i, _ := slices.BinarySearchFunc(v.codes, at, func(s span, at int) int { return cmp.Compare(s.start, at+1) })
	return i > 0 && at < v.codes[i-1].end
}

var (
	inlineCode = regexp.MustCompile("`[^`\n]+`")
	tableLine  = regexp.MustCompile(`(?m)^\|.*$`)
	quoted     = regexp.MustCompile(`"[^"\n]+"`)
)

// codeRanges are text's runs of markdown code, merged and in order: each
// fenced block, an unclosed one running to the end, each inline code span
// outside a fenced block, and each quoted string on a table's row.
func codeRanges(text string) []span {
	var ranges []span
	open, pos := -1, 0
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(strings.TrimLeftFunc(line, unicode.IsSpace), "```") {
			if open < 0 {
				open = pos
			} else {
				ranges = append(ranges, span{open, min(pos+len(line)+1, len(text))})
				open = -1
			}
		}
		pos += len(line) + 1
	}
	if open >= 0 {
		ranges = append(ranges, span{open, len(text)})
	}
	fenced := len(ranges)
	for _, m := range inlineCode.FindAllStringIndex(text, -1) {
		if !slices.ContainsFunc(ranges[:fenced], func(s span) bool { return s.start <= m[0] && m[0] < s.end }) {
			ranges = append(ranges, span{m[0], m[1]})
		}
	}
	for _, line := range tableLine.FindAllStringIndex(text, -1) {
		for _, q := range quoted.FindAllStringIndex(text[line[0]:line[1]], -1) {
			ranges = append(ranges, span{line[0] + q[0], line[0] + q[1]})
		}
	}
	slices.SortFunc(ranges, func(a, b span) int { return cmp.Compare(a.start, b.start) })
	merged := ranges[:0]
	for _, r := range ranges {
		if n := len(merged); n > 0 && r.start <= merged[n-1].end {
			merged[n-1].end = max(merged[n-1].end, r.end)
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

// textMatch is one rule's match in one text that matchTexts scanned, placed
// in that text's own bytes.
type textMatch struct {
	text       string
	rule       string
	severity   string
	start, end int
}

// matchTexts scans each text, named by its key, with the compiled rules over
// each of its views, and answers each match placed in the text as written.
// It drops a match inside code where its rule says to, and checks that the
// bytes yr printed are the view's bytes at the match's offset.
func matchTexts(ctx context.Context, yr, compiled string, texts map[string]string, printed, perPattern int) ([]textMatch, error) {
	prepared := map[string]*view{}
	files := map[string]string{}
	for name, text := range texts {
		if text == "" {
			continue
		}
		for _, v := range views(text) {
			prepared[name+v.suffix] = v
			files[name+v.suffix] = v.text
		}
	}
	if len(files) == 0 {
		return nil, nil
	}
	matches, err := yrScan(ctx, yr, compiled, files, printed, perPattern)
	if err != nil {
		return nil, err
	}
	out := make([]textMatch, 0, len(matches))
	for _, m := range matches {
		v, ok := prepared[m.File]
		if !ok {
			return nil, fmt.Errorf("yr named a match in %q, a file it was not given", m.File)
		}
		end := m.Offset + len(m.Printed)
		if m.Offset < 0 || end > len(v.text) || v.text[m.Offset:end] != string(m.Printed) {
			return nil, fmt.Errorf("yr named a match at offset %d that is not the text there", m.Offset)
		}
		if m.Meta[suppressMeta] == "true" && v.inCode(m.Offset) {
			continue
		}
		out = append(out, textMatch{
			text: strings.TrimSuffix(m.File, v.suffix), rule: cmp.Or(m.Meta["id"], m.Identifier), severity: m.Meta["severity"],
			start: v.rawStart(m.Offset), end: v.rawEnd(end + m.More),
		})
	}
	return out, nil
}
