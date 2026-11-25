// Package scan is the scan stage. It runs VirusTotal's YARA-X, as the yr
// program, over an item's parsed text with Agent Threat Rules converted to
// YARA and ingestion's own rules, and stores each finding: the rule, its
// severity and the text it matched. A finding never drops an item. The model
// read calls the same Scanner on each suggestion sentence it writes.
//
// The rules are built once per image by build-rules, which keeps each
// converted rule only where yr compiles it and it passes its own examples.
// The findings carry the rules' version, so a new rule set replays beside
// the old.
package scan

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/datapointchris/ingestion/store"
)

const (
	// RulesDir is where the worker image installs the built rules.
	RulesDir = "/usr/local/share/ingestion/scan-rules"
	// Yr is where the worker image installs yr.
	Yr = "/usr/local/bin/yr"
	// Producer names each scan output. Its version is the rules' version.
	Producer = "rules"
	// SpanKept bounds the bytes of the span that a finding keeps. The
	// finding's length still counts the whole match.
	SpanKept = 1024
	// MatchesPerRule bounds the findings that one rule makes on one text.
	MatchesPerRule = 100
)

// Scanner runs yr over text with one built rule set.
type Scanner struct {
	yr       string
	compiled string
	manifest Manifest
}

// Open is a scanner that runs yr with the rules that build-rules wrote into
// dir.
func Open(yr, dir string) (*Scanner, error) {
	info, err := os.Stat(yr)
	if err != nil {
		return nil, fmt.Errorf("yr: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return nil, fmt.Errorf("%s is not an executable file", yr)
	}
	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		return nil, fmt.Errorf("the rules' manifest: %w", err)
	}
	s := &Scanner{yr: yr, compiled: filepath.Join(dir, CompiledFile)}
	if err := json.Unmarshal(raw, &s.manifest); err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Join(dir, ManifestFile), err)
	}
	if s.manifest.Version == "" {
		return nil, fmt.Errorf("%s names no version", filepath.Join(dir, ManifestFile))
	}
	if _, err := os.Stat(s.compiled); err != nil {
		return nil, fmt.Errorf("the compiled rules: %w", err)
	}
	if s.compiled, err = filepath.Abs(s.compiled); err != nil {
		return nil, err
	}
	return s, nil
}

// Version is the rules' version, which each finding's output carries.
func (s *Scanner) Version() string { return s.manifest.Version }

// Manifest is what the rules' build decided.
func (s *Scanner) Manifest() Manifest { return s.manifest }

// Scan scans one text, such as a suggestion sentence, and answers each
// finding and its span in the order they sit in the text. No finding names a
// unit.
func (s *Scanner) Scan(ctx context.Context, text string) ([]store.Finding, []store.Span, error) {
	return s.scan(ctx, text, nil)
}

// ScanUnits scans a parse's units as one text, joined by a blank line as
// store.Parse.Text joins them, so a rule reads across them as it would read a
// page. Each finding names the unit it starts in, and its offset counts from
// that unit's start.
func (s *Scanner) ScanUnits(ctx context.Context, units []store.Unit) ([]store.Finding, []store.Span, error) {
	texts := make([]string, len(units))
	starts := make([]placed, len(units))
	at := 0
	for i, u := range units {
		texts[i] = u.Text
		starts[i] = placed{n: u.N, start: at}
		at += len(u.Text) + len("\n\n")
	}
	return s.scan(ctx, strings.Join(texts, "\n\n"), starts)
}

// placed is where one unit starts in the joined text.
type placed struct{ n, start int }

func (s *Scanner) scan(ctx context.Context, text string, units []placed) ([]store.Finding, []store.Span, error) {
	findings, spans := []store.Finding{}, []store.Span{}
	if text == "" {
		return findings, spans, nil
	}
	matches, err := yrScan(ctx, s.yr, s.compiled, map[string][]byte{"text": []byte(text)}, SpanKept, MatchesPerRule)
	if err != nil {
		return nil, nil, err
	}
	type found struct {
		finding store.Finding
		at      int
		span    string
	}
	var all []found
	for _, m := range matches {
		end := m.Offset + len(m.Printed)
		if m.Offset < 0 || end > len(text) || !bytes.Equal(m.Printed, []byte(text[m.Offset:end])) {
			return nil, nil, fmt.Errorf("yr named a match at offset %d that is not the text there", m.Offset)
		}
		f := store.Finding{
			Rule: cmp.Or(m.Meta["id"], m.Identifier), Severity: cmp.Or(m.Meta["severity"], "info"),
			Offset: m.Offset, Length: len(m.Printed) + m.More,
		}
		if len(units) > 0 {
			i, _ := slices.BinarySearchFunc(units, m.Offset, func(p placed, offset int) int { return cmp.Compare(p.start, offset+1) })
			unit := units[max(i-1, 0)]
			f.Unit, f.Offset = &unit.n, m.Offset-unit.start
		}
		all = append(all, found{finding: f, at: m.Offset, span: whole(text[m.Offset:end])})
	}
	slices.SortStableFunc(all, func(a, b found) int {
		return cmp.Or(cmp.Compare(a.at, b.at), cmp.Compare(a.finding.Rule, b.finding.Rule), cmp.Compare(b.finding.Length, a.finding.Length))
	})
	// Two patterns of one rule often match one place, such as a command
	// inside inline code, so a match that overlaps a kept one of its rule is
	// the same finding.
	perRule, keptUntil := map[string]int{}, map[string]int{}
	for _, f := range all {
		rule := f.finding.Rule
		if f.at < keptUntil[rule] {
			continue
		}
		keptUntil[rule] = f.at + f.finding.Length
		if perRule[rule]++; perRule[rule] > MatchesPerRule {
			continue
		}
		findings = append(findings, f.finding)
		spans = append(spans, store.Span{Rule: f.finding.Rule, Unit: f.finding.Unit, Span: f.span})
	}
	return findings, spans, nil
}

// whole is s without a last rune that SpanKept cut short, and with any other
// invalid UTF-8 replaced, so a span stored as text reads.
func whole(s string) string {
	for back := 1; back < utf8.UTFMax && back <= len(s); back++ {
		if utf8.RuneStart(s[len(s)-back]) {
			if !utf8.FullRuneInString(s[len(s)-back:]) {
				s = s[:len(s)-back]
			}
			break
		}
	}
	return strings.ToValidUTF8(s, string(utf8.RuneError))
}
