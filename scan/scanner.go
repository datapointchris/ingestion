// Package scan is the scan stage. It runs VirusTotal's YARA-X, as the yr
// program, over an item's parsed text with Agent Threat Rules converted to
// YARA and ingestion's own rules, and stores each finding: the rule, its
// severity and the text it matched. A finding never drops an item.
//
// The rules are built once per image by build-rules, which keeps each
// converted rule only where yr compiles it and it passes its own examples.
// The findings carry the rules' version, so a new rule set replays beside
// the old.
package scan

import (
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
	// Producer names each scan output. Its version is the rules' version and
	// the parse it read, as store.ScanVersion writes them.
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

// Scan scans one text that belongs to no parse, and answers each finding and
// its span in the order they sit in the text. No finding names a unit, and
// each offset counts from the text's start.
func (s *Scanner) Scan(ctx context.Context, text string) ([]store.Finding, []store.Span, error) {
	return s.scan(ctx, text, nil, nil)
}

// ScanUnits scans a parse's units as the one text that store.JoinUnits makes
// of them, so a rule reads across them as it would read a page. Each finding
// names the unit it starts in, and its offset counts from that unit's start.
func (s *Scanner) ScanUnits(ctx context.Context, units []store.Unit) ([]store.Finding, []store.Span, error) {
	text, starts := store.JoinUnits(units)
	numbers := make([]int, len(units))
	for i, u := range units {
		numbers[i] = u.N
	}
	return s.scan(ctx, text, starts, numbers)
}

func (s *Scanner) scan(ctx context.Context, text string, starts, numbers []int) ([]store.Finding, []store.Span, error) {
	findings, spans := []store.Finding{}, []store.Span{}
	matches, err := matchTexts(ctx, s.yr, s.compiled, map[string]string{"text": text}, SpanKept, MatchesPerRule)
	if err != nil {
		return nil, nil, err
	}
	type found struct {
		finding store.Finding
		at      int
		span    string
	}
	all := make([]found, 0, len(matches))
	for _, m := range matches {
		f := store.Finding{Rule: m.rule, Severity: cmp.Or(m.severity, Severities[0]), Offset: m.start, Length: m.end - m.start}
		if len(starts) > 0 {
			i, _ := slices.BinarySearch(starts, m.start+1)
			i = max(i-1, 0)
			f.Unit, f.Offset = &numbers[i], m.start-starts[i]
		}
		all = append(all, found{finding: f, at: m.start, span: whole(text[m.start:min(m.end, m.start+SpanKept)])})
	}
	slices.SortStableFunc(all, func(a, b found) int {
		return cmp.Or(cmp.Compare(a.at, b.at), cmp.Compare(a.finding.Rule, b.finding.Rule), cmp.Compare(b.finding.Length, a.finding.Length))
	})
	// Two patterns of one rule, or one pattern in both of a text's views,
	// often match one place, so a match that overlaps a kept one of its rule
	// is the same finding.
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
		spans = append(spans, store.Span{Rule: f.finding.Rule, Span: f.span})
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
