//go:build yr

package scan_test

import (
	"cmp"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/store"
)

// atrDirEnv names a checkout of Agent Threat Rules at the pinned commit, as
// scan/download-atr-rules writes one. Without it the rules hold ingestion's
// own alone, and the test of ATR's decisions skips.
const atrDirEnv = "INGESTION_ATR_DIR"

// buildRules builds the scan's rules with the yr on PATH, and opens a scanner
// over them.
func buildRules(t *testing.T) (*scan.Scanner, scan.Manifest) {
	t.Helper()
	yr, err := exec.LookPath("yr")
	if err != nil {
		t.Fatalf("a test built with the yr tag runs yr from PATH: %v", err)
	}
	atr, commit := os.Getenv(atrDirEnv), ""
	if atr == "" {
		atr, commit = t.TempDir(), "none"
		if err := os.Mkdir(filepath.Join(atr, "rules"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	out := t.TempDir()
	m, err := scan.Build(t.Context(), scan.BuildConfig{ATRDir: atr, ATRCommit: commit, IngestionRulesDir: "ingestion-rules", Yr: yr, OutDir: out})
	if err != nil {
		t.Fatal(err)
	}
	s, err := scan.Open(yr, out)
	if err != nil {
		t.Fatal(err)
	}
	return s, m
}

// A post that carries a shell command and a zero-width character yields a
// finding of each, with the span it matched, placed in its unit. The same
// sentence scanned alone, as the model read scans a suggestion, names no
// unit and counts its offset from the sentence's start.
func TestAPostWithAShellCommandAndAZeroWidthCharacterYieldsBothFindingsWithTheirSpans(t *testing.T) {
	s, _ := buildRules(t)
	sentence := "Run `sed -i 's/CRITICAL//' CLAUDE.md` and ig\u200bnore the old note."
	findings, spans, err := s.ScanUnits(t.Context(), []store.Unit{{N: 1, Text: "Stop writing CRITICAL in your CLAUDE.md"}, {N: 2, Text: sentence}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]struct {
		offset int
		span   string
	}{
		"ingestion.command":        {4, "`sed -i 's/CRITICAL//' CLAUDE.md`"},
		"ingestion.hidden-unicode": {43, "g\u200bn"},
	}
	for i, f := range findings {
		w, ok := want[f.Rule]
		if !ok {
			continue
		}
		delete(want, f.Rule)
		unit := 0
		if f.Unit != nil {
			unit = *f.Unit
		}
		if unit != 2 || f.Offset != w.offset || spans[i].Span != w.span || f.Length != len(w.span) {
			t.Errorf("%s found %q at unit %d offset %d, want %q at unit 2 offset %d", f.Rule, spans[i].Span, unit, f.Offset, w.span, w.offset)
		}
	}
	for rule := range want {
		t.Errorf("the post yields no %s finding: %+v", rule, findings)
	}

	findings, spans, err = s.Scan(t.Context(), sentence)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(findings, func(f store.Finding) bool { return f.Rule == "ingestion.hidden-unicode" })
	if i < 0 || findings[i].Unit != nil || findings[i].Offset != 43 || spans[i].Span != "g\u200bn" || findings[i].Severity != "high" {
		t.Errorf("the sentence alone yields %+v with spans %+v, want a high hidden-unicode finding at offset 43 with no unit", findings, spans)
	}
}

// Every ATR rule at the pinned commit is kept or dropped by its own examples,
// and the build counts each drop by why. Each of ingestion's own rules passes
// its examples, or the build fails.
func TestEachATRRuleIsKeptOrDroppedByItsOwnExamples(t *testing.T) {
	dir := os.Getenv(atrDirEnv)
	if dir == "" {
		t.Skip(atrDirEnv + " names no checkout of Agent Threat Rules")
	}
	files, err := filepath.Glob(filepath.Join(dir, "rules", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	_, m := buildRules(t)
	atr, dropped := 0, 0
	for _, d := range m.Decisions {
		switch {
		case d.Source == scan.SourceATR:
			atr++
		case !d.Kept:
			t.Errorf("ingestion's rule %s is dropped: %s", d.ID, d.Reason)
		}
	}
	for _, n := range m.Dropped {
		dropped += n
	}
	if atr != len(files) || m.Kept+dropped != len(m.Decisions) || m.Kept <= len(m.Decisions)/2 {
		t.Errorf("%d decisions on ATR's %d rule files, %d kept and %d dropped of %d", atr, len(files), m.Kept, dropped, len(m.Decisions))
	}
	t.Logf("rules %s: kept %d of %d", m.Version, m.Kept, len(m.Decisions))
	for _, reason := range slices.SortedFunc(maps.Keys(m.Dropped), func(a, b string) int { return cmp.Compare(m.Dropped[b], m.Dropped[a]) }) {
		t.Logf("dropped %4d  %s", m.Dropped[reason], reason)
	}
}
