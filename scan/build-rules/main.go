// Command build-rules converts Agent Threat Rules and ingestion's own rules to
// YARA, keeps each rule that yr compiles and that passes its own examples, and
// writes the compiled rules with a manifest of every decision. The worker
// image's build runs it, and prints the kept and dropped counts by reason.
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"

	"github.com/datapointchris/ingestion/scan"
)

func main() {
	atr := flag.String("atr", "", "a checkout of Agent Threat Rules at the commit to convert")
	atrCommit := flag.String("atr-commit", "", "the commit that --atr holds (read with git where unset)")
	own := flag.String("ingestion-rules", filepath.Join("scan", "ingestion-rules"), "the directory of ingestion's own rules")
	yr := flag.String("yr", "yr", "the yr binary")
	out := flag.String("out", "", "the directory to write rules.yar, rules.yarc and rules.json into")
	flag.Parse()
	if *atr == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "build-rules: --atr and --out name directories")
		flag.Usage()
		os.Exit(2)
	}
	path, err := exec.LookPath(*yr)
	if err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	m, err := scan.Build(ctx, scan.BuildConfig{ATRDir: *atr, ATRCommit: *atrCommit, IngestionRulesDir: *own, Yr: path, OutDir: *out})
	if err != nil {
		fail(err)
	}
	fmt.Printf("rules %s, from ATR %s and ingestion's own, compiled by yara-x %s\n", m.Version, m.ATRCommit, m.YaraX)
	fmt.Printf("kept %d of %d rules\n", m.Kept, len(m.Decisions))
	reasons := slices.SortedFunc(maps.Keys(m.Dropped), func(a, b string) int {
		return cmp.Or(cmp.Compare(m.Dropped[b], m.Dropped[a]), cmp.Compare(a, b))
	})
	for _, reason := range reasons {
		fmt.Printf("dropped %4d  %s\n", m.Dropped[reason], reason)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "build-rules:", err)
	os.Exit(1)
}
