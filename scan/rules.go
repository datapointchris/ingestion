package scan

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"go.yaml.in/yaml/v3"
)

// Rule is one rule in Agent Threat Rules' YAML shape. ATR's rules and
// ingestion's own are both read this way, and both carry their own examples.
type Rule struct {
	ID       string `yaml:"id"`
	Title    string `yaml:"title"`
	Status   string `yaml:"status"`
	Severity string `yaml:"severity"`
	// Suggests is the kind of thing that a match lists for a reader to run,
	// open or paste: command, link or code. It is empty for a rule whose
	// match flags an attack.
	Suggests  string    `yaml:"suggests"`
	Tags      Tags      `yaml:"tags"`
	Detection Detection `yaml:"detection"`
	TestCases TestCases `yaml:"test_cases"`
	// File is the rule's path below the directory it was read from.
	File string `yaml:"-"`
}

// Tags are what ATR's engine reads of a rule's tags.
type Tags struct {
	// SuppressInCodeBlocks drops a match inside markdown code: a fenced
	// block, inline code, or a quoted string on a table's row.
	SuppressInCodeBlocks bool `yaml:"suppress_in_code_blocks"`
}

// Detection is how a rule matches: Condition is any or all of Conditions.
type Detection struct {
	// Method is pattern where empty. ATR also writes semantic, trace and
	// behavioral, each needing an evaluator other than patterns.
	Method     string      `yaml:"method"`
	Condition  string      `yaml:"condition"`
	Conditions []Condition `yaml:"conditions"`
	Semantic   struct {
		FallbackMethod string `yaml:"fallback_method"`
	} `yaml:"semantic"`
}

// Condition is one pattern and the field of an agent's event it reads.
type Condition struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"`
	Value    string `yaml:"value"`
}

// TestCases are a rule's examples. Each case names its text under input or
// under the field it is written for, and says whether the rule fires.
type TestCases struct {
	TruePositives []map[string]any `yaml:"true_positives"`
	TrueNegatives []map[string]any `yaml:"true_negatives"`
}

// unreadable is a file under a rules directory that is no rule.
type unreadable struct {
	File string
	Err  error
}

// readRules reads every .yaml and .yml file below dir as one rule each, in
// path order, and answers each file it could not read as a rule apart.
func readRules(dir string) ([]Rule, []unreadable, error) {
	var paths []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && slices.Contains([]string{".yaml", ".yml"}, filepath.Ext(path)) {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("read the rules under %s: %w", dir, err)
	}
	slices.Sort(paths)
	var rules []Rule
	var bad []unreadable
	for _, path := range paths {
		rel, _ := filepath.Rel(dir, path)
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, err
		}
		var r Rule
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		if err := decoder.Decode(&r); err != nil {
			bad = append(bad, unreadable{File: rel, Err: err})
			continue
		}
		if r.ID == "" {
			bad = append(bad, unreadable{File: rel, Err: fmt.Errorf("it names no id")})
			continue
		}
		r.File = rel
		rules = append(rules, r)
	}
	return rules, bad, nil
}
