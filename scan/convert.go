package scan

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/datapointchris/ingestion/schemas"
)

// Fields are the fields of an agent's event whose conditions the scan keeps.
// An item's text is outsider-written content that a model reads, as a tool's
// response or a user's input is. A rule written only for a tool's arguments,
// its name or description, or an agent's own output is dropped, because an
// item's text plays none of those parts.
var Fields = []string{"content", "user_input", "tool_response"}

// Severities are what a rule's severity may be, from least to most severe,
// as the $defs/severity of schemas/ingestion-record.schema.json lists them.
var Severities = severities()

func severities() []string {
	var doc struct {
		Defs struct {
			Severity struct {
				Enum []string `json:"enum"`
			} `json:"severity"`
		} `json:"$defs"`
	}
	raw, err := schemas.Files.ReadFile("ingestion-record.schema.json")
	if err == nil {
		err = json.Unmarshal(raw, &doc)
	}
	if err != nil || len(doc.Defs.Severity.Enum) == 0 {
		panic(fmt.Sprintf("ingestion-record.schema.json lists no severity: %v", err))
	}
	return doc.Defs.Severity.Enum
}

// Why a rule is dropped. A decision's Detail says more, such as the fields it
// reads or the refusal under yr's caret.
const (
	DroppedSkippedByATR   = "ATR's engine skips a draft or deprecated rule"
	DroppedNeedsEvaluator = "it needs an evaluator other than patterns"
	DroppedOtherFields    = "it is written for fields the scan does not read"
	DroppedNotRegex       = "a condition is not a regular expression"
	DroppedUnreadable     = "its file is not a rule"
	DroppedNoSeverity     = "it names no severity the scan knows"
	DroppedNoExample      = "it has no example on the fields it keeps"
	DroppedMissesPositive = "it misses one of its own true positives"
	DroppedFiresNegative  = "it fires on one of its own true negatives"
	// droppedRefused prefixes yr's label under its caret, such as "backreferences
	// are not supported", so each kind of refusal counts apart. The label is
	// what differs between refusals, where yr's message is most often "invalid
	// regular expression".
	droppedRefused = "YARA-X refuses it: "
)

// Rule sources.
const (
	SourceATR       = "atr"
	SourceIngestion = "ingestion"
)

// Manifest is what a rules build wrote: the version every finding carries,
// and the decision on every rule it read. It is rules.json beside rules.yarc.
type Manifest struct {
	// Version names the rules: the ATR commit, the yr that compiled them,
	// and the SHA-256 of their YARA source, so any change to the converter,
	// ATR or ingestion's rules names a new version.
	Version   string         `json:"version"`
	ATRCommit string         `json:"atr_commit"`
	YaraX     string         `json:"yara_x"`
	Fields    []string       `json:"fields"`
	Kept      int            `json:"kept"`
	Dropped   map[string]int `json:"dropped"`
	Decisions []Decision     `json:"decisions"`
}

// Decision is whether one rule was kept, and why it was dropped where not.
type Decision struct {
	ID       string `json:"id"`
	Source   string `json:"source"`
	File     string `json:"file"`
	Severity string `json:"severity"`
	Kept     bool   `json:"kept"`
	Reason   string `json:"reason,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// Files in a built rules directory.
const (
	ManifestFile = "rules.json"
	CompiledFile = "rules.yarc"
	SourceFile   = "rules.yar"
)

// BuildConfig is one build of the scan's rules.
type BuildConfig struct {
	// ATRDir is a checkout of Agent Threat Rules at one commit. Its rules/
	// directory holds the rules.
	ATRDir string
	// ATRCommit names the commit checked out in ATRDir. Build reads it with
	// git where it is empty.
	ATRCommit string
	// IngestionRulesDir holds ingestion's own rules.
	IngestionRulesDir string
	Yr                string
	// OutDir receives rules.yar, rules.yarc and rules.json.
	OutDir string
}

// candidate is a rule that the converter turned into YARA, before yr and
// the rule's own examples have judged it.
type candidate struct {
	decision  *Decision
	rule      Rule
	source    string
	positives []string
	negatives []string
}

// Build converts ATR's rules and ingestion's own to YARA, keeps each one that
// yr compiles and that passes its own examples, and writes the kept rules,
// compiled, with a manifest of every decision. A rule is never patched by
// hand: one that yr refuses or that fails an example is dropped and counted
// by why.
func Build(ctx context.Context, c BuildConfig) (Manifest, error) {
	m := Manifest{Fields: Fields, Dropped: map[string]int{}}
	var err error
	if m.ATRCommit, err = atrCommit(ctx, c); err != nil {
		return m, err
	}
	if m.YaraX, err = yrVersion(ctx, c.Yr); err != nil {
		return m, err
	}
	atr, bad, err := readRules(filepath.Join(c.ATRDir, "rules"))
	if err != nil {
		return m, err
	}
	own, ownBad, err := readRules(c.IngestionRulesDir)
	if err != nil {
		return m, err
	}
	if len(ownBad) > 0 {
		return m, fmt.Errorf("ingestion's rule %s is not a rule: %w", ownBad[0].File, ownBad[0].Err)
	}
	var decisions []*Decision
	for _, u := range bad {
		decisions = append(decisions, &Decision{Source: SourceATR, File: u.File, Reason: DroppedUnreadable, Detail: u.Err.Error()})
	}
	seen := map[string]string{}
	var candidates []*candidate
	for _, group := range []struct {
		source string
		rules  []Rule
	}{{SourceATR, atr}, {SourceIngestion, own}} {
		for _, r := range group.rules {
			if held, dup := seen[r.ID]; dup {
				return m, fmt.Errorf("rules %s and %s both carry id %s", held, r.File, r.ID)
			}
			seen[r.ID] = r.File
			if group.source == SourceIngestion && !strings.HasPrefix(r.ID, "ingestion.") {
				return m, fmt.Errorf("ingestion's rule %s carries id %s, which does not start with %q", r.File, r.ID, "ingestion.")
			}
			d := &Decision{ID: r.ID, Source: group.source, File: r.File, Severity: r.Severity}
			decisions = append(decisions, d)
			if cand, reason, detail := convert(r, group.source); reason != "" {
				d.Reason, d.Detail = reason, detail
			} else {
				cand.decision = d
				candidates = append(candidates, cand)
			}
		}
	}
	if err := checkEach(ctx, c.Yr, candidates); err != nil {
		return m, err
	}
	compiled := slices.DeleteFunc(slices.Clone(candidates), func(cand *candidate) bool { return cand.decision.Reason != "" })
	if err := testExamples(ctx, c.Yr, compiled); err != nil {
		return m, err
	}
	var kept []*candidate
	for _, cand := range compiled {
		if cand.decision.Reason == "" {
			cand.decision.Kept = true
			kept = append(kept, cand)
		}
	}
	for _, cand := range candidates {
		if cand.decision.Source == SourceIngestion && !cand.decision.Kept {
			return m, fmt.Errorf("ingestion's rule %s is dropped: %s: %s", cand.rule.ID, cand.decision.Reason, cand.decision.Detail)
		}
	}
	for _, d := range decisions {
		m.Decisions = append(m.Decisions, *d)
		if d.Kept {
			m.Kept++
		} else {
			m.Dropped[d.Reason]++
		}
	}
	source := ruleSet(kept)
	sum := sha256.Sum256([]byte(source))
	m.Version = "atr@" + shortCommit(m.ATRCommit) + "+yr@" + m.YaraX + "+" + hex.EncodeToString(sum[:4])
	return m, write(ctx, c, m, source)
}

func atrCommit(ctx context.Context, c BuildConfig) (string, error) {
	if c.ATRCommit != "" {
		return c.ATRCommit, nil
	}
	out, err := exec.CommandContext(ctx, "git", "-C", c.ATRDir, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", fmt.Errorf("read the commit that %s holds: %w", c.ATRDir, err)
	}
	return strings.TrimSpace(string(out)), nil
}

func shortCommit(commit string) string {
	if len(commit) > 8 {
		return commit[:8]
	}
	return commit
}

// convert turns r into a YARA rule over the conditions it writes for Fields,
// and answers why not where it cannot. An ATR pattern is JavaScript's dialect,
// which yaraRegex carries across. Ingestion's own patterns are written in
// YARA-X's, and go through as written.
func convert(r Rule, source string) (*candidate, string, string) {
	switch r.Status {
	case "draft", "deprecated":
		return nil, DroppedSkippedByATR, "status " + r.Status
	}
	method := cmp.Or(r.Detection.Method, "pattern")
	if method == "semantic" && r.Detection.Semantic.FallbackMethod == "pattern" {
		method = "pattern"
	}
	if method != "pattern" {
		return nil, DroppedNeedsEvaluator, "method " + method
	}
	if !slices.Contains(Severities, r.Severity) {
		return nil, DroppedNoSeverity, "severity " + strconv.Quote(r.Severity)
	}
	var kept []Condition
	others := map[string]bool{}
	for _, cond := range r.Detection.Conditions {
		if cond.Operator != "regex" {
			return nil, DroppedNotRegex, "operator " + cond.Operator
		}
		if slices.Contains(Fields, cond.Field) {
			kept = append(kept, cond)
		} else {
			others[cond.Field] = true
		}
	}
	condition := cmp.Or(r.Detection.Condition, "any")
	switch {
	case condition != "any" && condition != "all":
		return nil, DroppedNeedsEvaluator, "condition " + condition
	case len(kept) == 0:
		return nil, DroppedOtherFields, "reads " + strings.Join(slices.Sorted(maps.Keys(others)), ", ")
	case condition == "all" && len(others) > 0:
		return nil, DroppedOtherFields, "every condition must hold, and one reads " + strings.Join(slices.Sorted(maps.Keys(others)), ", ")
	}
	cand := &candidate{rule: r}
	var b strings.Builder
	fmt.Fprintf(&b, "rule %s {\n  meta:\n    id = %s\n    severity = %s\n", identifier(r.ID), yaraString(r.ID), yaraString(r.Severity))
	if r.Tags.SuppressInCodeBlocks {
		fmt.Fprintf(&b, "    %s = \"true\"\n", suppressMeta)
	}
	b.WriteString("  strings:\n")
	for i, cond := range kept {
		pattern := cond.Value
		if source == SourceATR {
			pattern = yaraRegex(pattern)
		}
		fmt.Fprintf(&b, "    $p%d = /%s/ nocase\n", i, pattern)
	}
	fmt.Fprintf(&b, "  condition:\n    %s of them\n}\n", condition)
	cand.source = b.String()
	for _, tc := range r.TestCases.TruePositives {
		if text, ok := caseText(tc); ok && fires(tc, true) {
			cand.positives = append(cand.positives, text)
		} else if ok {
			cand.negatives = append(cand.negatives, text)
		}
	}
	for _, tc := range r.TestCases.TrueNegatives {
		if text, ok := caseText(tc); ok && !fires(tc, false) {
			cand.negatives = append(cand.negatives, text)
		} else if ok {
			cand.positives = append(cand.positives, text)
		}
	}
	if len(cand.positives) == 0 {
		return nil, DroppedNoExample, "no true positive names its text under input, " + strings.Join(Fields, ", ")
	}
	return cand, "", ""
}

// fires is whether a case expects its rule to fire: its expected value where
// it names one, and inPositives where it names neither verdict.
func fires(tc map[string]any, inPositives bool) bool {
	switch tc["expected"] {
	case "triggered", "trigger":
		return true
	case "not_triggered", "no_trigger":
		return false
	}
	return inPositives
}

// caseText is a case's text for the fields that the scan reads. It is false
// where the case names its text only under another field, such as tool_args.
// ATR's harness offers a bare input to every field.
func caseText(tc map[string]any) (string, bool) {
	for _, key := range []string{"input", "user_input", "content", "tool_response"} {
		switch v := tc[key].(type) {
		case string:
			if v != "" {
				return v, true
			}
		case map[string]any:
			for _, inner := range []string{"tool_response", "response"} {
				if s, ok := v[inner].(string); ok && s != "" {
					return s, true
				}
			}
		}
	}
	return "", false
}

// identifier is id as a YARA rule identifier.
func identifier(id string) string {
	var b strings.Builder
	b.WriteString("r_")
	for _, r := range id {
		if r < utf8.RuneSelf && (r == '_' || ('0' <= r && r <= '9') || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')) {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// yaraString is s as a YARA text string.
func yaraString(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\t", `\t`).Replace(s) + `"`
}

// yaraRegex is an ATR pattern as the body of a YARA regular expression.
// ATR's engine drops a leading flag group and matches every pattern without
// regard to case. So the flags go, the string carries nocase, and each letter
// outside ASCII is written in lower case, as each of the scan's views writes
// the text's. An unescaped slash is escaped, because YARA delimits a regular
// expression with slashes. A control character is written as its escape,
// because a YARA regular expression is one line. JavaScript reads \xE9 as the
// character é, where YARA-X reads it as one byte, so an escape above \x7F is
// written as the character's \u escape. YARA-X's dot matches one byte, so a
// dot under a quantifier other than * or + is written as one UTF-8
// character.
//
// Every quantifier is made greedy. ATR's engine asks only whether a pattern
// matches, never where, and whether a match exists does not depend on how
// greedy its quantifiers are. YARA-X refuses a pattern that mixes the two.
//
// The dialects still differ. A negated class, \S, \W and \D each match one
// byte of a character outside ASCII, where JavaScript's match the whole
// character. YARA-X's \s is ASCII whitespace, where JavaScript's holds
// Unicode's spaces too, most of which the normalized view writes as a space.
// YARA-X refuses a class that holds a character outside ASCII, so such a rule
// is dropped.
func yaraRegex(value string) string {
	value = leadingFlags.ReplaceAllString(value, "")
	var b strings.Builder
	inClass := false
	quantified := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		wasQuantifier := quantified
		quantified = false
		switch {
		case c == '\\' && i+1 < len(value):
			i = writeEscape(&b, value, i, inClass)
		case c == '/':
			b.WriteString(`\/`)
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		case inClass:
			b.WriteByte(c)
			inClass = c != ']'
		case c >= utf8.RuneSelf:
			r, size := utf8.DecodeRuneInString(value[i:])
			b.WriteRune(unicode.ToLower(r))
			i += size - 1
		case c == '[':
			b.WriteByte(c)
			inClass = true
			if strings.HasPrefix(value[i+1:], "^") {
				b.WriteByte('^')
				i++
			}
		case c == '.' && (i+1 == len(value) || (value[i+1] != '*' && value[i+1] != '+')):
			b.WriteString(utf8Character)
		case c == '(' && i+1 < len(value) && value[i+1] == '?':
			b.WriteString("(?")
			i++
		case c == '?' && wasQuantifier:
		case c == '*' || c == '+' || c == '?':
			b.WriteByte(c)
			quantified = true
		case c == '{':
			if n := boundedRepeat.FindString(value[i:]); n != "" {
				b.WriteString(n)
				i += len(n) - 1
				quantified = true
			} else {
				b.WriteByte(c)
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// utf8Character is one UTF-8 character other than a newline, as YARA-X's
// byte-wise regular expressions write it.
const utf8Character = `(?:[\x00-\x09\x0b-\x7f]|[\xc2-\xdf][\x80-\xbf]|[\xe0-\xef][\x80-\xbf]{2}|[\xf0-\xf4][\x80-\xbf]{3})`

// boundedRepeat is a quantifier such as {3}, {2,} or {0,30}.
var boundedRepeat = regexp.MustCompile(`^\{\d+(,\d*)?\}`)

// writeEscape writes the escape that starts at value[i] and answers the index
// of its last byte. An escape of a character above \x7F, as \xE9, \u00E9 or
// \u{E9}, is written as that character's \u escape, in lower case outside a
// class.
func writeEscape(b *strings.Builder, value string, i int, inClass bool) int {
	next := value[i+1]
	switch {
	case next == 'x' && i+3 < len(value) && isHex(value[i+2]) && isHex(value[i+3]) && value[i+2] >= '8':
		return writeRuneEscape(b, value[i+2:i+4], inClass, i+3)
	case next == 'u' && i+5 < len(value) && isHex(value[i+2]) && isHex(value[i+3]) && isHex(value[i+4]) && isHex(value[i+5]):
		return writeRuneEscape(b, value[i+2:i+6], inClass, i+5)
	case next == 'u' && i+2 < len(value) && value[i+2] == '{':
		if end := strings.IndexByte(value[i:], '}'); end > 0 {
			if _, err := strconv.ParseUint(value[i+3:i+end], 16, 32); err == nil {
				return writeRuneEscape(b, value[i+3:i+end], inClass, i+end)
			}
			b.WriteString(value[i : i+end+1])
			return i + end
		}
	case next < 0x20 || next == 0x7f:
		fmt.Fprintf(b, `\x%02x`, next)
		return i + 1
	}
	b.WriteByte('\\')
	b.WriteByte(next)
	return i + 1
}

// writeRuneEscape writes the character whose code point hex names as a \u
// escape, and answers last.
func writeRuneEscape(b *strings.Builder, hex string, inClass bool, last int) int {
	n, _ := strconv.ParseUint(hex, 16, 32)
	r := rune(n)
	if !inClass && r >= utf8.RuneSelf {
		r = unicode.ToLower(r)
	}
	if r <= 0xFFFF {
		fmt.Fprintf(b, `\u%04x`, r)
	} else {
		fmt.Fprintf(b, `\u{%x}`, r)
	}
	return last
}

// leadingFlags is a leading inline flag group, such as (?i) or (?is), as ATR's
// engine strips it.
var leadingFlags = regexp.MustCompile(`^\(\?[imsx]+\)`)

func isHex(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')
}

// checkEach compiles each candidate alone, so a rule yr refuses is dropped
// with yr's own reason and the rest still build.
func checkEach(ctx context.Context, yr string, candidates []*candidate) error {
	dir, err := os.MkdirTemp("", "ingestion-rules-check-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	var wg sync.WaitGroup
	limit := make(chan struct{}, runtime.GOMAXPROCS(0))
	errs := make([]error, len(candidates))
	for i, cand := range candidates {
		wg.Go(func() {
			limit <- struct{}{}
			defer func() { <-limit }()
			err := yrCompile(ctx, yr, cand.source, filepath.Join(dir, strconv.Itoa(i)+".yarc"))
			var refused yrRefusal
			switch {
			case err == nil:
			case asRefusal(err, &refused):
				cand.decision.Reason, cand.decision.Detail = droppedRefused+cmp.Or(refused.Detail, refused.Message), refused.Message
			default:
				errs[i] = fmt.Errorf("compile %s: %w", cand.rule.ID, err)
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func asRefusal(err error, into *yrRefusal) bool {
	r, ok := err.(yrRefusal)
	if ok {
		*into = r
	}
	return ok
}

// testExamples compiles the candidates together and scans every example
// once, through the views that a scan reads. A candidate that misses a true
// positive or fires on a true negative is dropped with the case that showed
// it.
func testExamples(ctx context.Context, yr string, candidates []*candidate) error {
	if len(candidates) == 0 {
		return nil
	}
	dir, err := os.MkdirTemp("", "ingestion-rules-test-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	compiled := filepath.Join(dir, CompiledFile)
	if err := yrCompile(ctx, yr, ruleSet(candidates), compiled); err != nil {
		return fmt.Errorf("compile the rules that each compiled alone: %w", err)
	}
	texts := map[string]string{}
	for i, cand := range candidates {
		for j, text := range cand.positives {
			texts[fmt.Sprintf("%d-p-%d", i, j)] = text
		}
		for j, text := range cand.negatives {
			texts[fmt.Sprintf("%d-n-%d", i, j)] = text
		}
	}
	matches, err := matchTexts(ctx, yr, compiled, texts, 1, 1)
	if err != nil {
		return err
	}
	fired := map[string]map[string]bool{}
	for _, m := range matches {
		if fired[m.text] == nil {
			fired[m.text] = map[string]bool{}
		}
		fired[m.text][m.rule] = true
	}
	for i, cand := range candidates {
		for j := range cand.positives {
			if !fired[fmt.Sprintf("%d-p-%d", i, j)][cand.rule.ID] {
				cand.decision.Reason, cand.decision.Detail = DroppedMissesPositive, fmt.Sprintf("true positive %d of %d", j+1, len(cand.positives))
				break
			}
		}
		if cand.decision.Reason != "" {
			continue
		}
		for j := range cand.negatives {
			if fired[fmt.Sprintf("%d-n-%d", i, j)][cand.rule.ID] {
				cand.decision.Reason, cand.decision.Detail = DroppedFiresNegative, fmt.Sprintf("true negative %d of %d", j+1, len(cand.negatives))
				break
			}
		}
	}
	return nil
}

// ruleSet is the YARA source of candidates, in their order.
func ruleSet(candidates []*candidate) string {
	var b strings.Builder
	for _, cand := range candidates {
		b.WriteString(cand.source)
		b.WriteByte('\n')
	}
	return b.String()
}

// write writes the kept rules' source, their compiled form and the manifest
// into c.OutDir.
func write(ctx context.Context, c BuildConfig, m Manifest, source string) error {
	if err := os.MkdirAll(c.OutDir, 0o755); err != nil {
		return err
	}
	out, err := filepath.Abs(c.OutDir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, SourceFile), []byte(source), 0o644); err != nil {
		return err
	}
	if err := yrCompile(ctx, c.Yr, source, filepath.Join(out, CompiledFile)); err != nil {
		return fmt.Errorf("compile the kept rules: %w", err)
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, ManifestFile), append(raw, '\n'), 0o644)
}
