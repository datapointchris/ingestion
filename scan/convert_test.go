package scan

import "testing"

// An ATR pattern keeps its meaning in YARA-X's dialect. Its flags go, a slash
// and a control character are escaped, JavaScript's \xE9 becomes the
// character é, and every quantifier is greedy, while a group's (? and a
// \u{…} escape's braces are left as they are.
func TestAnATRPatternCarriesItsMeaningIntoYARA(t *testing.T) {
	for pattern, want := range map[string]string{
		`(?i)ignore\s+previous`:      `ignore\s+previous`,
		`https?://evil\.com/x`:       `https?:\/\/evil\.com\/x`,
		"a\tb":                       `a\x09b`,
		`r\xe9sum\xe9|\x41`:          `r\u00e9sum\u00e9|\x41`,
		`a.*?b.+c{2,5}?d??(?:e)*?`:   `a.*b.+c{2,5}d?(?:e)*`,
		`(?<name>x)(?=y)[*?+]?`:      `(?<name>x)(?=y)[*?+]?`,
		`\u{1F600}?x\{3\}?`:          `\u{1F600}?x\{3\}?`,
		`[^/]{0,30}?\/`:              `[^\/]{0,30}\/`,
		`[?]+?`:                      `[?]+`,
		`(?is)\bsystem\s*prompt\b`:   `\bsystem\s*prompt\b`,
		`price: \$\d+(\.\d{2})?\s*$`: `price: \$\d+(\.\d{2})?\s*$`,
	} {
		if got := yaraRegex(pattern); got != want {
			t.Errorf("yaraRegex(%q) = %q, want %q", pattern, got, want)
		}
	}
}
