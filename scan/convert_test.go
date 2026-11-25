package scan

import "testing"

// An ATR pattern keeps its meaning in YARA-X's dialect. Its flags go, a slash
// and a control character are escaped, JavaScript's \xE9 becomes the
// character é, each letter outside ASCII is in lower case outside a
// class, a dot under a bounded count is one UTF-8 character, and every
// quantifier is greedy, while a group's (? is left as it is.
func TestAnATRPatternCarriesItsMeaningIntoYARA(t *testing.T) {
	for pattern, want := range map[string]string{
		`(?i)ignore\s+previous`:      `ignore\s+previous`,
		`https?://evil\.com/x`:       `https?:\/\/evil\.com\/x`,
		"a\tb":                       `a\x09b`,
		`r\xe9sum\xe9|\x41`:          `r\u00e9sum\u00e9|\x41`,
		`a.*?b.+c{2,5}?d??(?:e)*?`:   `a.*b.+c{2,5}d?(?:e)*`,
		`(?<name>x)(?=y)[*?+]?`:      `(?<name>x)(?=y)[*?+]?`,
		`\u{1F600}?x\{3\}?`:          `\u{1f600}?x\{3\}?`,
		`\xC9t\u0414[\xC9\u0414]`:    `\u00e9t\u0434[\u00c9\u0414]`,
		"\xd0\x94\xd0\x9e\\s+":       "\xd0\xb4\xd0\xbe\\s+",
		`a.{0,4}b.?c\.d.*e.+`:        "a" + utf8Character + "{0,4}b" + utf8Character + `?c\.d.*e.+`,
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
