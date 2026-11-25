package scan

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/datapointchris/ingestion/isolatedprocess"
)

// yrRefusal is what yr compile printed where it refused a rule: its message,
// such as "invalid regular expression", and the detail under the caret, such
// as "look-around ... is not supported".
type yrRefusal struct {
	Message string
	Detail  string
}

func (r yrRefusal) Error() string {
	if r.Detail == "" {
		return r.Message
	}
	return r.Message + ": " + r.Detail
}

var (
	yrError  = regexp.MustCompile(`(?m)^error(?:\[E\d+\])?: (.+)$`)
	yrDetail = regexp.MustCompile(`(?m)^\s*\|\s*(?:[\^\-~]+\s+)+([^\s\^\-~|].*)$`)
)

// parseRefusal reads the first error that yr printed to stderr.
func parseRefusal(stderr string) yrRefusal {
	m := yrError.FindStringSubmatch(stderr)
	if m == nil {
		return yrRefusal{Message: strings.TrimSpace(firstLine(stderr))}
	}
	r := yrRefusal{Message: strings.TrimSpace(m[1])}
	if d := yrDetail.FindStringSubmatch(stderr); d != nil {
		r.Detail = strings.TrimSpace(d[1])
	}
	return r
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}

// yrCompile compiles source to out, an absolute path. A rule that yr refuses
// answers a yrRefusal.
func yrCompile(ctx context.Context, yr, source, out string) error {
	stderr, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
		Path: yr, Args: []string{"compile", "--disable-warnings", "-o", out, "rules.yar"},
		Files: map[string][]byte{"rules.yar": []byte(source)},
	})
	var exit *isolatedprocess.ExitError
	if errors.As(err, &exit) {
		return parseRefusal(stderr)
	}
	return err
}

// yrVersion is the version that yr --version prints, such as 1.24.0.
func yrVersion(ctx context.Context, yr string) (string, error) {
	var out bytes.Buffer
	_, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
		Path: yr, Args: []string{"--version"},
		Stdout: func(r io.Reader) error { _, err := io.Copy(&out, r); return err },
	})
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out.String())
	if len(fields) == 0 {
		return "", fmt.Errorf("%s --version printed nothing", yr)
	}
	return fields[len(fields)-1], nil
}

// yrMatch is one string's match, as yr scan prints each.
type yrMatch struct {
	File       string
	Identifier string
	Meta       map[string]string
	Offset     int
	// Printed is the match's bytes as yr printed them, up to the limit
	// that the scan names, and More counts the bytes past that limit.
	Printed []byte
	More    int
}

// yrLine is one line of yr scan --output-format ndjson.
type yrLine struct {
	Path  string `json:"path"`
	Rules []struct {
		Identifier string     `json:"identifier"`
		Meta       [][]string `json:"meta"`
		Strings    []struct {
			Offset int    `json:"offset"`
			Match  string `json:"match"`
		} `json:"strings"`
	} `json:"rules"`
}

// yrScan scans files, each written under its name, with the compiled rules
// at rules, an absolute path, and answers every match. It prints at most
// printed bytes of each match. yr exits 0 where it cannot read a file, and
// says so only on stderr, so anything on stderr fails the scan.
func yrScan(ctx context.Context, yr, rules string, files map[string][]byte, printed, perPattern int) ([]yrMatch, error) {
	var matches []yrMatch
	stderr, err := isolatedprocess.Run(ctx, isolatedprocess.Command{
		Path: yr,
		Args: []string{
			"scan", "--compiled-rules", "--output-format", "ndjson", "--print-meta",
			"--print-strings=" + strconv.Itoa(printed), "--max-matches-per-pattern", strconv.Itoa(perPattern),
			"--disable-console-logs", rules, ".",
		},
		Files: files,
		Stdout: func(r io.Reader) error {
			var err error
			matches, err = readYrLines(r, printed)
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	if stderr != "" {
		return nil, fmt.Errorf("yr scan: %s", firstLine(stderr))
	}
	return matches, nil
}

func readYrLines(r io.Reader, printed int) ([]yrMatch, error) {
	lines := bufio.NewScanner(r)
	lines.Buffer(make([]byte, 64<<10), 256<<20)
	var matches []yrMatch
	for lines.Scan() {
		if len(bytes.TrimSpace(lines.Bytes())) == 0 {
			continue
		}
		var line yrLine
		if err := json.Unmarshal(lines.Bytes(), &line); err != nil {
			return nil, fmt.Errorf("a line that is not a scan result: %w", err)
		}
		file := strings.TrimPrefix(line.Path, "./")
		for _, rule := range line.Rules {
			meta := map[string]string{}
			for _, pair := range rule.Meta {
				if len(pair) == 2 {
					meta[pair[0]] = pair[1]
				}
			}
			for _, s := range rule.Strings {
				match, more, err := unescapeMatch(s.Match, printed)
				if err != nil {
					return nil, err
				}
				matches = append(matches, yrMatch{
					File: file, Identifier: rule.Identifier, Meta: meta, Offset: s.Offset, Printed: match, More: more,
				})
			}
		}
	}
	return matches, lines.Err()
}

var truncated = regexp.MustCompile(` \.\.\. (\d+) more bytes$`)

// unescapeMatch is a match as yr printed it, back as its bytes, and the count
// of bytes that yr left off past printed. yr escapes a backslash, a quote, a
// tab, a newline and every byte outside printable ASCII.
func unescapeMatch(s string, printed int) ([]byte, int, error) {
	if m := truncated.FindStringSubmatchIndex(s); m != nil {
		n, _ := strconv.Atoi(s[m[2]:m[3]])
		head, err := unescape(s[:m[0]])
		if err == nil && len(head) == printed {
			return head, n, nil
		}
	}
	out, err := unescape(s)
	return out, 0, err
}

func unescape(s string) ([]byte, error) {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			out = append(out, s[i])
			continue
		}
		i++
		if i >= len(s) {
			return nil, fmt.Errorf("a match that ends in a lone backslash")
		}
		switch s[i] {
		case '\\', '"', '\'':
			out = append(out, s[i])
		case 'n':
			out = append(out, '\n')
		case 't':
			out = append(out, '\t')
		case 'r':
			out = append(out, '\r')
		case '0':
			out = append(out, 0)
		case 'x':
			if i+2 >= len(s) {
				return nil, fmt.Errorf("a match with a short \\x escape")
			}
			b, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
			if err != nil {
				return nil, fmt.Errorf("a match with an escape \\x%s: %w", s[i+1:i+3], err)
			}
			out = append(out, byte(b))
			i += 2
		default:
			return nil, fmt.Errorf("a match with an escape yr does not print: \\%c", s[i])
		}
	}
	return out, nil
}
