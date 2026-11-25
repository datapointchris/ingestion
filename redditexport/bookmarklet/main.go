// Command bookmarklet writes the page that installs the Reddit export
// bookmarklet: a link to drag onto the bookmarks bar, and the same script to
// paste into the console. A bookmark editor can refuse a typed javascript:
// URL, and a dragged link is saved as written.
package main

import (
	_ "embed"
	"fmt"
	"html"
	"os"
	"strings"
)

//go:embed reddit-export.js
var script string

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./redditexport/bookmarklet <page.html>")
		os.Exit(2)
	}
	source := strings.TrimSpace(script)
	page := fmt.Sprintf(pageTemplate, html.EscapeString("javascript:"+encodeURIComponent(source)), html.EscapeString(source))
	if err := os.WriteFile(os.Args[1], []byte(page), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote", os.Args[1])
}

// encodeURIComponent escapes s as JavaScript's function of that name does, so
// the link runs the script byte for byte.
func encodeURIComponent(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', strings.IndexByte("-_.!~*'()", c) >= 0:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

const pageTemplate = `<!doctype html><meta charset="utf-8"><title>Reddit export</title>
<body style="font:16px sans-serif;max-width:46em;margin:3em auto">
<h2>Drag this link to the bookmarks bar</h2>
<p><a href="%s" style="font-size:22px;padding:.4em .8em;border:2px solid #f60;border-radius:6px">Reddit export</a></p>
<p>Click it on a thread to export that thread. Click it anywhere else on reddit.com to export your newest saves or this month's top posts.</p>
<p>Each export downloads as one JSON file. Drop it into a connector whose format is <code>reddit-export</code>:</p>
<pre>ingestion items drop reddit-saved-2026-11-25.json --connector reddit-exports</pre>
<h2>Or paste it into the console on reddit.com</h2>
<textarea style="width:100%%;height:14em">%s</textarea>
`
