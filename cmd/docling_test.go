package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/store"
)

// The write that ends a book's structure queues a Docling parse where its
// sampled pages are code-heavy and math-light, and for no other book. The
// parse writes beside pdf-text's and is scanned, and a structure replay
// queues none again once it is done at its version. A newer pdf-text parse
// leaves Docling's as the one that the item's parse route answers.
func TestTheStructureQueuesADoclingParseForACodeHeavyMathLightBook(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	script := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	pdfText := extract.Program(extract.PDFText.Name, extract.PDFText.Version, extract.MediaTypePDF, script("pdf-text",
		`cat >/dev/null; printf '%s\n' '{"label":null,"text":"one"}' '{"label":null,"text":"two"}'`+"\n"))
	pdfText.Positional = true
	docling := extract.Program(extract.PDFDocling.Name, extract.PDFDocling.Version, extract.MediaTypePDF, script("pdf-docling",
		"cat >/dev/null; printf '%s\\n' '{\"label\":null,\"text\":\"one\"}' '{\"label\":null,\"text\":\"```\\nselect 1;\\n```\"}'\n"))
	docling.Positional, docling.Secondary = true, true
	extractors, err := extract.NewRegistry(pdfText, docling)
	if err != nil {
		t.Fatal(err)
	}
	profile := func(monospace, math string) string {
		return `'{"pages":2,"outline":[],"page_labels":[],"creation_date":null,"title":null,` +
			`"font_shares":{"sampled_pages":1,"monospace":` + monospace + `,"tex_or_math":` + math + `}}'`
	}
	profiles := script("pdf-structure", `case "$(cat)" in
*math*) printf '%s\n' `+profile("1", "0.5")+` ;;
*prose*) printf '%s\n' `+profile("0.1", "0")+` ;;
*) printf '%s\n' `+profile("0.2", "0")+` ;;
esac
`)
	p := newPipeline(t, extractors, afterStage(quiet))
	p.apply(t, mustAbs(t, "../examples/declarations"))
	structure := sections.NewHandler(profiles, p.worker, quiet)
	keys := map[string]string{}
	for _, body := range []string{"a code book", "a code and math book", "a prose book"} {
		landed, err := p.lander.Land(ctx, landing.Arrival{
			Connector: "books-folder", Zone: store.Outside, MediaType: extract.MediaTypePDF, Body: strings.NewReader(body),
		})
		if err != nil {
			t.Fatal(err)
		}
		keys[body] = landed.Key
	}
	if n := p.parse(t, extractors); n != 3 {
		t.Fatalf("ran %d pdf-text parses, want one a book", n)
	}
	p.scanFindingNothing(t)
	if n := p.run(t, structure); n != 3 {
		t.Fatalf("ran %d structure tasks, want 3", n)
	}
	queued := func() []string {
		rows, err := p.st.Reader().QueryContext(ctx, `SELECT item_key FROM tasks WHERE capability = ? AND state = 'queued'`,
			extract.Capability(extract.PDFDocling))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				t.Fatal(err)
			}
			out = append(out, key)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := queued(); len(got) != 1 || got[0] != keys["a code book"] {
		t.Fatalf("the structure queued Docling parses for %v, want the code book %s alone", got, keys["a code book"])
	}

	if n := p.parse(t, extractors); n != 1 {
		t.Fatalf("ran %d parses, want the code book's Docling parse", n)
	}
	newest := p.readParse(t, keys["a code book"], "")
	if newest.Extractor != extract.PDFDocling.Name || len(newest.Units) != 2 || !strings.Contains(newest.Units[1].Text, "```") {
		t.Errorf("the code book's newest parse is %s with units %+v, want Docling's two pages, the second fenced", newest.Extractor, newest.Units)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE item_key = ? AND capability = ? AND state = 'queued'`,
		keys["a code book"], scan.Capability); n != 1 {
		t.Errorf("the Docling parse queued %d scans, want the one that reads it", n)
	}

	if out, err := runCLI(t, "items", "replay", "--stage", "structure", "--connector", "books-folder", "--config", p.config); err != nil {
		t.Fatalf("the structure replay printed %q, %v", out, err)
	}
	if n := p.run(t, structure); n != 3 {
		t.Fatalf("the structure replay ran %d tasks, want 3", n)
	}
	if got := queued(); len(got) != 0 {
		t.Errorf("the structure replay queued Docling parses for %v, want none, because the code book's is done", got)
	}

	newerText := pdfText
	newerText.Version = pdfText.Version + ".1"
	newer, err := extract.NewRegistry(newerText, docling)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.st.Update(ctx, func(tx *store.Tx) error {
		key := keys["a code book"]
		_, err := tx.Enqueue(ctx, store.Task{ItemKey: &key, Stage: store.StageParse, Capability: extract.Capability(newerText)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if n := p.parse(t, newer); n != 1 {
		t.Fatalf("ran %d parses, want the newer pdf-text parse", n)
	}
	if got := p.readParse(t, keys["a code book"], ""); got.Extractor != extract.PDFDocling.Name {
		t.Errorf("after a newer pdf-text parse the item's parse route answers %s %s, want Docling's", got.Extractor, got.ExtractorVersion)
	}
}
