package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/store"
)

// A PDF's parse queues its structure, whose end writes the outline's sections
// and a chapter item for each chapter, dated by the PDF's creation date. Each
// chapter takes its book's placement, and a person who places the book again
// moves every chapter with it. A second profile of the same version writes
// nothing and makes no chapter again.
func TestABooksStructureMakesItsChaptersAndEachTakesTheBooksPlacement(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	script := func(name, body string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\ncat >/dev/null\n"+body), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	pdfText := extract.Program(extract.PDFText.Name, extract.PDFText.Version, extract.MediaTypePDF, script("pdf-text",
		`printf '%s\n' '{"label":"i","text":"Contents"}' '{"label":"1","text":"One"}' '{"label":"2","text":"Two"}' `+
			`'{"label":"3","text":"Three"}' '{"label":"4","text":"Four"}' '{"label":"5","text":"Index"}'`+"\n"))
	pdfText.Positional = true
	extractors, err := extract.NewRegistry(pdfText)
	if err != nil {
		t.Fatal(err)
	}
	script("pdf-structure", `printf '%s\n' '{"pages":6,"outline":[`+
		`{"level":1,"title":"Contents","page":1},{"level":1,"title":"Part I","page":2},{"level":2,"title":"Chapter 1","page":2},`+
		`{"level":2,"title":"Chapter 2","page":4},{"level":1,"title":"Part II","page":5},{"level":2,"title":"Chapter 3","page":5},`+
		`{"level":1,"title":"Index","page":6}],"page_labels":[],"creation_date":"D:20190415093021+02'"'"'00'"'"'",`+
		`"font_shares":{"sampled_pages":2,"monospace":0.5,"tex_or_math":0}}'`+"\n")
	p := newPipeline(t, extractors, afterStage(quiet))
	p.apply(t, mustAbs(t, "../examples/declarations"))
	structure := sections.NewHandler(filepath.Join(dir, "pdf-structure"), p.worker, quiet)
	landed, err := p.lander.Land(ctx, landing.Arrival{
		Connector: "books", Zone: store.Outside, MediaType: extract.MediaTypePDF, Body: strings.NewReader("a book"),
	})
	if err != nil {
		t.Fatal(err)
	}
	book := landed.Key
	if n := p.parse(t, extractors); n != 1 {
		t.Fatalf("ran %d parse tasks, want 1", n)
	}
	if n := p.run(t, structure); n != 1 {
		t.Fatalf("ran %d structure tasks, want the one the parse queued", n)
	}
	p.scanFindingNothing(t)

	shown := p.shown(t, book)
	var kinds, chapters []string
	for _, s := range shown.Sections {
		kinds = append(kinds, s.Kind)
		if s.Kind == store.SectionChapter {
			chapters = append(chapters, *s.ChildKey+" "+*s.PrintedPageStart+"-"+*s.PrintedPageEnd)
		}
	}
	if got, want := strings.Join(kinds, " "), "front-matter part chapter chapter part chapter back-matter"; got != want {
		t.Errorf("the book's sections are %s, want %s", got, want)
	}
	if got, want := strings.Join(chapters, ", "), book+"#1 1-2, "+book+"#2 3-3, "+book+"#3 4-4"; got != want {
		t.Errorf("the book's chapters are %s, want %s, each ending where the next begins", got, want)
	}
	if shown.Item.SourceTS == nil || *shown.Item.SourceTS != "2019-04-15T07:30:21.000Z" {
		t.Errorf("the book's source time is %v, want its creation date in UTC", shown.Item.SourceTS)
	}
	first := p.shown(t, book+"#1")
	if it := first.Item; it.MediaType != store.MediaTypeChapter || it.Zone != store.Outside || it.Title == nil || *it.Title != "Chapter 1" ||
		it.SourceTS == nil || *it.SourceTS != *shown.Item.SourceTS || it.ParentKey == nil || *it.ParentKey != book {
		t.Errorf("the first chapter's item is %+v, want the book's zone, source time and its own title", it)
	}

	for _, placement := range []struct{ destination, side, read string }{
		{"reading-list", "personal", "as-is"},
		{"work-inbox", "fleet", "model"},
	} {
		if _, err := runCLI(t, "review", "place", book, placement.destination, "--config", p.config); err != nil {
			t.Fatal(err)
		}
		for n := 1; n <= 3; n++ {
			_, side, read := p.decisions(t, book+"#"+string(rune('0'+n)))
			if side.Verdict != placement.side || side.Destination == nil || *side.Destination != placement.destination ||
				side.DecidedBy != route.ByParent(book) || read.Verdict != placement.read || read.DecidedBy != route.ByParent(book) {
				t.Errorf("chapter %d took side %+v and read %+v, want the book's %s on %s, read %s", n, side, read,
					placement.side, placement.destination, placement.read)
			}
		}
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND item_key LIKE ?`, store.StageModelRead, book+"#%"); n != 0 {
		t.Errorf("%d model reads were queued for chapters, which hold no parse of their own", n)
	}

	if out, err := runCLI(t, "items", "replay", "--stage", "structure", "--key", book, "--config", p.config); err != nil ||
		out != "Queued structure on 1 items.\n" {
		t.Fatalf("the structure replay printed %q, %v", out, err)
	}
	p.run(t, structure)
	if n := p.count(t, `SELECT count(*) FROM sections WHERE item_key = ?`, book); n != len(kinds) {
		t.Errorf("a second profile of one version left %d sections, want the first's %d", n, len(kinds))
	}
	if n := p.count(t, `SELECT count(*) FROM items WHERE parent_key = ?`, book); n != 3 {
		t.Errorf("the book holds %d chapter items after a second profile, want 3", n)
	}
}
