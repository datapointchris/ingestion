package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/datapointchris/ingestion/contents"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/sections"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// A long PDF whose outline gives it no sections waits at the contents stage,
// and the connector's first fill holds every book after the first. The
// worker quotes only the page that looks like a contents page, under the
// config's model and effort, and the stand-in answers in the stage's schema.
// Serve places each entry: by the offset of the numbers printed at its pages'
// foot, by a search for its title where a chapter opens two pages after the
// printed one, a roman page that maps to none at the first page opening on
// its title, and a part with no printed page where its first chapter starts.
// The entries make a part, three chapter items, and front and back matter.
// connectors show prints the first call's tokens, and the release sends the
// second book. Its answer names two chapters on pages that its PDF lacks, so
// the placement leaves out a third of its chapters and the book stays whole. A structure replay then
// places each stored answer again and starts no model call.
func TestABookWithNoOutlineTakesItsSectionsFromItsPrintedContents(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	var units []string
	for n := 1; n <= 60; n++ {
		text := "body text\n" + strconv.Itoa(n-4)
		switch n {
		case 1, 2:
			text = "Cover"
		case 3:
			text = "Contents\nPart I Beginnings\n1 Start 1\n2 Middle 15\n3 End 30\nIndex 52"
		case 4:
			text = "Preface\nwhy this book"
		case 5:
			text = "Chapter 1\nStart\nbody"
		case 21:
			text = "2 Middle\nbody"
		case 22:
			text = "Middle\nmore body"
		case 34:
			text = "3 End\nbody"
		case 56:
			text = "Index\nterms"
		}
		unit, _ := json.Marshal(map[string]string{"text": text})
		units = append(units, string(unit))
	}
	script := func(name, out string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\ncat >/dev/null\ncat <<'EOF'\n"+out+"\nEOF\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	pdfText := extract.Program(extract.PDFText.Name, extract.PDFText.Version, extract.MediaTypePDF, script("pdf-text", strings.Join(units, "\n")))
	pdfText.Positional = true
	extractors, err := extract.NewRegistry(pdfText)
	if err != nil {
		t.Fatal(err)
	}
	profile := script("pdf-structure",
		`{"pages":60,"outline":[],"page_labels":[],"creation_date":null,"title":null,"font_shares":{"sampled_pages":12,"monospace":0,"tex_or_math":0}}`)
	p := newPipeline(t, extractors, afterStage(quiet))
	p.apply(t, mustAbs(t, "../examples/declarations"))
	structure := sections.NewHandler(profile, p.worker, quiet)
	for _, body := range []string{"one book", "another book"} {
		if _, err := p.lander.Land(ctx, landing.Arrival{
			Connector: "books-folder", Zone: store.Outside, MediaType: extract.MediaTypePDF, Body: strings.NewReader(body),
		}); err != nil {
			t.Fatal(err)
		}
	}
	p.parse(t, extractors)
	if n := p.run(t, structure); n != 2 {
		t.Fatalf("ran %d structure tasks, want 2", n)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ? AND held_by = 'books-folder'`, store.StageContents); n != 1 {
		t.Fatalf("the first fill holds %d contents tasks, want every book's but the first", n)
	}

	stand := newStandInDispatch(t, p.provider)
	calls := 0
	stand.answer = func(prompt string) map[string]any {
		if !strings.Contains(prompt, "[PDF page 3]\nContents") || strings.Contains(prompt, "[PDF page 4]") {
			t.Errorf("the prompt quotes more or less than the contents page:\n%s", prompt)
		}
		entry := func(title string, page any, level int) map[string]any {
			return map[string]any{"title": title, "page": page, "level": level}
		}
		if calls++; calls > 1 {
			return map[string]any{"entries": []any{
				entry("1 Start", "1", 1), entry("2 Middle", "15", 1), entry("Absent One", "901", 1), entry("3 End", "30", 1),
				entry("Absent Two", "930", 1), entry("Index", "52", 1),
			}}
		}
		return map[string]any{"entries": []any{
			entry("Preface", "iv", 1), entry("Part I Beginnings", nil, 1), entry("1 Start", "1", 2), entry("2 Middle", "15", 2),
			entry("3 End", "30", 2), entry("Index", "52", 1),
		}}
	}
	models := stand.models(t, "", "")
	models.Contents = stageModel{Model: "opus", Effort: "high"}
	reader := p.modelHandler(t, models, contents.Capability)
	p.run(t, reader)
	p.advance(time.Minute)
	p.run(t, reader)
	starts := stand.starts()
	if len(starts) != 1 || starts[0].Model != "opus" || starts[0].Effort != "high" || !strings.HasSuffix(starts[0].Schema, "/prompts/contents/answer.schema.json") {
		t.Fatalf("the reader started %+v, want one contents call to the config's opus at high", starts)
	}

	var shown server.ConnectorView
	out, err := runCLI(t, "connectors", "show", "books-folder", "--json", "--config", p.config)
	if err != nil || json.Unmarshal([]byte(out), &shown) != nil || shown.FirstFill == nil || shown.FirstFill.ItemKey == nil {
		t.Fatalf("connectors show printed %q, %v", out, err)
	}
	if tokens := shown.FirstFill.Tokens[store.StageContents]; tokens.CacheWrite != 3400 || tokens.Output != 180 {
		t.Errorf("the first fill shows %+v of the contents call, want the stand-in's tokens", shown.FirstFill.Tokens)
	}
	book := *shown.FirstFill.ItemKey
	var placed []string
	for _, s := range p.shown(t, book).Sections {
		placed = append(placed, fmt.Sprintf("%s %d %s", s.Kind, s.PDFPageStart, s.StartPlacedBy))
	}
	want := "front-matter 4 title, part 5 next-entry, chapter 5 offset, chapter 21 title, chapter 34 offset, back-matter 56 offset"
	if got := strings.Join(placed, ", "); got != want {
		t.Errorf("the book's contents sections are %s, want %s", got, want)
	}
	if n := p.count(t, `SELECT count(*) FROM items WHERE parent_key = ? AND media_type = ?`, book, store.MediaTypeChapter); n != 3 {
		t.Errorf("the book holds %d chapter items, want 3", n)
	}

	if _, err := runCLI(t, "connectors", "release", "books-folder", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, reader)
	p.advance(time.Minute)
	p.run(t, reader)
	if n := len(stand.starts()); n != 2 {
		t.Errorf("the release let %d contents calls through in all, want the second book's too", n)
	}
	stays := func(when string) {
		t.Helper()
		if n := p.count(t, `SELECT count(*) FROM sections WHERE item_key != ?`, book); n != 0 {
			t.Errorf("%s, the second book holds %d sections, want none from an answer that left out a third of its chapters", when, n)
		}
		if n := p.count(t, `SELECT count(*) FROM items WHERE media_type = ?`, store.MediaTypeChapter); n != 3 {
			t.Errorf("%s, the books hold %d chapter items, want the first book's 3", when, n)
		}
	}
	stays("after the release")

	replay := func(stage string) server.ReplayAnswer {
		t.Helper()
		out, err := runCLI(t, "items", "replay", "--stage", stage, "--connector", "books-folder", "--json", "--config", p.config)
		var answer server.ReplayAnswer
		if err != nil || json.Unmarshal([]byte(out), &answer) != nil {
			t.Fatalf("items replay --stage %s printed %q, %v", stage, out, err)
		}
		holdsTo(t, []byte(out), "replay.schema.json")
		return answer
	}
	replay(store.StageStructure)
	if n := p.run(t, structure); n != 2 {
		t.Fatalf("the structure replay ran %d tasks, want 2", n)
	}
	if n := p.count(t, `SELECT count(*) FROM tasks WHERE stage = ?`, store.StageContents); n != 2 || len(stand.starts()) != 2 {
		t.Errorf("after the structure replay the books hold %d contents tasks and %d calls, want each book's first alone", n, len(stand.starts()))
	}
	stays("after the structure replay")
	if a := replay(store.StageContents); a.Items != 0 || a.Queued {
		t.Errorf("a contents replay answered %+v, want none due: each book holds an answer of the prompt in force", a)
	}
}
