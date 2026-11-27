package lens

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// slateAnswers is each route the reader asks, with the stand-in answer that
// testdata/slate holds for it and the pointer into slate's published OpenAPI
// that the answer holds to.
var slateAnswers = map[string]struct{ file, schema string }{
	"/api/v1/focus": {
		"testdata/slate/focus.json",
		"#/paths/~1api~1v1~1focus/get/responses/200/content/application~1json/schema",
	},
	"/api/v1/initiatives": {
		"testdata/slate/initiatives.json",
		"#/paths/~1api~1v1~1initiatives/get/responses/200/content/application~1json/schema",
	},
}

// TestSlateReaderReadsSlatesPublishedAnswers holds each stand-in answer to
// slate's OpenAPI, then reads them through the registered reader, so the
// reader decodes what slate answers.
func TestSlateReaderReadsSlatesPublishedAnswers(t *testing.T) {
	spec, err := os.ReadFile("testdata/slate/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(spec))
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err := compiler.AddResource("openapi.json", doc); err != nil {
		t.Fatal(err)
	}
	for route, a := range slateAnswers {
		schema, err := compiler.Compile("openapi.json" + a.schema)
		if err != nil {
			t.Fatalf("%s: %v", route, err)
		}
		raw, err := os.ReadFile(a.file)
		if err != nil {
			t.Fatal(err)
		}
		answer, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(answer); err != nil {
			t.Fatalf("%s's stand-in answer breaks slate's schema: %v", route, err)
		}
	}

	var asked []string
	slate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = append(asked, r.URL.RequestURI())
		a, ok := slateAnswers[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		raw, err := os.ReadFile(a.file)
		if err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
	}))
	defer slate.Close()

	reg, ok := Lookup(Slate)
	if !ok {
		t.Fatal("no slate reader is registered")
	}
	reader := reg.Open(slate.URL+"/", slate.Client())
	got, err := reader.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := Context{
		Focus: &Focus{
			Statement: "Mostly the rocket garden's irrigation controller for six weeks.",
			Until:     "2030-04-12",
			Shares:    []Share{{"Garden automation", 0.7}, {"Kitchen ledger", 0.2}},
		},
		Initiatives: []Initiative{
			{101, "The irrigation controller waters each bed by its soil reading", "Each bed's valve opens on its own moisture sensor rather than a timer.", []string{"Garden automation"}},
			{102, "The kitchen ledger imports each receipt", "A photographed receipt lands as one ledger row per line item.", []string{"Kitchen ledger"}},
		},
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("read\n%s\nwant\n%s", gotJSON, wantJSON)
	}
	if strings.Join(asked, " ") != "/api/v1/focus /api/v1/initiatives?status=active" {
		t.Fatalf("asked %q", asked)
	}

	if _, err := reader.Read(context.Background()); err != nil || len(asked) != 2 {
		t.Fatalf("a second read within the cache asked slate again: %q, %v", asked, err)
	}
}
