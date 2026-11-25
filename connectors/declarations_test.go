package connectors_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/datapointchris/ingestion/connectors"
)

// The example set reads whole, and each of its files holds to the JSON Schema
// that an editor checks it against.
func TestTheExampleSetReadsAndHoldsToItsSchemas(t *testing.T) {
	examples := filepath.Join("..", "examples", "declarations")
	if _, err := connectors.Read(examples, []string{"books"}); err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	files, err := filepath.Glob(filepath.Join("..", "schemas", "*.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.AddResource(doc.(map[string]any)["$id"].(string), doc); err != nil {
			t.Fatal(err)
		}
	}
	connectorFiles, err := filepath.Glob(filepath.Join(examples, "connectors", "*.yml"))
	if err != nil || len(connectorFiles) == 0 {
		t.Fatalf("the example set holds no connector: %v", err)
	}
	bySchema := map[string][]string{
		"connector.schema.json":     connectorFiles,
		"routing-rules.schema.json": {filepath.Join(examples, "routing-rules.yml")},
		"destinations.schema.json":  {filepath.Join(examples, "destinations.yml")},
	}
	for schema, files := range bySchema {
		compiled, err := c.Compile("https://github.com/datapointchris/ingestion/schemas/" + schema)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var doc any
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(encoded))
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.Validate(instance); err != nil {
				t.Errorf("%s does not hold to %s: %v", f, schema, err)
			}
		}
	}
}
