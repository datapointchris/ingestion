// Package prompts holds each model stage's prompt and the JSON Schema that
// holds its answer, one directory per stage. A stage's version is its number
// and a digest of both files, so any change to either writes its outputs
// beside the old ones, and a replay runs the new prompt over what is landed.
package prompts

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed quality/prompt.tmpl quality/answer.schema.json
var files embed.FS

// Prompt is one stage's prompt and answer schema at one version.
type Prompt struct {
	Stage string
	// Version is the prompt's number and a digest of its files.
	Version  string
	template *template.Template
	// Schema is the answer's JSON Schema as the file holds it.
	Schema json.RawMessage
	schema *jsonschema.Schema
}

// Quality is the quality gate's prompt.
var Quality = load("quality", "1")

// load reads stage's files under number, and panics where one does not parse,
// because the binary carries them.
func load(stage, number string) Prompt {
	text, err := files.ReadFile(stage + "/prompt.tmpl")
	if err != nil {
		panic(err)
	}
	schema, err := files.ReadFile(stage + "/answer.schema.json")
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(append(append(append([]byte{}, text...), 0), schema...))
	p := Prompt{Stage: stage, Version: number + "-" + hex.EncodeToString(sum[:4]), Schema: schema}
	if p.template, err = template.New(stage).Option("missingkey=error").Parse(string(text)); err != nil {
		panic(fmt.Sprintf("prompts/%s/prompt.tmpl: %v", stage, err))
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		panic(fmt.Sprintf("prompts/%s/answer.schema.json: %v", stage, err))
	}
	// The schema compiles under its own $id, so an error names that URL
	// rather than a path on the machine that checked the answer.
	id, _ := document.(map[string]any)["$id"].(string)
	if id == "" {
		panic(fmt.Sprintf("prompts/%s/answer.schema.json names no $id", stage))
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(id, document); err != nil {
		panic(err)
	}
	if p.schema, err = c.Compile(id); err != nil {
		panic(fmt.Sprintf("prompts/%s/answer.schema.json: %v", stage, err))
	}
	return p
}

// Render is the prompt with data's fields in place.
func (p Prompt) Render(data any) (string, error) {
	var b strings.Builder
	if err := p.template.Execute(&b, data); err != nil {
		return "", fmt.Errorf("render the %s prompt: %w", p.Stage, err)
	}
	return b.String(), nil
}

// Check says why answer does not hold to the prompt's schema, and nil where it
// does. A model's answer that the schema does not hold fails its stage rather
// than being patched.
func (p Prompt) Check(answer json.RawMessage) error {
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(answer))
	if err != nil {
		return fmt.Errorf("the answer is not JSON: %w", err)
	}
	if err := p.schema.Validate(instance); err != nil {
		return fmt.Errorf("the answer does not hold to prompts/%s/answer.schema.json: %w", p.Stage, err)
	}
	return nil
}
