// Package prompts holds each model stage's prompt and the JSON Schema that
// holds its answer, one directory per stage. A stage's version is its number
// and a digest of both files, so any change to either writes its outputs
// beside the old ones, and a replay runs the new prompt over what is landed.
//
// The model read reads an item through a lens: what it reads the item for,
// and what it answers where the item's note asks nothing. Each lens is a file
// under the stage's lenses directory, which the prompt quotes as its "lens"
// template. A lens has a version of its own, and the prompt under a lens
// digests the lens's file too, so a better lens also writes beside the old.
package prompts

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"text/template"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed quality/prompt.tmpl quality/answer.schema.json
//go:embed contents/prompt.tmpl contents/answer.schema.json
//go:embed topics/prompt.tmpl topics/answer.schema.json
//go:embed model-read/prompt.tmpl model-read/answer.schema.json model-read/lenses/*.tmpl
//go:embed prompt-proposal/prompt.tmpl prompt-proposal/answer.schema.json
//go:embed note/prompt.tmpl note/answer.schema.json note/lenses/*.tmpl
//go:embed comments/prompt.tmpl comments/answer.schema.json
var files embed.FS

// Prompt is one stage's prompt and answer schema at one version.
type Prompt struct {
	Stage string
	// Version is the prompt's number and a digest of its files.
	Version string
	// Lens names how the prompt reads an item, and is nil for a stage that
	// reads through no lens.
	Lens     *Lens
	text     string
	template *template.Template
	// Schema is the answer's JSON Schema as the file holds it.
	Schema json.RawMessage
	schema *jsonschema.Schema
}

// Lens is one way a model read reads an item.
type Lens struct {
	Name string
	// Version is the lens's number and a digest of its file.
	Version string
}

// Quality is the quality gate's prompt.
var Quality = load("quality", "1", nil)

// Contents is the contents stage's prompt.
var Contents = load("contents", "1", nil)

// Topics is the topics stage's prompt, which splits a memo's transcript into
// its topics.
var Topics = load("topics", "1", nil)

// Comments is the prompt that reads what a video's top comments say about it.
var Comments = load("comments", "1", nil)

// PromptProposal is the prompt that drafts a change to another stage's
// prompt from the quality gate's misses.
var PromptProposal = load("prompt-proposal", "1", nil)

// ModelRead is the model read's prompt under each lens, by the lens's name. No choice reads under
// the learning lens, and it stays for the read decisions already written under it.
var ModelRead = loadLenses("model-read", "1", map[string]string{"fleet": "1", "personal": "1", "learning": "1"})

// Note is the note stage's prompt under each lens, by the lens's name. A read
// under a lens with no note prompt answers the lens's own questions.
var Note = loadLenses("note", "1", map[string]string{"fleet": "1"})

// Lenses names each lens that ModelRead holds, in order.
func Lenses() []string { return slices.Sorted(maps.Keys(ModelRead)) }

// lensFile is a lens and the text of its file.
type lensFile struct {
	Lens
	text []byte
}

// loadLenses loads stage's prompt under each lens in lenses, which maps the
// lens's name to its number.
func loadLenses(stage, number string, lenses map[string]string) map[string]Prompt {
	out := make(map[string]Prompt, len(lenses))
	for name, lensNumber := range lenses {
		text, err := files.ReadFile(stage + "/lenses/" + name + ".tmpl")
		if err != nil {
			panic(err)
		}
		sum := sha256.Sum256(text)
		out[name] = load(stage, number, &lensFile{Lens: Lens{Name: name, Version: lensNumber + "-" + hex.EncodeToString(sum[:4])}, text: text})
	}
	return out
}

// load reads stage's files under number, with lens's file as its "lens"
// template where lens is set, and panics where one does not parse, because
// the binary carries them.
func load(stage, number string, lens *lensFile) Prompt {
	text, err := files.ReadFile(stage + "/prompt.tmpl")
	if err != nil {
		panic(err)
	}
	schema, err := files.ReadFile(stage + "/answer.schema.json")
	if err != nil {
		panic(err)
	}
	digested := append(append(append([]byte{}, text...), 0), schema...)
	if lens != nil {
		digested = append(append(digested, 0), lens.text...)
	}
	sum := sha256.Sum256(digested)
	p := Prompt{Stage: stage, Version: number + "-" + hex.EncodeToString(sum[:4]), Schema: schema, text: string(text)}
	if p.template, err = template.New(stage).Option("missingkey=error").Parse(string(text)); err != nil {
		panic(fmt.Sprintf("prompts/%s/prompt.tmpl: %v", stage, err))
	}
	if lens != nil {
		l := lens.Lens
		p.Lens = &l
		if _, err := p.template.New("lens").Parse(string(lens.text)); err != nil {
			panic(fmt.Sprintf("prompts/%s/lenses/%s.tmpl: %v", stage, l.Name, err))
		}
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

// Text is the prompt's template as its file holds it, before any field is in
// place.
func (p Prompt) Text() string { return p.text }

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
