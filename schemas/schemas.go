// Package schemas embeds the JSON Schema of each record the outbox ships, each
// API answer and each declaration file, so the service checks a declaration
// or a request body against the same file an editor and a data catalog read.
package schemas

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Files is every *.schema.json in this directory.
//
//go:embed *.schema.json
var Files embed.FS

// Base is the URL each file's $id sits under, as Base + its file name.
const Base = "https://github.com/datapointchris/ingestion/schemas/"

var (
	compileMu sync.Mutex
	compiler  *jsonschema.Compiler
	compiled  = map[string]*jsonschema.Schema{}
)

// Compiled is the schema at, a file's name with an optional #pointer into it,
// compiled beside every other file here so a $ref between them resolves. A
// format such as date-time is checked, not only noted.
func Compiled(at string) (*jsonschema.Schema, error) {
	compileMu.Lock()
	defer compileMu.Unlock()
	if s, ok := compiled[at]; ok {
		return s, nil
	}
	if compiler == nil {
		c := jsonschema.NewCompiler()
		c.AssertFormat()
		names, err := fs.Glob(Files, "*.schema.json")
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			raw, err := Files.ReadFile(name)
			if err != nil {
				return nil, err
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
			if err != nil {
				return nil, fmt.Errorf("read schema %s: %w", name, err)
			}
			if err := c.AddResource(Base+name, doc); err != nil {
				return nil, err
			}
		}
		compiler = c
	}
	s, err := compiler.Compile(Base + at)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", at, err)
	}
	compiled[at] = s
	return s, nil
}

var english = message.NewPrinter(language.English)

// Problems is each way v, as JSON, does not hold to schema, by the field it
// names, and nil where v holds to it.
func Problems(schema *jsonschema.Schema, v any) []string {
	raw, err := json.Marshal(v)
	if err != nil {
		return []string{err.Error()}
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return []string{err.Error()}
	}
	err = schema.Validate(instance)
	var invalid *jsonschema.ValidationError
	if !errors.As(err, &invalid) {
		if err != nil {
			return []string{err.Error()}
		}
		return nil
	}
	seen := map[string]bool{}
	var out []string
	var leaves func(e *jsonschema.ValidationError)
	leaves = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, cause := range e.Causes {
				leaves(cause)
			}
			return
		}
		problem := e.ErrorKind.LocalizedString(english)
		if at := strings.Join(e.InstanceLocation, "."); at != "" {
			problem = at + ": " + problem
		}
		if !seen[problem] {
			seen[problem] = true
			out = append(out, problem)
		}
	}
	leaves(invalid)
	sort.Strings(out)
	return out
}
