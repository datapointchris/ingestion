package connectors

import (
	"bytes"
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

	"github.com/datapointchris/ingestion/schemas"
)

// declarationSchemas holds the compiled schema of a connector, a routing rule
// and a destination, which every declaration is checked against.
type declarationSchemas struct {
	connector, rule, destination *jsonschema.Schema
}

var compiledSchemas = sync.OnceValues(func() (declarationSchemas, error) {
	c := jsonschema.NewCompiler()
	names, err := fs.Glob(schemas.Files, "*.schema.json")
	if err != nil {
		return declarationSchemas{}, err
	}
	for _, name := range names {
		raw, err := schemas.Files.ReadFile(name)
		if err != nil {
			return declarationSchemas{}, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return declarationSchemas{}, fmt.Errorf("read schema %s: %w", name, err)
		}
		if err := c.AddResource(schemas.Base+name, doc); err != nil {
			return declarationSchemas{}, err
		}
	}
	var out declarationSchemas
	for at, into := range map[string]**jsonschema.Schema{
		"connector.schema.json":                       &out.connector,
		"routing-rules.schema.json#/$defs/rule":       &out.rule,
		"destinations.schema.json#/$defs/destination": &out.destination,
	} {
		if *into, err = c.Compile(schemas.Base + at); err != nil {
			return declarationSchemas{}, fmt.Errorf("compile schema %s: %w", at, err)
		}
	}
	return out, nil
})

var english = message.NewPrinter(language.English)

// schemaProblems is each way v, as JSON, does not hold to schema, by the
// field it names.
func schemaProblems(schema *jsonschema.Schema, v any) []string {
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
