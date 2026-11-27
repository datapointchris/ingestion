package connectors

import (
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/datapointchris/ingestion/schemas"
)

// declarationSchemas holds the compiled schema of a connector, a routing rule
// and a destination, which every declaration is checked against.
type declarationSchemas struct {
	connector, rule, destination *jsonschema.Schema
}

var compiledSchemas = sync.OnceValues(func() (declarationSchemas, error) {
	var out declarationSchemas
	for at, into := range map[string]**jsonschema.Schema{
		"connector.schema.json":                       &out.connector,
		"routing-rules.schema.json#/$defs/rule":       &out.rule,
		"destinations.schema.json#/$defs/destination": &out.destination,
	} {
		var err error
		if *into, err = schemas.Compiled(at); err != nil {
			return declarationSchemas{}, err
		}
	}
	return out, nil
})
