package scan

import (
	"embed"
	"io/fs"
	"sync"

	"go.yaml.in/yaml/v3"
)

// ingestionRules are ingestion's own rules, embedded so that a CLI reads
// which of them list what an item suggests.
//
//go:embed ingestion-rules/*.yaml
var ingestionRules embed.FS

// suggestionKinds maps each of ingestion's own rules that declares suggests
// to the kind it declares.
var suggestionKinds = sync.OnceValue(func() map[string]string {
	kinds := map[string]string{}
	paths, _ := fs.Glob(ingestionRules, "ingestion-rules/*.yaml")
	for _, path := range paths {
		raw, err := ingestionRules.ReadFile(path)
		if err != nil {
			continue
		}
		var r Rule
		if yaml.Unmarshal(raw, &r) == nil && r.Suggests != "" {
			kinds[r.ID] = r.Suggests
		}
	}
	return kinds
})

// Suggests is the kind of thing that a finding of rule lists, as the rule's
// file declares it, and false for a rule whose match flags an attack.
func Suggests(rule string) (string, bool) {
	kind, ok := suggestionKinds()[rule]
	return kind, ok
}
