package schemas_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/schemas"
	"github.com/datapointchris/ingestion/store"
)

// The review tab cycles store's sorts and groupings, and the server takes a
// card that the record schema accepts. A sort that one names and the other
// does not would refuse every decision made under it.
func TestTheTabsSortsAndGroupingsAreTheOnesTheRecordSchemaAccepts(t *testing.T) {
	raw, err := schemas.Files.ReadFile("ingestion-record.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Defs struct {
			Presented struct {
				Properties struct {
					Sort, Grouping struct{ Enum []string }
				}
			}
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if got := doc.Defs.Presented.Properties.Sort.Enum; !slices.Equal(got, store.PresentedSorts) {
		t.Errorf("the record schema enumerates the sorts %v, and store.PresentedSorts is %v", got, store.PresentedSorts)
	}
	if got := doc.Defs.Presented.Properties.Grouping.Enum; !slices.Equal(got, store.PresentedGroupings) {
		t.Errorf("the record schema enumerates the groupings %v, and store.PresentedGroupings is %v", got, store.PresentedGroupings)
	}
}

// A model stage added to store and left out of a schema that names a stage
// fails a reader that validates the answer. Each such enum holds every model
// stage, and the two that name where an item waits hold the line's alone.
func TestTheSchemasStageEnumsHoldStoresModelStages(t *testing.T) {
	enum := func(file string, path ...any) []string {
		t.Helper()
		raw, err := schemas.Files.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var at any
		if err := json.Unmarshal(raw, &at); err != nil {
			t.Fatal(err)
		}
		for _, step := range path {
			switch k := step.(type) {
			case string:
				at = at.(map[string]any)[k]
			case int:
				at = at.([]any)[k]
			}
		}
		var out []string
		for _, v := range at.([]any) {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	for name, got := range map[string][]string{
		"item.schema.json's output stages": enum("item.schema.json", "properties", "outputs", "items", "properties", "stage", "enum"),
		"tasks-list.schema.json's stages":  enum("tasks-list.schema.json", "properties", "tasks", "items", "properties", "stage", "enum"),
	} {
		for _, stage := range store.ModelStages() {
			if !slices.Contains(got, stage) {
				t.Errorf("%s leave out the model stage %s", name, stage)
			}
		}
	}
	for name, got := range map[string][]string{
		"item.schema.json's waiting stage":    enum("item.schema.json", "properties", "waiting", "anyOf", 0, "properties", "stage", "enum"),
		"items-list.schema.json's waiting_at": enum("items-list.schema.json", "$defs", "waiting_at", "enum"),
	} {
		if !slices.Equal(got, store.LineModelStages()) {
			t.Errorf("%s is %v, and store.LineModelStages is %v", name, got, store.LineModelStages())
		}
	}
}
