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
