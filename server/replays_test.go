package server_test

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/server"
)

// When a stage joins or leaves the replayable set, ReplayStages and the
// published schema's stage enum change together, because the CLI offers the
// first and a catalog reader trusts the second.
func TestReplayStagesAreTheSchemasStageEnum(t *testing.T) {
	raw, err := os.ReadFile("../schemas/replay.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Stage struct {
				Enum []string `json:"enum"`
			} `json:"stage"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(schema.Properties.Stage.Enum, server.ReplayStages) {
		t.Errorf("replay.schema.json names stages %v, and server.ReplayStages %v", schema.Properties.Stage.Enum, server.ReplayStages)
	}
}
