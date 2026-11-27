package ypl_test

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/connectors"
	_ "github.com/datapointchris/ingestion/ypl"
)

// A set refuses each ypl connector that cannot run, and accepts a capture
// playlist, a playlist landed once and a channel. Each refused connector
// breaks one rule, so a rule that stops refusing leaves its connector
// accepted.
func TestASetRefusesAYPLConnectorThatCannotRun(t *testing.T) {
	head := "source: ypl\nzone: outside\n"
	poll := head + "mode: poll\n"
	declarations := map[string]string{
		"capture":         poll + "schedule: \"@every 30m\"\nafter_landing: delete\nsource_parameters:\n  playlist: capture\n",
		"backfill":        poll + "schedule: once\nsource_parameters:\n  playlist: PLtestbackfill\n",
		"talks":           poll + "schedule: \"@hourly\"\nsource_parameters:\n  channel: \"@testchannel\"\n",
		"nothing":         poll + "schedule: once\n",
		"both":            poll + "schedule: \"@hourly\"\nsource_parameters:\n  playlist: capture\n  channel: UCtestchannel\n",
		"misspelled":      poll + "schedule: once\nsource_parameters:\n  playlist: capture\n  playlists: capture\n",
		"bad-channel":     poll + "schedule: \"@hourly\"\nsource_parameters:\n  channel: \"not a channel\"\n",
		"channel-deletes": poll + "schedule: \"@hourly\"\nafter_landing: delete\nsource_parameters:\n  channel: UCtestchannel\n",
		"channel-once":    poll + "schedule: once\nsource_parameters:\n  channel: UCtestchannel\n",
		"playlist-kept":   poll + "schedule: \"@every 30m\"\nsource_parameters:\n  playlist: capture\n",
		"pushed":          "source: ypl\nmode: push\ndue_interval: 7d\nsource_parameters:\n  channel: UCtestchannel\n",
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "connectors"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range declarations {
		if err := os.WriteFile(filepath.Join(dir, "connectors", name+".yml"), []byte("name: "+name+"\n"+body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	_, err := connectors.Read(dir, nil)
	var invalid *connectors.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("reading the set answered %v, want the invalid declarations", err)
	}
	refused := map[string][]string{}
	for _, inv := range invalid.Invalid {
		refused[inv.Declaration] = inv.Problems
	}
	want := []string{
		"connector bad-channel", "connector both", "connector channel-deletes", "connector channel-once",
		"connector misspelled", "connector nothing", "connector playlist-kept", "connector pushed",
	}
	if got := slices.Sorted(maps.Keys(refused)); !slices.Equal(got, want) {
		t.Errorf("the set refused %v, want %v", got, want)
	}
	for declaration, problems := range refused {
		if len(problems) != 1 {
			t.Errorf("%s breaks %d rules, want one: %q", declaration, len(problems), problems)
		}
	}
}
