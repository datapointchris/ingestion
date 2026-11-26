package ypl_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/connectors"
	_ "github.com/datapointchris/ingestion/ypl"
)

// A set refuses each ypl connector that names no playlist or channel, names
// both, names a key ypl does not take, or deletes or polls once from a
// channel, and names each problem. A playlist and a channel declared as ypl
// reads them are not refused.
func TestASetRefusesAYPLConnectorThatCannotRun(t *testing.T) {
	head := "mode: poll\nsource: ypl\nzone: outside\n"
	declarations := map[string]string{
		"capture":         head + "schedule: \"@every 30m\"\nafter_landing: delete\nsource_parameters:\n  playlist: capture\n",
		"talks":           head + "schedule: \"@hourly\"\nsource_parameters:\n  channel: \"@testchannel\"\n",
		"nothing":         head + "schedule: once\n",
		"both":            head + "schedule: once\nsource_parameters:\n  playlist: capture\n  channel: UCtestchannel\n",
		"misspelled":      head + "schedule: once\nsource_parameters:\n  playlists: capture\n",
		"channel-deletes": head + "schedule: once\nafter_landing: delete\nsource_parameters:\n  channel: UCtestchannel\n",
	}
	want := map[string][]string{
		"connector nothing":         {"name the playlist or the channel"},
		"connector both":            {"a playlist or a channel, not both"},
		"connector misspelled":      {"not playlists", "name the playlist or the channel"},
		"connector channel-deletes": {"nothing deletes them", "a channel polls on a schedule"},
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
	got := map[string]string{}
	for _, inv := range invalid.Invalid {
		got[inv.Declaration] = strings.Join(inv.Problems, "; ")
	}
	for declaration, phrases := range want {
		for _, phrase := range phrases {
			if !strings.Contains(got[declaration], phrase) {
				t.Errorf("%s's problems are %q, want one naming %q", declaration, got[declaration], phrase)
			}
		}
	}
	if len(got) != len(want) {
		t.Errorf("the set refused %v, want only %d connectors", got, len(want))
	}
}
