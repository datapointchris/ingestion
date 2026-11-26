package ypl

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// wireDocument decodes testdata/wire/<name>.json into into. The documents are
// ypl's own, which its suite writes from its code under
// api/handlers/testdata/wire, copied here as they stood at one of its
// commits.
func wireDocument(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "wire", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// The client reaches only routes that ypl's route table lists, and Scopes is
// exactly the scopes of the routes it reaches. Its id limit and its channel
// pattern are the ones that ypl's suite writes.
func TestTheClientHoldsWhatYPLsSuiteWrites(t *testing.T) {
	var routes map[string][]string
	wireDocument(t, "service-scopes", &routes)
	reached := map[string]bool{}
	mux := http.NewServeMux()
	for scope, patterns := range routes {
		for _, pattern := range patterns {
			mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
				reached[scope] = true
				switch {
				case r.Method == http.MethodDelete:
					w.WriteHeader(http.StatusNoContent)
				case strings.HasSuffix(r.URL.Path, "/audio"):
					_, _ = w.Write([]byte("audio"))
				default:
					_, _ = w.Write([]byte("null"))
				}
			})
		}
	}
	api := httptest.NewServer(mux)
	defer api.Close()

	c := NewClient(api.URL, api.Client())
	ctx := t.Context()
	if _, err := c.PlaylistItems(ctx, "PLtestcapture"); err != nil {
		t.Error(err)
	}
	if _, err := c.ChannelUploads(ctx, "@testchannel", ""); err != nil {
		t.Error(err)
	}
	if _, err := c.Videos(ctx, []string{"testvid0001"}); err != nil {
		t.Error(err)
	}
	if err := c.DeletePlaylistItem(ctx, "PLtestcapture", "item0001"); err != nil {
		t.Error(err)
	}
	if _, err := c.Captions(ctx, "testvid0001"); err != nil {
		t.Error(err)
	}
	if audio, err := c.Audio(ctx, "testvid0001"); err != nil {
		t.Error(err)
	} else {
		_ = audio.Body.Close()
	}
	if got, want := slices.Sorted(maps.Keys(reached)), slices.Sorted(slices.Values(Scopes)); !slices.Equal(got, want) {
		t.Errorf("the client's routes need the scopes %v, and Scopes is %v", got, want)
	}

	var limit int
	wireDocument(t, "video_details_limit", &limit)
	if MaxVideos != limit {
		t.Errorf("MaxVideos is %d, and ypl reads at most %d ids at once", MaxVideos, limit)
	}
	var pattern string
	wireDocument(t, "youtube_id_pattern", &pattern)
	if channelName.String() != pattern {
		t.Errorf("channelName is %s, and ypl takes a channel of the shape %s", channelName, pattern)
	}
}
