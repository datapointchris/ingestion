package ypl

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// wireDocument decodes testdata/wire/<name>.json into into, refusing a field
// that into does not declare, and returns the document's bytes. The documents
// are ypl's own, which its suite writes from its code under
// api/handlers/testdata/wire, copied here as they stood at one of its
// commits.
func wireDocument(t *testing.T, name string, into any) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "wire", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return raw
}

// captionsTimeout and audioWait each outlast the longest that ypl holds such a
// read before its answer begins. One that ends sooner gives up on a read that
// ypl goes on making, so every long video fails while ypl still downloads it.
func TestTheClientWaitsLongerThanYPLHoldsARead(t *testing.T) {
	var budgets struct {
		DoorWaitSeconds     int64 `json:"door_wait_seconds"`
		CaptionsReadSeconds int64 `json:"captions_read_seconds"`
		AudioReadSeconds    int64 `json:"audio_read_seconds"`
	}
	wireDocument(t, "video_read_budgets", &budgets)
	if budgets.CaptionsReadSeconds == 0 || budgets.AudioReadSeconds == 0 {
		t.Fatalf("ypl's read budgets name no captions or audio read: %+v", budgets)
	}
	if most := time.Duration(budgets.CaptionsReadSeconds) * time.Second; captionsTimeout <= most {
		t.Errorf("captionsTimeout is %s, and ypl holds a captions read for up to %s", captionsTimeout, most)
	}
	if most := time.Duration(budgets.AudioReadSeconds) * time.Second; audioWait <= most {
		t.Errorf("audioWait is %s, and ypl holds an audio read for up to %s before it answers", audioWait, most)
	}
}

// Captions holds every field of ypl's captions answer and no other. A field ypl
// renames is refused here. A field ypl stops sending would otherwise decode to
// zero, so the decoded answer has to encode back to the document.
func TestCaptionsHoldsEveryFieldOfYPLsCaptionsAnswer(t *testing.T) {
	var captions Captions
	raw := wireDocument(t, "captions", &captions)
	encoded, err := json.Marshal(captions)
	if err != nil {
		t.Fatal(err)
	}
	var sent, held any
	if err := json.Unmarshal(raw, &sent); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &held); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sent, held) {
		t.Errorf("Captions encodes ypl's captions answer as\n%s\nand ypl sent\n%s", encoded, raw)
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
