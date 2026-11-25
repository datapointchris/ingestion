package redditexport_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/datapointchris/ingestion/redditexport"
)

// Each bookmarklet mode splits into one save per post it carries, each with
// its own thread, so a top export and a thread export land as a saved one does.
func TestEveryExportKindSplitsIntoOneSavePerPostWithItsThread(t *testing.T) {
	post := func(name string) string {
		return `{"kind":"t3","data":{"name":"` + name + `","subreddit":"golang","title":"A post","permalink":"/r/golang/comments/` + name[3:] + `/a_post/"}}`
	}
	thread := func(name string) string {
		return `[{"data":{"children":[` + post(name) + `]}},{"data":{"children":[]}}]`
	}
	exports := map[string]string{
		redditexport.KindSaved: `{"kind":"reddit-saved","exported_at":"2026-10-30T19:18:39Z","items":[` +
			`{"child":` + post("t3_aa1") + `,"thread":` + thread("t3_aa1") + `},` +
			`{"child":{"kind":"t1","data":{"name":"t1_cc3","subreddit":"golang","link_title":"A post","permalink":"/r/golang/comments/aa1/a_post/cc3/"}},"thread":null,"thread_error":"403"}]}`,
		redditexport.KindTop: `{"kind":"reddit-top","exported_at":"2026-10-30T19:18:39Z","period":"month","subreddits":[{"name":"golang","limit":2,` +
			`"listing":{"data":{"children":[` + post("t3_bb1") + `,` + post("t3_bb2") + `]}},` +
			`"threads":{"t3_bb1":` + thread("t3_bb1") + `,"t3_bb2":{"error":"429"}}}]}`,
		redditexport.KindThread: `{"kind":"reddit-thread","exported_at":"2026-10-30T19:18:39Z","fullname":"t3_dd1","listing":` + thread("t3_dd1") + `}`,
	}
	want := map[string][]string{
		redditexport.KindSaved:  {"reddit/t3_aa1", "reddit/t1_cc3"},
		redditexport.KindTop:    {"reddit/t3_bb1", "reddit/t3_bb2"},
		redditexport.KindThread: {"reddit/t3_dd1"},
	}
	for kind, raw := range exports {
		e, err := redditexport.Parse([]byte(raw))
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		saves, err := e.Saves()
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		var keys []string
		for _, s := range saves {
			key, err := s.Key()
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
			var body struct {
				Child       json.RawMessage `json:"child"`
				Thread      json.RawMessage `json:"thread"`
				ThreadError string          `json:"thread_error"`
			}
			if err := json.Unmarshal(s.Bytes, &body); err != nil || len(body.Child) == 0 {
				t.Errorf("%s's save %s is not a child and its thread: %v", kind, key, err)
			}
			if refused := body.ThreadError != ""; refused == (len(body.Thread) > 0 && string(body.Thread) != "null") {
				t.Errorf("%s's save %s holds thread %q and thread error %q, want one", kind, key, body.Thread, body.ThreadError)
			}
		}
		if !slices.Equal(keys, want[kind]) {
			t.Errorf("%s split into %v, want %v", kind, keys, want[kind])
		}
	}
	if _, err := redditexport.Parse([]byte(`{"kind":"pocket","exported_at":"2026-10-30T19:18:39Z"}`)); err == nil {
		t.Error("a file of another kind parsed as a Reddit export")
	}
}
