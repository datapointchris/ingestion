package extract_test

import (
	"strings"
	"testing"

	"github.com/datapointchris/ingestion/extract"
)

func comment(name, author, body string, replies ...string) string {
	listing := `""`
	if len(replies) > 0 {
		listing = `{"kind":"Listing","data":{"children":[` + strings.Join(replies, ",") + `]}}`
	}
	return `{"kind":"t1","data":{"name":"` + name + `","author":"` + author + `","body":"` + body + `","replies":` + listing + `}}`
}

// A save parses as its post, then each comment depth first in the thread's
// order. A deleted comment and a placeholder for unloaded ones are left out,
// and the deleted comment's reply is kept. A saved comment whose thread
// Reddit refused parses as its post's title and the comment.
func TestASaveParsesAsItsPostThenEachCommentDepthFirst(t *testing.T) {
	post := `{"kind":"t3","data":{"name":"t3_aa1","author":"op","title":"A post","selftext":"Its body","is_self":true}}`
	thread := `[{"kind":"Listing","data":{"children":[` + post + `]}},{"kind":"Listing","data":{"children":[` +
		comment("t1_c1", "alice", "First", comment("t1_c2", "bob", "Reply")) + `,` +
		comment("t1_c3", "[deleted]", "[deleted]", comment("t1_c4", "carol", "Kept reply")) + `,` +
		`{"kind":"more","data":{"children":["t1_c9"]}}]}}]`
	refused := `{"kind":"t1","data":{"name":"t1_c5","author":"dave","body":"Saved words","link_title":"Its post","link_author":"op2","replies":""}}`
	for name, tc := range map[string]struct {
		save string
		want []string
	}{
		"a post": {
			`{"child":` + post + `,"thread":` + thread + `}`,
			[]string{"u/op: A post\n\nIts body", "u/alice: First", "u/bob: Reply", "u/carol: Kept reply"},
		},
		"a refused comment": {
			`{"child":` + refused + `,"thread":null,"thread_error":"403"}`,
			[]string{"u/op2: Its post", "u/dave: Saved words"},
		},
	} {
		read, err := extract.ThreadText.Extract(t.Context(), strings.NewReader(tc.save))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var got []string
		for _, u := range read.Units {
			got = append(got, *u.Label+": "+u.Text)
		}
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s parsed as %q, want %q", name, got, tc.want)
		}
	}
}
