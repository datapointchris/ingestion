package itemkey_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/datapointchris/ingestion/itemkey"
)

// Each form an artifact arrives in reaches its one key, and two forms of one
// artifact reach the same key.
func TestEveryFormOfAnArtifactResolvesToItsOneKey(t *testing.T) {
	sum := sha256.Sum256([]byte("https://example.com/post?id=7"))
	page := "url/" + hex.EncodeToString(sum[:])[:16]
	cases := []struct{ in, want string }{
		{"reddit/t3_aq7ggd", "reddit/t3_aq7ggd"},
		{"https://www.reddit.com/r/ClaudeAI/comments/aq7ggd/stop_writing/", "reddit/t3_aq7ggd"},
		{"https://old.reddit.com/r/ClaudeAI/comments/aq7ggd/stop_writing/?utm_source=share#top", "reddit/t3_aq7ggd"},
		{"https://redd.it/aq7ggd", "reddit/t3_aq7ggd"},
		{"https://www.reddit.com/r/golang/comments/aq7ggd/a_post/eh5x2kq/", "reddit/t1_eh5x2kq"},
		{"https://www.youtube.com/watch?v=dQw4w9WgXcQ&si=share", "youtube/dQw4w9WgXcQ"},
		{"https://youtu.be/dQw4w9WgXcQ?t=42", "youtube/dQw4w9WgXcQ"},
		{"https://m.youtube.com/shorts/dQw4w9WgXcQ", "youtube/dQw4w9WgXcQ"},
		{"https://example.com/post?id=7", page},
		{"HTTPS://Example.COM:443/post?utm_campaign=x&id=7&fbclid=y#comments", page},
		{"url/0123456789abcdef#2", "url/0123456789abcdef#2"},
	}
	for _, c := range cases {
		got, err := itemkey.Resolve(c.in)
		if err != nil || got != c.want {
			t.Errorf("Resolve(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
	for _, refused := range []string{
		"reddit/aq7ggd", "url/0123", "url/0123456789abcdef#0", "ftp://example.com/x", "t3_aq7ggd", "file/../etc",
		"https://www.reddit.com/r/golang/s/AbCdEf1234",
	} {
		if got, err := itemkey.Resolve(refused); err == nil {
			t.Errorf("Resolve(%q) = %q, want it refused", refused, got)
		}
	}
	one, _ := itemkey.FromMemo("ingestion-svc-phone", "a1")
	again, _ := itemkey.FromMemo("ingestion-svc-phone", "a1")
	other, _ := itemkey.FromMemo("ingestion-svc-phon", "ea1")
	if one != again || one == other || itemkey.Check(one) != nil {
		t.Errorf("memo keys %q, %q and %q: a retry must reach the first and another caller must not", one, again, other)
	}
}
