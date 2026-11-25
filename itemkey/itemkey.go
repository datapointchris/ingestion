// Package itemkey derives an item's key from any form of its artifact. Every
// key is made here, so a link from a feed and the same link from a phone reach
// one item. A caller never prepares the input this package hashes.
//
// The keys are reddit/<fullname>, youtube/<video id>, url/<16 hex>, file/<16
// hex> and memo/<16 hex>, and <parent key>#<n> for a child. A url key is the
// first 16 hex of the SHA-256 of the canonical URL, a file key of the bytes,
// and a memo key of its caller and Idempotency-Key.
package itemkey

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// The kinds of key, each the prefix before the slash.
const (
	Reddit  = "reddit"
	YouTube = "youtube"
	URL     = "url"
	File    = "file"
	Memo    = "memo"
)

// ErrNotAKey is input that is neither a key nor a URL.
var ErrNotAKey = errors.New("not an item key or a URL")

// ErrShareLink is a link that the Reddit app shares, such as
// reddit.com/r/golang/s/AbCdEf1234. It names its post only through the redirect
// it answers with, so its key is the key of the URL it redirects to.
var ErrShareLink = errors.New("a Reddit share link names its post only through its redirect")

var (
	fullnamePattern = regexp.MustCompile(`^t[1-6]_[a-z0-9]{1,13}$`)
	videoPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)
	hashPattern     = regexp.MustCompile(`^[0-9a-f]{16}$`)
	base36Pattern   = regexp.MustCompile(`^[a-z0-9]{1,13}$`)
)

// Resolve is the key that s names: s itself where it is a key, and the key of
// the artifact where it is an http or https URL.
func Resolve(s string) (string, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), "http://") || strings.HasPrefix(strings.ToLower(s), "https://") {
		return FromURL(s)
	}
	if err := Check(s); err != nil {
		return "", fmt.Errorf("%q is %w: %w", s, ErrNotAKey, err)
	}
	return s, nil
}

// Check says why key is not one this package makes, and is nil where it is.
func Check(key string) error {
	base, children, _ := strings.Cut(key, "#")
	if children != "" {
		for n := range strings.SplitSeq(children, "#") {
			if i, err := strconv.Atoi(n); err != nil || i < 1 || strconv.Itoa(i) != n {
				return fmt.Errorf("a child's number is a whole number from 1, not %q", n)
			}
		}
	}
	kind, id, ok := strings.Cut(base, "/")
	if !ok {
		return errors.New("a key is <kind>/<id>, such as reddit/t3_aq7ggd")
	}
	switch kind {
	case Reddit:
		if !fullnamePattern.MatchString(id) {
			return fmt.Errorf("a reddit key names a fullname such as t3_aq7ggd, not %q", id)
		}
	case YouTube:
		if !videoPattern.MatchString(id) {
			return fmt.Errorf("a youtube key names an 11-character video id, not %q", id)
		}
	case URL, File, Memo:
		if !hashPattern.MatchString(id) {
			return fmt.Errorf("a %s key names 16 lowercase hex, not %q", kind, id)
		}
	default:
		return fmt.Errorf("a key's kind is reddit, youtube, url, file or memo, not %q", kind)
	}
	return nil
}

// FromURL is the key of the artifact at raw: a Reddit post's or comment's
// fullname, a YouTube video's id, or else the hash of the canonical URL. It
// refuses a Reddit share link with ErrShareLink, because hashing it would
// split its post into a second item.
func FromURL(raw string) (string, error) {
	canonical, err := CanonicalURL(raw)
	if err != nil {
		return "", err
	}
	u, _ := url.Parse(canonical)
	if redditShare(u) {
		return "", fmt.Errorf("%q: %w; follow it, and give the URL it reaches", raw, ErrShareLink)
	}
	if fullname, ok := redditFullname(u); ok {
		return Reddit + "/" + fullname, nil
	}
	if id, ok := youtubeVideo(u); ok {
		return YouTube + "/" + id, nil
	}
	return URL + "/" + shortHash(canonical), nil
}

// FromRedditFullname is the key of the Reddit post or comment named fullname,
// such as t3_aq7ggd.
func FromRedditFullname(fullname string) (string, error) {
	fullname = strings.ToLower(strings.TrimSpace(fullname))
	if !fullnamePattern.MatchString(fullname) {
		return "", fmt.Errorf("%q is not a Reddit fullname such as t3_aq7ggd", fullname)
	}
	return Reddit + "/" + fullname, nil
}

// FromYouTubeID is the key of the YouTube video id.
func FromYouTubeID(id string) (string, error) {
	if !videoPattern.MatchString(id) {
		return "", fmt.Errorf("%q is not an 11-character YouTube video id", id)
	}
	return YouTube + "/" + id, nil
}

// FromFileSHA256 is the key of a file with no URL, from the SHA-256 of its
// bytes in hex.
func FromFileSHA256(sum string) (string, error) {
	sum = strings.ToLower(sum)
	if len(sum) != sha256.Size*2 {
		return "", fmt.Errorf("a SHA-256 is %d hex, not %d", sha256.Size*2, len(sum))
	}
	if _, err := hex.DecodeString(sum); err != nil {
		return "", fmt.Errorf("a SHA-256 is hex: %w", err)
	}
	return File + "/" + sum[:16], nil
}

// FromMemo is the key of a memo or typed text, from the caller that sent it
// and the Idempotency-Key it sent, so a retry lands on the first attempt's item.
func FromMemo(caller, idempotencyKey string) (string, error) {
	if caller == "" || idempotencyKey == "" {
		return "", errors.New("a memo's key needs its caller and its Idempotency-Key")
	}
	// A NUL cannot sit in either, so no two pairs join to one input.
	return Memo + "/" + shortHash(caller+"\x00"+idempotencyKey), nil
}

// Child is the key of parent's nth child, from 1.
func Child(parent string, n int) (string, error) {
	if err := Check(parent); err != nil {
		return "", err
	}
	if n < 1 {
		return "", fmt.Errorf("a child's number is from 1, not %d", n)
	}
	return parent + "#" + strconv.Itoa(n), nil
}

// trackingParameters are dropped from a canonical URL, beside every utm_ one.
var trackingParameters = map[string]bool{
	"fbclid": true, "gclid": true, "dclid": true, "gbraid": true, "wbraid": true, "msclkid": true,
	"yclid": true, "twclid": true, "ttclid": true, "li_fat_id": true, "igshid": true, "igsh": true,
	"mc_cid": true, "mc_eid": true, "_hsenc": true, "_hsmi": true, "mkt_tok": true, "ref_src": true,
	"oly_anon_id": true, "oly_enc_id": true, "vero_id": true, "rb_clickid": true, "_gl": true, "si": true,
}

// CanonicalURL is raw with its scheme and host lowercased, its fragment, its
// tracking parameters and its default port dropped, and an empty path written
// as /. Every other part stays as raw wrote it.
func CanonicalURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("%q is not a URL: %w", raw, err)
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%q is not an http or https URL", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%q names no host", raw)
	}
	u.Host = strings.ToLower(u.Host)
	if port := u.Port(); (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		u.Host = u.Hostname()
	}
	u.User = nil
	u.Fragment, u.RawFragment = "", ""
	if u.Path == "" {
		u.Path, u.RawPath = "/", ""
	}
	if u.RawQuery != "" {
		var kept []string
		for pair := range strings.SplitSeq(u.RawQuery, "&") {
			name, _, _ := strings.Cut(pair, "=")
			decoded, err := url.QueryUnescape(name)
			if err != nil {
				decoded = name
			}
			decoded = strings.ToLower(decoded)
			if pair == "" || strings.HasPrefix(decoded, "utm_") || trackingParameters[decoded] {
				continue
			}
			kept = append(kept, pair)
		}
		u.RawQuery = strings.Join(kept, "&")
	}
	u.ForceQuery = false
	return u.String(), nil
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// redditShare says whether u is a share link of the Reddit app,
// /r/<subreddit>/s/<token> or /u/<user>/s/<token>.
func redditShare(u *url.URL) bool {
	host := strings.TrimPrefix(u.Hostname(), "www.")
	if host != "reddit.com" && !strings.HasSuffix(host, ".reddit.com") {
		return false
	}
	segments := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	return len(segments) == 4 && (segments[0] == "r" || segments[0] == "u" || segments[0] == "user") && segments[2] == "s"
}

// redditFullname is the fullname a Reddit URL names: a comment's where the
// permalink names one, and otherwise its post's.
func redditFullname(u *url.URL) (string, bool) {
	host := strings.TrimPrefix(u.Hostname(), "www.")
	segments := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	if host == "redd.it" {
		if len(segments) == 1 && base36Pattern.MatchString(segments[0]) {
			return "t3_" + segments[0], true
		}
		return "", false
	}
	if host != "reddit.com" && !strings.HasSuffix(host, ".reddit.com") {
		return "", false
	}
	for i, s := range segments {
		switch s {
		case "comments":
			if i+1 >= len(segments) || !base36Pattern.MatchString(segments[i+1]) {
				return "", false
			}
			// /comments/<post>/<slug>/<comment>
			if i+3 < len(segments) && base36Pattern.MatchString(segments[i+3]) {
				return "t1_" + segments[i+3], true
			}
			return "t3_" + segments[i+1], true
		case "gallery":
			if i+1 < len(segments) && base36Pattern.MatchString(segments[i+1]) {
				return "t3_" + segments[i+1], true
			}
		}
	}
	return "", false
}

// youtubeVideo is the video id a YouTube URL names.
func youtubeVideo(u *url.URL) (string, bool) {
	host := strings.TrimPrefix(u.Hostname(), "www.")
	segments := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	var id string
	switch host {
	case "youtu.be":
		if len(segments) == 1 {
			id = segments[0]
		}
	case "youtube.com", "m.youtube.com", "music.youtube.com", "youtube-nocookie.com":
		switch {
		case len(segments) == 1 && segments[0] == "watch":
			id = u.Query().Get("v")
		case len(segments) == 2 && (segments[0] == "shorts" || segments[0] == "embed" || segments[0] == "live" || segments[0] == "v"):
			id = segments[1]
		}
	}
	return id, videoPattern.MatchString(id)
}
