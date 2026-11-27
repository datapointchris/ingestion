// Package publicweb is the one door that GETs a page an outsider's link
// names. It dials public addresses alone, and it refuses an answer that is
// not the linked page, such as a bot's challenge or a redirect to the site's
// home.
package publicweb

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// pageTimeout bounds one page's GET, its redirects and its body included. A
// slow site is an ordinary outcome for a page that a feed links, so it is
// long.
const pageTimeout = time.Minute

// MaxPageBytes bounds one page that a Reader reads. A page past it is
// refused.
const MaxPageBytes = 16 << 20

// errPrivateAddress is a dial that Client refuses.
var errPrivateAddress = errors.New("the page's address is not public")

// cgnat is the shared address space of RFC 6598, which carrier NAT and
// overlay networks such as Tailscale use inside a site.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// challengeTitles are the titles, lowercased, that a site serves in place of
// the page once it decides that the request is a bot's. Each segment of a
// page's title between titleSeparators is compared whole, so an article that
// mentions one is not caught.
var challengeTitles = map[string]bool{
	"just a moment...":    true, // Cloudflare's interstitial
	"attention required!": true, // Cloudflare's block page
	"prove your humanity": true, // Reddit
	"access denied":       true, // Akamai
}

var titleSeparators = []string{" | ", " - ", " — ", " · "}

// titleWindow bounds how much of a page the search for its title reads.
const titleWindow = 64 << 10

var titleElement = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)

// Client is the client that GETs an outsider's page. It dials only public
// addresses, checked after each name resolves and on every redirect, so a
// link to a loopback, private, link-local or shared address reaches nothing
// inside the caller's network. It reads no proxy from the environment, which
// would carry the dial past that check, and it carries no credential.
func Client() *http.Client {
	dialer := &net.Dialer{Timeout: 30 * time.Second, Control: func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip, err := netip.ParseAddr(host)
		if err != nil {
			return err
		}
		if !public(ip) {
			return errPrivateAddress
		}
		return nil
	}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = dialer.DialContext
	return &http.Client{Timeout: pageTimeout, Transport: transport}
}

// public says whether ip is an address on the public internet.
func public(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !cgnat.Contains(ip)
}

// Refusal is why a page cannot land. Its reason never quotes the link or the
// page.
type Refusal struct {
	Reason string
	// MayClear says that a later GET may reach the page, as after a 503, a
	// 429 or a timeout. It is false where the same GET would be refused
	// again, as after a 404 or a bot's challenge.
	MayClear bool
}

// Reader GETs the page that a link names.
type Reader struct {
	client *http.Client
}

// NewReader is the reader that GETs pages through client, as Client opens
// one.
func NewReader(client *http.Client) Reader { return Reader{client: client} }

// Read GETs the page at link, and answers its bytes and its media type as
// the page's Content-Type names it, or why it refused. It refuses a page that
// did not answer 2xx, a redirect to the site's home or a section above the
// page, a media type that is not HTML, a page past MaxPageBytes, a bot's
// challenge, and a GET that reached nothing.
func (r Reader) Read(ctx context.Context, link string) (body []byte, mediaType string, refused *Refusal) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, http.NoBody)
	if err != nil {
		return nil, "", &Refusal{Reason: "the link is not a URL to GET"}
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ingestion)")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, "", unreached(err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch status := resp.StatusCode; {
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		return nil, "", &Refusal{Reason: fmt.Sprintf("HTTP %d", status), MayClear: true}
	case status < 200 || status > 299:
		return nil, "", &Refusal{Reason: fmt.Sprintf("HTTP %d", status)}
	}
	if redirectedAway(req.URL, resp.Request.URL) {
		return nil, "", &Refusal{Reason: "a redirect to the site's home or a section above the page"}
	}
	mediaType = resp.Header.Get("Content-Type")
	parsed, _, err := mime.ParseMediaType(mediaType)
	if err != nil || parsed != "text/html" && parsed != "application/xhtml+xml" {
		return nil, "", &Refusal{Reason: fmt.Sprintf("media type %q", parsed)}
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
	switch {
	case err != nil:
		return nil, "", unreached(err)
	case len(body) > MaxPageBytes:
		return nil, "", &Refusal{Reason: fmt.Sprintf("larger than %d bytes", MaxPageBytes)}
	case challenge(body):
		return nil, "", &Refusal{Reason: "a bot's challenge in place of the page"}
	}
	return body, mediaType, nil
}

// redirectedAway says whether a redirect replaced the page at requested with
// the site's home or a section above it. A moved page lands on another path
// and is still the page.
func redirectedAway(requested, final *url.URL) bool {
	if final == nil {
		return false
	}
	asked := strings.TrimRight(requested.Path, "/")
	landed := strings.TrimRight(final.Path, "/")
	if asked == "" || landed == asked {
		return false
	}
	return landed == "" || strings.HasPrefix(asked, landed+"/")
}

// challenge says whether page's title names a bot's challenge.
func challenge(page []byte) bool {
	m := titleElement.FindSubmatch(page[:min(len(page), titleWindow)])
	if m == nil {
		return false
	}
	segments := []string{strings.ToLower(html.UnescapeString(string(m[1])))}
	for _, separator := range titleSeparators {
		var split []string
		for _, s := range segments {
			split = append(split, strings.Split(s, separator)...)
		}
		segments = split
	}
	for _, s := range segments {
		if challengeTitles[strings.TrimSpace(s)] {
			return true
		}
	}
	return false
}

// unreached is why a GET reached no answer, naming no address.
func unreached(err error) *Refusal {
	var timeout interface{ Timeout() bool }
	var dns *net.DNSError
	switch {
	case errors.Is(err, errPrivateAddress):
		return &Refusal{Reason: "an address that is not public"}
	case errors.As(err, &dns) && dns.IsNotFound:
		return &Refusal{Reason: "the name does not resolve"}
	case errors.As(err, &timeout) && timeout.Timeout():
		return &Refusal{Reason: "timed out", MayClear: true}
	case dns != nil:
		return &Refusal{Reason: "the name's lookup failed", MayClear: true}
	}
	return &Refusal{Reason: "no answer", MayClear: true}
}
