// Package publicweb is the one door that GETs a page an outsider's link
// names. It dials public addresses alone, and it refuses an answer that is
// not the linked page, such as a bot's challenge or a redirect to the site's
// home.
package publicweb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/html"
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
	// also are the media types beyond HTML that the reader answers.
	also []string
}

// NewReader is the reader that GETs pages through client, as Client opens
// one.
func NewReader(client *http.Client) Reader { return Reader{client: client} }

// Accepting is r answering a page of any of mediaTypes too, such as a PDF
// that a pushed link names, where r alone refuses each type but HTML.
func (r Reader) Accepting(mediaTypes ...string) Reader {
	r.also = append(slices.Clone(r.also), mediaTypes...)
	return r
}

// Read GETs the page at link, and answers its bytes and its media type as
// the page's Content-Type names it, or why it refused. It refuses a page that
// did not answer 2xx, a redirect to the site's home or a section above the
// page, a media type that is neither HTML nor one that the reader accepts, a
// page past MaxPageBytes, a bot's challenge, and a GET that reached nothing.
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
	if err != nil || parsed != "text/html" && parsed != "application/xhtml+xml" && !slices.Contains(r.also, parsed) {
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

// titleElementText is the text of page's first title element, unescaped with
// its whitespace collapsed, and "" where the page's opening holds none.
func titleElementText(page []byte) string {
	m := titleElement.FindSubmatch(page[:min(len(page), titleWindow)])
	if m == nil {
		return ""
	}
	return strings.Join(strings.Fields(html.UnescapeString(string(m[1]))), " ")
}

// PageTitle is an HTML page's own title, read as life's get_page_title reads
// one, so a page keeps one title whichever of the two read it. It is the
// page's og:title, else its first title element outside an inline SVG, else
// its first h1, with a trailing site name cut. It is "" where the page holds
// none. An inline SVG draws its own title, so a logo's can come first.
func PageTitle(page []byte) string {
	og, document, heading := titles(page)
	for _, t := range []string{og, document, heading} {
		if t = strings.Join(strings.Fields(t), " "); t != "" {
			return withoutSiteName(t)
		}
	}
	return ""
}

// titles reads page's og:title, its first title element outside an SVG, and
// the text of its first h1.
func titles(page []byte) (og, document, heading string) {
	z := html.NewTokenizer(bytes.NewReader(page))
	svgDepth := 0
	var h1 *strings.Builder
	for {
		token := z.Next()
		switch token {
		case html.ErrorToken:
			return og, document, heading
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			opens := token == html.StartTagToken
			switch string(name) {
			case "svg":
				if opens {
					svgDepth++
				}
			case "meta":
				if hasAttr {
					og = ogTitle(z, og)
				}
			case "title":
				if opens && document == "" && svgDepth == 0 && z.Next() == html.TextToken {
					document = string(z.Text())
				}
			case "h1":
				if opens && heading == "" && h1 == nil {
					h1 = &strings.Builder{}
				}
			}
		case html.EndTagToken:
			switch name, _ := z.TagName(); string(name) {
			case "svg":
				svgDepth = max(svgDepth-1, 0)
			case "h1":
				if h1 != nil {
					heading, h1 = h1.String(), nil
				}
			}
		case html.TextToken:
			if h1 != nil {
				h1.Write(z.Text())
			}
		}
		if strings.TrimSpace(og) != "" {
			return og, document, heading
		}
	}
}

// ogTitle is the content of the meta tag under z where its property is
// og:title, and have otherwise.
func ogTitle(z *html.Tokenizer, have string) string {
	var property, content string
	for more := true; more; {
		var key, value []byte
		key, value, more = z.TagAttr()
		switch string(key) {
		case "property":
			property = string(value)
		case "content":
			content = string(value)
		}
	}
	if property != "og:title" {
		return have
	}
	return content
}

// withoutSiteName cuts a trailing site name from title, so "Article | Site"
// reads "Article". It cuts at the last place where the first separator that
// title holds appears. It cuts only where four words or fewer follow, so a
// title that holds a separator of its own stays whole.
func withoutSiteName(title string) string {
	for _, separator := range titleSeparators {
		if i := strings.LastIndex(title, separator); i >= 0 {
			if len(strings.Fields(title[i+len(separator):])) <= 4 {
				title = title[:i]
			}
			break
		}
	}
	return strings.TrimSpace(strings.TrimSuffix(title, " - YouTube"))
}

// challenge says whether page's title names a bot's challenge.
func challenge(page []byte) bool {
	title := titleElementText(page)
	if title == "" {
		return false
	}
	segments := []string{strings.ToLower(title)}
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
