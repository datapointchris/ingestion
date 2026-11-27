package freshrss

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

// pageTimeout bounds one page's GET, its redirects and its body included. A
// slow site is an ordinary outcome for a page that a feed links, so it is
// long.
const pageTimeout = time.Minute

// MaxPageBytes bounds one page that a poll keeps. A page past it lands the
// feed's content instead.
const MaxPageBytes = 16 << 20

// errPrivateAddress is a dial that PageClient refuses.
var errPrivateAddress = errors.New("the page's address is not public")

// cgnat is the shared address space of RFC 6598, which carrier NAT and
// overlay networks such as Tailscale use inside a site.
var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// PageClient is the client that GETs the page an entry links. It dials only
// public addresses, checked after each name resolves and on every redirect,
// so a feed that links a loopback, private, link-local or shared address
// reaches nothing inside the worker's network. It reads no proxy from the
// environment, which would carry the dial past that check, and it carries no
// credential.
func PageClient() *http.Client {
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

// PageReader GETs the page that an entry links.
type PageReader struct {
	client *http.Client
}

// NewPageReader is the reader that GETs pages through client, as PageClient
// opens one.
func NewPageReader(client *http.Client) PageReader { return PageReader{client: client} }

// Read GETs the page at link, and answers its bytes and its media type as
// the page's Content-Type names it. refused says why the page cannot land,
// and is "" where it read: the status of a page that did not answer 2xx, a
// media type that is not HTML, a page past MaxPageBytes, or a GET that
// reached nothing. refused never quotes the link or the page.
func (r PageReader) Read(ctx context.Context, link string) (body []byte, mediaType, refused string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, http.NoBody)
	if err != nil {
		return nil, "", "the link is not a URL to GET"
	}
	req.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; ingestion)")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, "", unreached(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, "", fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	mediaType = resp.Header.Get("Content-Type")
	parsed, _, err := mime.ParseMediaType(mediaType)
	if err != nil || parsed != "text/html" && parsed != "application/xhtml+xml" {
		return nil, "", fmt.Sprintf("media type %q", parsed)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, MaxPageBytes+1))
	switch {
	case err != nil:
		return nil, "", unreached(err)
	case len(body) > MaxPageBytes:
		return nil, "", fmt.Sprintf("larger than %d bytes", MaxPageBytes)
	}
	return body, mediaType, ""
}

// unreached says why a GET reached no answer, naming no address.
func unreached(err error) string {
	var timeout interface{ Timeout() bool }
	switch {
	case errors.Is(err, errPrivateAddress):
		return "an address that is not public"
	case errors.As(err, &timeout) && timeout.Timeout():
		return "timed out"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "the name does not resolve"
	}
	return "no answer"
}
