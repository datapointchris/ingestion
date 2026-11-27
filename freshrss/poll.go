package freshrss

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/server"
)

// pageSize is how many entries one read of the reading list asks for.
const pageSize = 100

// What a poll passes over, by the name its log line counts it under.
const (
	// countReadBefore is an entry crawled no later than the cursor, which an
	// earlier poll read or the baseline passed over. The API answers an entry
	// that changed since the cursor's second, as well as one crawled since.
	countReadBefore = "read_before"
	// countNoLink is an entry whose feed gives it no link, so it has no key.
	countNoLink = "no_link"
	// countUnkeyedLink is an entry whose link is not an http or https URL.
	countUnkeyedLink = "unkeyed_link"
	// countRepeated is a second entry that links the same page in one poll,
	// such as an article that two feeds carry.
	countRepeated = "repeated"
	// countFeedContent is an entry whose page refused its GET, so it landed
	// the feed's own content.
	countFeedContent = "feed_content"
)

// Poller reads the reading list for each freshrss connector's poll.
type Poller struct {
	client *Client
	pages  PageReader
}

// NewPoller is the poller that reads the API through client and GETs each
// new entry's page through pages.
func NewPoller(client *Client, pages PageReader) Poller { return Poller{client: client, pages: pages} }

// cursor is a connector's position.
type cursor struct {
	// CrawlTimeMsec is the newest crawl time that a poll read, in
	// milliseconds since the epoch. An entry crawled at or before it has
	// been read.
	CrawlTimeMsec int64 `json:"crawl_time_msec"`
}

// Poll reads each entry crawled since the connector's cursor, oldest first,
// and reports one arrival for each with the page that it links. Its first
// poll keeps the newest entry's crawl time as the cursor and lands nothing.
func (p Poller) Poll(ctx context.Context, poll polling.Poll) polling.Outcome {
	var was cursor
	if poll.Cursor != nil {
		if err := json.Unmarshal([]byte(*poll.Cursor), &was); err != nil {
			reason := fmt.Sprintf("the connector's cursor is not a %s reading list's position: %v", Source, err)
			return polling.Outcome{Error: &reason}
		}
	}
	if was.CrawlTimeMsec == 0 {
		return p.baseline(ctx)
	}
	out := polling.Outcome{Counts: map[string]int{}}
	next := was
	landed := map[string]bool{}
	continuation := ""
	for {
		page, err := p.client.ReadingList(ctx, ReadingListQuery{
			Since: time.UnixMilli(was.CrawlTimeMsec), Order: OldestFirst, Count: pageSize, Continuation: continuation,
		})
		if err != nil {
			return failed(err)
		}
		for _, e := range page.Items {
			crawled := int64(e.CrawlTimeMsec)
			if crawled <= was.CrawlTimeMsec {
				out.Counts[countReadBefore]++
				continue
			}
			next.CrawlTimeMsec = max(next.CrawlTimeMsec, crawled)
			a, reason, err := p.arrival(ctx, poll.Keep, e, landed)
			if err != nil {
				return failed(err)
			}
			if reason != "" {
				out.Counts[reason]++
			}
			if a != nil {
				out.Arrivals = append(out.Arrivals, *a)
			}
		}
		if page.Continuation == "" || page.Continuation == continuation || len(page.Items) == 0 {
			break
		}
		continuation = page.Continuation
	}
	if next == was {
		return out
	}
	return withCursor(out, next)
}

// baseline is a first poll: the newest entry's crawl time, or the server's
// time where the reading list holds none, as the cursor, and no arrival.
func (p Poller) baseline(ctx context.Context) polling.Outcome {
	page, err := p.client.ReadingList(ctx, ReadingListQuery{Order: NewestFirst, Count: 1})
	if err != nil {
		return failed(err)
	}
	at := cursor{CrawlTimeMsec: time.Now().UnixMilli()}
	switch {
	case len(page.Items) > 0 && page.Items[0].CrawlTimeMsec > 0:
		at.CrawlTimeMsec = int64(page.Items[0].CrawlTimeMsec)
	case page.Updated > 0:
		at.CrawlTimeMsec = page.Updated * 1000
	}
	return withCursor(polling.Outcome{}, at)
}

// arrival is e as a poll reports it, with the page that it links kept
// through keep, or else the feed's content. It answers nil and the count to
// pass it over under where e cannot land, and a count beside the arrival
// where it lands the feed's content. landed holds each key that the poll
// reported, and an error is a page that could not be kept.
func (p Poller) arrival(ctx context.Context, keep func(context.Context, []byte, string) (string, error),
	e Entry, landed map[string]bool,
) (*server.ReportArrival, string, error) {
	link := e.link()
	if link == "" {
		return nil, countNoLink, nil
	}
	key, err := itemkey.FromURL(link)
	if err != nil {
		return nil, countUnkeyedLink, nil
	}
	canonical, err := itemkey.CanonicalURL(link)
	if err != nil {
		return nil, countUnkeyedLink, nil
	}
	if landed[key] {
		return nil, countRepeated, nil
	}
	landed[key] = true
	fields := map[string]string{FieldFeedStreamID: e.Origin.StreamID, FieldBody: BodyPage}
	if e.Origin.Title != "" {
		fields[FieldFeedTitle] = e.Origin.Title
	}
	if label := e.feedLabel(); label != "" {
		fields[FieldFeedLabel] = label
	}
	count := ""
	body, mediaType, refused := p.pages.Read(ctx, canonical)
	if refused != "" && ctx.Err() != nil {
		return nil, "", fmt.Errorf("the poll stopped before it read every new entry's page: %w", ctx.Err())
	}
	if refused != "" {
		body, mediaType = feedDocument(e.content()), "text/html; charset=utf-8"
		fields[FieldBody], fields[FieldPageRefused] = BodyFeed, refused
		count = countFeedContent
	}
	sum, err := keep(ctx, body, mediaType)
	if err != nil {
		return nil, "", err
	}
	a := &server.ReportArrival{
		Key: key, URL: canonical, MediaType: extract.MediaTypeHTML, Title: html.UnescapeString(e.Title), ExternalKey: e.ID,
		Fields: fields, BlobSHA256: &sum,
	}
	if e.Published > 0 {
		published := time.Unix(e.Published, 0).UTC().Format(time.RFC3339)
		a.SourceTS = &published
	}
	return a, count, nil
}

// link is the URL that e names its page by: its canonical link, or else its
// first alternate.
func (e Entry) link() string {
	for _, links := range [][]Link{e.Canonical, e.Alternate} {
		if len(links) > 0 && strings.TrimSpace(links[0].Href) != "" {
			return strings.TrimSpace(links[0].Href)
		}
	}
	return ""
}

// feedLabel is the label of the category that holds e's feed. FreshRSS
// writes the category's label before any label that a person put on the
// entry itself, so it is the first.
func (e Entry) feedLabel() string {
	for _, c := range e.Categories {
		if !strings.HasPrefix(c, "user/") {
			continue
		}
		if _, label, ok := strings.Cut(c, "/label/"); ok {
			return label
		}
	}
	return ""
}

// content is e's content as the feed carried it: its summary, as FreshRSS's
// API writes it, or else its content.
func (e Entry) content() string {
	for _, c := range []*Contents{e.Summary, e.Content} {
		if c != nil && c.Content != "" {
			return c.Content
		}
	}
	return ""
}

// feedDocument is fragment, a feed's HTML content, as a page of its own in
// UTF-8, so an extractor that reads a whole page reads it.
func feedDocument(fragment string) []byte {
	return []byte("<!doctype html>\n<html><head><meta charset=\"utf-8\"></head><body>\n" + fragment + "\n</body></html>\n")
}

// withCursor is out moving the connector's cursor to position.
func withCursor(out polling.Outcome, position cursor) polling.Outcome {
	raw, err := json.Marshal(position)
	if err != nil {
		reason := "write the connector's cursor: " + err.Error()
		return polling.Outcome{Error: &reason}
	}
	written := string(raw)
	out.Cursor = &written
	return out
}

// failed is a poll that err stopped. The connector's backoff makes it due
// again, and the cursor stays where it was.
func failed(err error) polling.Outcome {
	reason := err.Error()
	return polling.Outcome{Error: &reason}
}
