package freshrss

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/polling"
	"github.com/datapointchris/ingestion/publicweb"
	"github.com/datapointchris/ingestion/server"
)

// pageSize is how many entries one read of the reading list asks for.
const pageSize = 100

// overlap is how far behind its newest crawl time a poll reads again.
// FreshRSS stamps an entry's id as it parses the entry, and commits the
// feed's entries after. So two refreshes at once can commit an id below one
// that a poll already read. The cursor holds each id that it read inside the
// overlap, so a second read of one costs a skip.
const overlap = time.Hour

// pollBudget is how long a poll goes on reading new entries. A poll past it
// reports what it read, and the next poll reads on from there.
const pollBudget = 10 * time.Minute

// pageAttempts is how many polls GET an entry's page whose refusal may clear,
// such as a 503 or a timeout, before the entry lands the feed's content.
const pageAttempts = 3

// What a poll passes over or holds, by the name its log line counts it under.
const (
	// countReadBefore is an entry that an earlier poll read, or that was
	// crawled before the overlap behind the cursor or before the baseline.
	// The API answers an entry that changed since the read's start, as well
	// as one crawled since.
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
	// countPageHeld is an entry whose page refused its GET for a cause that
	// may clear, so the next poll GETs it again.
	countPageHeld = "page_held"
	// countHeldGone is a held entry that a whole read of the reading list did
	// not answer, such as one that FreshRSS purged. The cursor lets it go.
	countHeldGone = "held_gone"
	// countReadAtSource is an entry whose link names an item that an
	// extractor reads at its source, such as a YouTube video, which lands
	// with no bytes, as a pushed link of it does.
	countReadAtSource = "read_at_source"
	// countAnotherSourcesLink is an entry whose link does not land, as a
	// pushed link of it would not. It names an item of another source that no
	// extractor reads from its link, such as a Reddit post. Or it names a page
	// of a site whose items an extractor reads, such as a channel's.
	countAnotherSourcesLink = "another_sources_link"
)

// LinkLandings answers how a link keyed key lands, where link is its
// canonical URL, as extract.Registry answers the push route too.
type LinkLandings interface {
	LinkLanding(key, link string) server.LinkLanding
}

// Poller reads the reading list for each freshrss connector's poll.
type Poller struct {
	client *Client
	pages  publicweb.Reader
	links  LinkLandings
}

// NewPoller is the poller that reads the API through client, GETs each new
// entry's page through pages, and asks links how each entry's link lands.
func NewPoller(client *Client, pages publicweb.Reader, links LinkLandings) Poller {
	return Poller{client: client, pages: pages, links: links}
}

// cursor is a connector's position.
type cursor struct {
	// CrawlTimeMsec is the newest crawl time that a poll read, in
	// milliseconds since the epoch.
	CrawlTimeMsec int64 `json:"crawl_time_msec"`
	// BaselineMsec is the crawl time that the first poll kept. An entry
	// crawled at or before it never lands.
	BaselineMsec int64 `json:"baseline_crawl_time_msec,omitempty"`
	// Read is each entry crawled inside the overlap behind CrawlTimeMsec that
	// a poll read, which a later poll passes over.
	Read []entryAt `json:"read,omitempty"`
	// Held is each entry whose page refused for a cause that may clear.
	Held []heldEntry `json:"held,omitempty"`
}

type entryAt struct {
	ID            string `json:"id"`
	CrawlTimeMsec int64  `json:"crawl_time_msec"`
}

type heldEntry struct {
	entryAt
	// Attempts is how many polls GET the entry's page.
	Attempts int `json:"attempts"`
}

// Poll reads each entry crawled since the connector's cursor, oldest first,
// and reports one arrival for each with the page that it links. Its first
// poll keeps the newest entry's crawl time as the cursor and lands nothing.
// A poll that stops partway, past pollBudget or at a failure after its first
// read, reports what it read before the stop, and the next poll reads on.
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
	start := time.Now()
	out := polling.Outcome{Counts: map[string]int{}}
	floor := max(was.BaselineMsec, was.CrawlTimeMsec-overlap.Milliseconds())
	since := floor
	read := map[string]int64{}
	for _, e := range was.Read {
		read[e.ID] = e.CrawlTimeMsec
	}
	held := map[string]heldEntry{}
	for _, h := range was.Held {
		held[h.ID] = h
		since = min(since, h.CrawlTimeMsec)
	}
	next := was.CrawlTimeMsec
	answered := map[string]bool{}
	landed := map[string]bool{}
	progressed := false
	stop := ""
	continuation := ""
pages:
	for {
		page, err := p.client.ReadingList(ctx, ReadingListQuery{
			Since: time.UnixMilli(since), Order: OldestFirst, Count: pageSize, Continuation: continuation,
		})
		if err != nil && !progressed {
			return failed(err)
		}
		if err != nil {
			stop = "the poll stopped before the end of the reading list: " + err.Error()
			break
		}
		for _, e := range page.Items {
			crawled := int64(e.CrawlTimeMsec)
			h, isHeld := held[e.ID]
			answered[e.ID] = true
			if _, done := read[e.ID]; done || crawled <= floor && !isHeld {
				out.Counts[countReadBefore]++
				continue
			}
			if time.Since(start) > pollBudget {
				stop = fmt.Sprintf("the poll read for %s, so the next one reads on", pollBudget)
				break pages
			}
			d, err := p.decide(ctx, poll.Keep, e, landed, h.Attempts)
			if err != nil {
				stop = "the poll stopped before it read every new entry: " + err.Error()
				break pages
			}
			progressed = true
			next = max(next, crawled)
			if d.count != "" {
				out.Counts[d.count]++
			}
			if d.hold {
				held[e.ID] = heldEntry{entryAt: entryAt{ID: e.ID, CrawlTimeMsec: crawled}, Attempts: h.Attempts + 1}
				continue
			}
			delete(held, e.ID)
			read[e.ID] = crawled
			if d.arrival != nil {
				out.Arrivals = append(out.Arrivals, *d.arrival)
			}
		}
		if page.Continuation == "" || page.Continuation == continuation || len(page.Items) == 0 {
			break
		}
		continuation = page.Continuation
	}
	if stop == "" {
		for id := range held {
			if !answered[id] {
				delete(held, id)
				out.Counts[countHeldGone]++
			}
		}
	} else {
		out.Warnings = append(out.Warnings, stop)
	}
	position := cursor{CrawlTimeMsec: next, BaselineMsec: was.BaselineMsec}
	behind := max(was.BaselineMsec, next-overlap.Milliseconds())
	for id, at := range read {
		if at > behind {
			position.Read = append(position.Read, entryAt{ID: id, CrawlTimeMsec: at})
		}
	}
	for _, h := range held {
		position.Held = append(position.Held, h)
	}
	slices.SortFunc(position.Read, byCrawlTime)
	slices.SortFunc(position.Held, func(a, b heldEntry) int { return byCrawlTime(a.entryAt, b.entryAt) })
	slices.SortFunc(was.Read, byCrawlTime)
	slices.SortFunc(was.Held, func(a, b heldEntry) int { return byCrawlTime(a.entryAt, b.entryAt) })
	moved, _ := json.Marshal(position)
	kept, _ := json.Marshal(was)
	if string(moved) == string(kept) {
		return out
	}
	return withCursor(out, position)
}

func byCrawlTime(a, b entryAt) int {
	return cmp.Or(cmp.Compare(a.CrawlTimeMsec, b.CrawlTimeMsec), strings.Compare(a.ID, b.ID))
}

// baseline is a first poll: the newest entry's crawl time, or the server's
// time where the reading list holds none, as the cursor and its baseline, and
// no arrival.
func (p Poller) baseline(ctx context.Context) polling.Outcome {
	page, err := p.client.ReadingList(ctx, ReadingListQuery{Order: NewestFirst, Count: 1})
	if err != nil {
		return failed(err)
	}
	at := time.Now().UnixMilli()
	switch {
	case len(page.Items) > 0 && page.Items[0].CrawlTimeMsec > 0:
		at = int64(page.Items[0].CrawlTimeMsec)
	case page.Updated > 0:
		at = page.Updated * 1000
	}
	return withCursor(polling.Outcome{}, cursor{CrawlTimeMsec: at, BaselineMsec: at})
}

// decision is what a poll does with one new entry.
type decision struct {
	// arrival is the entry as the poll reports it, and nil where it cannot
	// land.
	arrival *server.ReportArrival
	// count is the name to count the entry under, where it lands the feed's
	// content, is held or cannot land.
	count string
	// hold says that the entry's page refused for a cause that may clear, so
	// the next poll GETs it again.
	hold bool
}

// decide is how e lands: with the page that it links kept through keep, with
// no bytes where its link names an item that an extractor reads at its
// source, or with the feed's content where the page refused. attempts is how
// many earlier polls GET its page. landed holds each key that the poll
// reported. An error is a page that could not be kept, or a poll that
// stopped.
func (p Poller) decide(ctx context.Context, keep func(context.Context, []byte, string) (string, error),
	e Entry, landed map[string]bool, attempts int,
) (decision, error) {
	link := e.link()
	if link == "" {
		return decision{count: countNoLink}, nil
	}
	key, err := itemkey.FromURL(link)
	if err != nil {
		return decision{count: countUnkeyedLink}, nil
	}
	canonical, err := itemkey.CanonicalURL(link)
	if err != nil {
		return decision{count: countUnkeyedLink}, nil
	}
	if landed[key] {
		return decision{count: countRepeated}, nil
	}
	a := &server.ReportArrival{Key: key, URL: canonical, Title: html.UnescapeString(e.Title), ExternalKey: e.ID, Fields: e.fields()}
	if e.Published > 0 {
		published := time.Unix(e.Published, 0).UTC().Format(time.RFC3339)
		a.SourceTS = &published
	}
	linked := p.links.LinkLanding(key, canonical)
	switch linked.Lands {
	case server.LinkReadAtSource:
		landed[key] = true
		a.MediaType, a.URL = linked.MediaType, linked.URL
		return decision{arrival: a, count: countReadAtSource}, nil
	case server.LinkPage:
	default:
		return decision{count: countAnotherSourcesLink}, nil
	}
	d := decision{arrival: a}
	body, mediaType, refused := p.pages.Read(ctx, canonical)
	switch {
	case refused != nil && ctx.Err() != nil:
		return decision{}, ctx.Err()
	case refused != nil && refused.MayClear && attempts+1 < pageAttempts:
		return decision{count: countPageHeld, hold: true}, nil
	case refused != nil:
		body, mediaType = feedDocument(e.content()), "text/html; charset=utf-8"
		a.Fields[FieldBody], a.Fields[FieldPageRefused] = BodyFeed, refused.Reason
		d.count = countFeedContent
	default:
		a.Fields[FieldBody] = BodyPage
	}
	sum, err := keep(ctx, body, mediaType)
	if err != nil {
		return decision{}, err
	}
	landed[key] = true
	a.MediaType, a.BlobSHA256 = linked.MediaType, &sum
	return d, nil
}

// fields are the fields of e's sighting that name its feed.
func (e Entry) fields() map[string]string {
	fields := map[string]string{FieldFeedStreamID: e.Origin.StreamID}
	if e.Origin.Title != "" {
		fields[FieldFeedTitle] = e.Origin.Title
	}
	if label := e.feedLabel(); label != "" {
		fields[FieldFeedLabel] = label
	}
	return fields
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

// failed is a poll that err stopped before it read anything. The connector's
// backoff makes it due again, and the cursor stays where it was.
func failed(err error) polling.Outcome {
	reason := err.Error()
	return polling.Outcome{Error: &reason}
}
