package review

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/comments"
	"github.com/datapointchris/ingestion/redditexport"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/ypl"
)

// The sorts s cycles, the first the default, as server.PresentedSorts names
// them. Each puts an item with no value for it after every item with one, and
// breaks a tie newest first.
const (
	sortNewest       = "newest"
	sortQuality      = "quality"
	sortLikesPerView = "likes-per-view"
	sortLength       = "length"
	sortFit          = "fit"
)

var sorts = []string{sortNewest, sortQuality, sortLikesPerView, sortLength, sortFit}

// sortWords says what each sort puts first, for the strip and the ? overlay.
var sortWords = map[string]string{
	sortNewest:       "newest made at its source first",
	sortQuality:      "the gate's highest quality first",
	sortLikesPerView: "the most likes per view first",
	sortLength:       "the shortest first",
	sortFit:          "the best fit to the current work first",
}

// sortValue is it's value under sort, where a larger one leads, and false
// where it has none.
func sortValue(it item, sort string) (float64, bool) {
	switch sort {
	case sortQuality:
		if it.answer != nil {
			return float64(it.answer.Quality), true
		}
	case sortFit:
		if it.answer != nil && it.answer.Fit != nil {
			return float64(*it.answer.Fit), true
		}
	case sortLikesPerView:
		return likesPerView(it)
	case sortLength:
		if seconds, ok := metric(it, ypl.MetricDuration); ok {
			return -float64(seconds), true
		}
	default:
		return float64(made(it).UnixNano()), true
	}
	return 0, false
}

// sorted is items under sort: each with a value before each without, larger
// values first, then newest first, then by key.
func sorted(items []item, sort string) []item {
	out := slices.Clone(items)
	slices.SortStableFunc(out, func(a, z item) int {
		av, aok := sortValue(a, sort)
		zv, zok := sortValue(z, sort)
		switch {
		case aok != zok && aok:
			return -1
		case aok != zok:
			return 1
		case av != zv:
			return cmp.Compare(zv, av)
		}
		return cmp.Or(made(z).Compare(made(a)), strings.Compare(a.Key, z.Key))
	})
	return out
}

// made is when the item's artifact was made at its source, such as a
// video's upload, and when it began to wait where its source gave no time.
func made(it item) time.Time {
	if it.SourceTS != nil {
		if at, err := time.Parse(time.RFC3339Nano, *it.SourceTS); err == nil {
			return at
		}
	}
	return since(it)
}

// metric is the count called name that the item's first sighting carrying
// it holds, and false where none does.
func metric(it item, name string) (int64, bool) {
	for _, s := range it.Sightings {
		if v, ok := s.Metrics[name]; ok {
			return v, true
		}
	}
	return 0, false
}

// likesPerView is the item's likes over its views, the worth-watching signal,
// and false where it holds no views.
func likesPerView(it item) (float64, bool) {
	views, ok := metric(it, ypl.MetricViews)
	likes, liked := metric(it, ypl.MetricLikes)
	if !ok || !liked || views <= 0 {
		return 0, false
	}
	return float64(likes) / float64(views), true
}

// channelOf is the channel that published the item: a video's channel, or a
// Reddit save's subreddit as r/<name>, and "" where no sighting names one.
func channelOf(it item) string {
	for _, s := range it.Sightings {
		if title := s.Fields[ypl.FieldChannelTitle]; title != "" {
			return title
		}
		if sub := s.Fields[redditexport.FieldSubreddit]; sub != "" {
			return "r/" + sub
		}
	}
	return ""
}

// connectorsOf is the item's connectors joined, or a dash where it has none.
func connectorsOf(it item) string {
	if len(it.Connectors) == 0 {
		return "—"
	}
	return strings.Join(it.Connectors, ", ")
}

// sourceOf is what the first column draws for an item: its channel, else the
// connectors that brought it.
func sourceOf(it item) string { return cmp.Or(channelOf(it), connectorsOf(it)) }

// verdictOf is the gate's verdict on it, and "" where it gave no answer that
// reads.
func verdictOf(it item) string {
	if it.answer == nil {
		return ""
	}
	return it.answer.Verdict
}

// length is seconds as h:mm:ss or m:ss.
func length(seconds int64) string {
	if seconds >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
	}
	return fmt.Sprintf("%d:%02d", seconds/60, seconds%60)
}

// count is n as a person reads a count at a glance: 950, 1.2k, 34k, 5.6M.
func count(n int64) string {
	switch {
	case n >= 10_000_000:
		return strconv.FormatInt(n/1_000_000, 10) + "M"
	case n >= 1_000_000:
		return strconv.FormatFloat(float64(n)/1e6, 'f', 1, 64) + "M"
	case n >= 10_000:
		return strconv.FormatInt(n/1000, 10) + "k"
	case n >= 1000:
		return strconv.FormatFloat(float64(n)/1e3, 'f', 1, 64) + "k"
	}
	return strconv.FormatInt(n, 10)
}

// percent is a ratio as a percentage to one decimal.
func percent(ratio float64) string { return strconv.FormatFloat(ratio*100, 'f', 1, 64) + "%" }

// presented is the card under key as the last frame drew it, and how long
// ago the cursor came to rest on it, for the decision a key makes now. Both
// are nil where the last frame did not draw it under the cursor.
func (b *Board) presented(key string, now time.Time) (*store.Presented, *int64) {
	rank, position := slices.Index(b.ranked, key)+1, slices.Index(b.drawn, key)+1
	it, listed := b.queue.Value().item(key)
	if rank == 0 || position == 0 || !listed || b.cursorKey != key || b.cursorAt.IsZero() {
		return nil, nil
	}
	p := &store.Presented{
		Sort: sorts[b.sortAt], Grouping: b.grouping, Rank: rank, Position: position, Shown: len(b.drawn),
		DrawnTS: b.cursorAt.UTC().Format(time.RFC3339Nano),
	}
	if a := it.answer; a != nil {
		quality, verdict, verdictSure, side, sideSure := a.Quality, a.Verdict, a.VerdictConfidence, a.Side, a.SideConfidence
		p.Quality, p.Fit, p.Verdict, p.VerdictConfidence, p.Side, p.SideConfidence = &quality, a.Fit, &verdict, &verdictSure, &side, &sideSure
	} else if g := it.Guess; g != nil {
		side, sure := g.Side, g.Confidence
		p.Side, p.SideConfidence = &side, &sure
	}
	after := max(now.Sub(b.cursorAt).Milliseconds(), 0)
	return p, &after
}

// commentsReadable says whether a source of this build reads the comments of
// it, as the route that reads them asks.
func commentsReadable(it item) bool { return comments.Reads(it.MediaType) }

// commentsWords is how the read of the item's comments stands, as the card
// says it.
func commentsWords(c *server.ReviewComments) string {
	switch {
	case c == nil:
		return "not read; c reads them"
	case c.State == server.CommentsDone:
		return "read"
	case c.State == server.CommentsFailed && c.Error != nil:
		return "the read failed: " + *c.Error
	case c.State == server.CommentsFailed:
		return "the read failed"
	}
	return "reading; the card shows them once the read lands"
}
