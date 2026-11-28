package deliver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// LifeArticles is the deliverer that saves each item as an article in life's
// reading list, through life's POST /api/v1/articles with the item's key as
// the article's source_key.
const LifeArticles = route.ArticlesDeliverer

// LifeArticlesScopes is what the life client asks its token for: the one
// create that the deliverer saves through.
var LifeArticlesScopes = []string{"life.articles.write"}

func init() {
	Register(Registration{Name: LifeArticles, Scopes: LifeArticlesScopes, Takes: lifeTakes, Open: NewLifeArticles})
}

// errNoURL is an item that life-articles cannot take.
var errNoURL = errors.New("it holds no URL, which a life article needs")

// lifeTakes takes an item that holds a URL.
func lifeTakes(item store.Item) error {
	if item.URL == nil || *item.URL == "" {
		return errNoURL
	}
	return nil
}

const (
	// lifeTimeout bounds one create, so a life that hangs frees its worker
	// slot and the task tries again.
	lifeTimeout = 30 * time.Second
	// answerQuoted bounds the runes of life's answer that a delivery keeps.
	answerQuoted = 300
)

// NewLifeArticles is the life-articles deliverer for the life API at api. It
// calls life through client, which carries the token of a service client
// holding LifeArticlesScopes.
func NewLifeArticles(api string, client *http.Client) Deliverer {
	return lifeArticles{create: strings.TrimRight(api, "/") + "/api/v1/articles", client: client}
}

type lifeArticles struct {
	create string
	client *http.Client
}

// lifeArticle is the create's body, following life's ArticleCreate. It sends
// no zone, so the item's zone does not reach life.
type lifeArticle struct {
	Title     string `json:"title"`
	URL       string `json:"url"`
	SaveDate  string `json:"save_date"`
	SourceKey string `json:"source_key"`
}

// lifeSaved is life's 200 and 201 answer, its ArticleSaved.
type lifeSaved struct {
	ID      int64 `json:"id"`
	Adopted bool  `json:"adopted"`
}

// lifeConflict is life's 409 answer, its ArticleConflict: source_key where
// the key holds another body, and url where another key holds the URL.
type lifeConflict struct {
	Conflict  string  `json:"conflict"`
	ArticleID int64   `json:"article_id"`
	SourceKey *string `json:"source_key"`
}

// Deliver saves item as an article. life answers 201 for a new article and
// 200 for the article that the key already names. A 200 with adopted true is
// an article saved by hand under no key that took the key, which is
// delivered-to-existing. Where life answers 409 naming an article, the
// delivery is refused. A 5xx, 408 or 429 leaves it pending to try again, and
// any other answer fails the task, since sending the same request cannot mend
// it. The save date is when the item was made at its source, such as a Reddit
// post's creation, or when it first landed where its source gave no time. The
// title, URL and source time are the delivery's as first queued, so a repeat
// sends the same body.
func (l lifeArticles) Deliver(ctx context.Context, item Item) Outcome {
	if err := lifeTakes(store.Item{URL: item.URL}); err != nil {
		return permanent("the item " + err.Error())
	}
	saved, err := saveDate(item)
	if err != nil {
		return permanent(err.Error())
	}
	title := *item.URL
	if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
		title = *item.Title
	}
	body, err := json.Marshal(lifeArticle{Title: title, URL: *item.URL, SaveDate: saved.Format(time.RFC3339), SourceKey: item.Key})
	if err != nil {
		return permanent(err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, lifeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.create, bytes.NewReader(body))
	if err != nil {
		return permanent(err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := l.client.Do(req)
	if err != nil {
		return pending(fmt.Sprintf("the create could not reach life: %v", err))
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return pending(fmt.Sprintf("life's %d answer broke off: %v", resp.StatusCode, err))
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		var saved lifeSaved
		if json.Unmarshal(raw, &saved) != nil || saved.ID <= 0 {
			return pending(fmt.Sprintf("life answered %d naming no article: %s", resp.StatusCode, quoted(raw)))
		}
		id := strconv.FormatInt(saved.ID, 10)
		if saved.Adopted {
			return Outcome{Outcome: store.DeliveryDeliveredToExisting, DestinationID: &id}
		}
		return Outcome{Outcome: store.DeliveryDelivered, DestinationID: &id}
	case http.StatusConflict:
		var c lifeConflict
		if json.Unmarshal(raw, &c) != nil || c.ArticleID <= 0 || (c.Conflict != "source_key" && c.Conflict != "url") {
			return pending("life answered 409 naming no article: " + quoted(raw))
		}
		id := strconv.FormatInt(c.ArticleID, 10)
		why := fmt.Sprintf("life's article %d holds this key from another body", c.ArticleID)
		if c.Conflict == "url" {
			why = fmt.Sprintf("life's article %d holds this URL under source_key %s", c.ArticleID, orNone(c.SourceKey))
		}
		return Outcome{Outcome: store.DeliveryRefused, DestinationID: &id, Error: &why}
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests {
		return pending(fmt.Sprintf("life answered %d: %s", resp.StatusCode, quoted(raw)))
	}
	return permanent(fmt.Sprintf("life answered %d, which the same request cannot mend: %s", resp.StatusCode, quoted(raw)))
}

// saveDate is the item's source time, else its landing time, in UTC.
func saveDate(item Item) (time.Time, error) {
	if item.SourceTS != nil {
		at, err := time.Parse(store.StampLayout, *item.SourceTS)
		if err != nil {
			return at, fmt.Errorf("the item's source time %q is not a store stamp", *item.SourceTS)
		}
		return at.UTC(), nil
	}
	at, err := time.Parse(store.StampLayout, item.LandedTS)
	if err != nil {
		return at, fmt.Errorf("the item's landing time %q is not a store stamp", item.LandedTS)
	}
	return at.UTC(), nil
}

func pending(why string) Outcome { return Outcome{Outcome: store.DeliveryPending, Error: &why} }

func permanent(why string) Outcome {
	o := pending(why)
	o.Permanent = true
	return o
}

// quoted is the message of life's error answer, {"error", "message"}, else
// the answer itself, cut to answerQuoted runes.
func quoted(raw []byte) string {
	var answer struct {
		Message string `json:"message"`
	}
	text := strings.TrimSpace(string(raw))
	if json.Unmarshal(raw, &answer) == nil && answer.Message != "" {
		text = answer.Message
	}
	if runes := []rune(text); len(runes) > answerQuoted {
		text = string(runes[:answerQuoted]) + "…"
	}
	return text
}

func orNone(s *string) string {
	if s == nil {
		return "none"
	}
	return *s
}
