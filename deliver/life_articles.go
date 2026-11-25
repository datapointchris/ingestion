package deliver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/store"
)

// LifeArticles is the deliverer that saves each item as an article in life's
// reading list, through life's POST /api/v1/articles with the item's key as
// the article's source_key.
const LifeArticles = "life-articles"

// LifeArticlesScopes is what the life client asks its token for: the one
// create that the deliverer saves through.
var LifeArticlesScopes = []string{"life.articles.write"}

func init() { connectors.RegisterDeliverer(LifeArticles) }

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

// lifeArticle is the create's body, life's ArticleCreate. life's body holds
// no zone, so an article does not carry its item's zone.
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
// delivery is refused. The save date is when the item first landed, so a
// repeat sends the same body.
func (l lifeArticles) Deliver(ctx context.Context, item Item) Outcome {
	if item.URL == nil || *item.URL == "" {
		return permanent("the item holds no URL, which a life article needs")
	}
	landed, err := time.Parse(store.StampLayout, item.LandedTS)
	if err != nil {
		return permanent(fmt.Sprintf("the item's landing time %q is not a store stamp", item.LandedTS))
	}
	title := *item.URL
	if item.Title != nil && strings.TrimSpace(*item.Title) != "" {
		title = *item.Title
	}
	body, err := json.Marshal(lifeArticle{Title: title, URL: *item.URL, SaveDate: landed.UTC().Format(time.RFC3339), SourceKey: item.Key})
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
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusUnprocessableEntity:
		return permanent(fmt.Sprintf("life refused the article with %d: %s", resp.StatusCode, quoted(raw)))
	}
	return pending(fmt.Sprintf("life answered %d: %s", resp.StatusCode, quoted(raw)))
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
