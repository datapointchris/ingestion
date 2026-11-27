package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// CodeNotShelved refuses an opening of an item that is not on the shelf.
const CodeNotShelved = "not_shelved"

// ShelfAnswer is what GET /api/v1/shelf answers:
// schemas/shelf-list.schema.json. Items are those on the shelf, the oldest
// landed first.
type ShelfAnswer struct {
	SchemaVersion int         `json:"schema_version"`
	Items         []ShelfItem `json:"items"`
}

// ShelfOpenAnswer is what POST /api/v1/shelf/{key}/open answers:
// schemas/shelf-open.schema.json. Opened says whether this request opened
// the item, and false where an earlier one had, so nothing new ran.
type ShelfOpenAnswer struct {
	SchemaVersion int       `json:"schema_version"`
	Opened        bool      `json:"opened"`
	Item          ShelfItem `json:"item"`
}

// ShelfItem is one item on the shelf and where it stands. Summary is the
// model read's answer once done, a model's over outsider text, and Error why
// it failed.
type ShelfItem struct {
	Key       string          `json:"key"`
	Zone      store.Zone      `json:"zone"`
	Title     *string         `json:"title"`
	URL       *string         `json:"url"`
	MediaType string          `json:"media_type"`
	Verb      string          `json:"verb"`
	Lens      *string         `json:"lens"`
	State     string          `json:"state"`
	SortedBy  string          `json:"sorted_by"`
	SortedTS  string          `json:"sorted_ts"`
	OpenedTS  *string         `json:"opened_ts"`
	Summary   json.RawMessage `json:"summary"`
	Error     *string         `json:"error"`
}

func shelfItem(r store.ItemRecords, s route.Shelved) ShelfItem {
	item := ShelfItem{
		Key: r.Item.Key, Zone: r.Item.Zone, Title: r.Item.Title, URL: r.Item.URL, MediaType: r.Item.MediaType,
		Verb: s.Verb, Lens: optional(s.Lens), State: s.State, SortedBy: s.Side.DecidedBy, SortedTS: s.Side.DecidedTS, Error: s.Error,
	}
	if s.Opened != nil {
		item.OpenedTS = &s.Opened.DecidedTS
	}
	if s.ReadDone != nil && len(s.ReadDone.Answer) > 0 {
		item.Summary = s.ReadDone.Answer
	}
	return item
}

func (h reviewHandlers) shelf(w http.ResponseWriter, r *http.Request) {
	candidates, err := h.store.ShelfCandidates(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the shelf", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the shelf")
		return
	}
	answer := ShelfAnswer{SchemaVersion: 1, Items: make([]ShelfItem, 0, len(candidates))}
	for _, c := range candidates {
		if s, ok := route.ShelfOf(c); ok {
			answer.Items = append(answer.Items, shelfItem(c, s))
		}
	}
	JSON(w, http.StatusOK, answer)
}

func (h reviewHandlers) open(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Login string `json:"login,omitempty"`
	}
	if r.ContentLength != 0 && !decode(w, r, &body) {
		return
	}
	key, login, clientID, ok := h.decidedItem(w, r, body.Login)
	if !ok {
		return
	}
	var answer ShelfOpenAnswer
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		s, opened, err := route.OpenShelved(r.Context(), tx, key, login, clientID)
		if err != nil {
			return err
		}
		records, err := tx.Records(r.Context(), key)
		answer = ShelfOpenAnswer{SchemaVersion: 1, Opened: opened, Item: shelfItem(records, s)}
		return err
	})
	var notShelved *route.NotShelvedError
	switch {
	case errors.As(err, &notShelved):
		Refuse(w, http.StatusConflict, CodeNotShelved, "%v", notShelved)
		return
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "open a shelved item", "error", err, "item", key)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not open the item")
		return
	}
	if answer.Opened {
		h.log.Info("a person opened a shelved item", "item", key, "verb", answer.Item.Verb, "decided_by", route.ByPerson(login), "client", clientID)
	}
	JSON(w, http.StatusOK, answer)
}
