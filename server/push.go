package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// Refusal codes of the push route.
const (
	// CodeInvalidLink is a url that is not an http or https URL naming a
	// host, or one that names its artifact only through a redirect, such as a
	// Reddit share link.
	CodeInvalidLink = "invalid_link"
	// CodeLinkNotRead is a link of a kind that no extractor of this build
	// reads at its source, so the push stores nothing.
	CodeLinkNotRead = "link_not_read"
)

// LinkExtractors answers whether an extractor reads a pushed link at its
// source, and as what media type.
type LinkExtractors interface {
	// LinkMediaType is the media type that a link keyed key lands as, and
	// false where no extractor reads such a link.
	LinkMediaType(key string) (string, bool)
	// LinkKinds names, in order, each kind of key whose link an extractor
	// reads.
	LinkKinds() []string
}

// PushRequest is what POST /api/v1/items reads.
type PushRequest struct {
	// Connector names the push connector that the link arrives through, one
	// that the set in force declares.
	Connector string `json:"connector"`
	// URL is the link, an http or https URL naming a host.
	URL string `json:"url"`
	// Destination is where the person places the item, one that the set in
	// force declares. Empty leaves the item to the gates.
	Destination string `json:"destination,omitempty"`
}

// push lands the link that the body names as an outside item under its
// itemkey, through the push connector that it names. It answers
// schemas/push.schema.json, and is keyed as a drop is. A link lands only
// where an extractor of this build reads it at its source, because the
// service stores nothing that it did not fetch that way. Any other link
// answers 422 and stores nothing. A push that names its destination is the
// person's placement, written in the landing's write, so no gate and no model
// stage reads the item.
func (h connectorHandlers) push(w http.ResponseWriter, r *http.Request) {
	key := requestKey(r)
	var body PushRequest
	if !decode(w, r, &body) {
		return
	}
	if body.Connector == "" || body.URL == "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a push names its connector and its url")
		return
	}
	link, err := itemkey.CanonicalURL(body.URL)
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidLink, "%v, so nothing was stored", err)
		return
	}
	itemKey, err := itemkey.FromURL(link)
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidLink, "%v, so nothing was stored", err)
		return
	}
	mediaType, read := "", false
	if h.cfg.Links != nil {
		mediaType, read = h.cfg.Links.LinkMediaType(itemKey)
	}
	if !read {
		kind, _, _ := strings.Cut(itemKey, "/")
		Refuse(w, http.StatusUnprocessableEntity, CodeLinkNotRead,
			"the link keys as %s, and no extractor of this build reads a %s link at its source, so nothing was stored. %s",
			itemKey, kind, kindsRead(h.cfg.Links))
		return
	}
	set, _, ok := h.applied(w, r)
	if !ok {
		return
	}
	c, found := set.Connector(body.Connector)
	if !found {
		Refuse(w, http.StatusNotFound, CodeConnectorNotFound, "the declarations in force declare no connector %s, so nothing was stored", body.Connector)
		return
	}
	if c.Mode != connectors.ModePush {
		Refuse(w, http.StatusConflict, CodeConnectorMode, "%s is a %s connector, so it takes no pushed link", c.Name, c.Mode)
		return
	}
	var placement route.Placement
	if body.Destination != "" {
		placement = route.Placement{Destination: body.Destination, Pushed: true}
		if placement.Login, placement.ClientID, err = decider(r, ""); err != nil {
			Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%v", err)
			return
		}
	}
	caller := callerOf(r)
	fingerprint := fingerprintOf("items", c.Name, link, body.Destination)
	var (
		answer PushAnswer
		replay *store.KeyedRequest
	)
	err = h.cfg.Store.Update(r.Context(), func(tx *store.Tx) error {
		prior, found, err := tx.KeyedRequest(r.Context(), caller, key)
		if err != nil || found {
			replay = &prior
			return err
		}
		landed, err := h.cfg.Lander.LandIn(r.Context(), tx, landing.Arrival{
			Connector: c.Name, Zone: store.Outside, Key: itemKey, MediaType: mediaType, URL: link, Tags: c.Tags,
		})
		if err != nil {
			return err
		}
		if body.Destination != "" {
			if _, err := route.Place(r.Context(), tx, itemKey, placement); err != nil {
				return err
			}
		}
		item, _, err := tx.Item(r.Context(), itemKey)
		if err != nil {
			return err
		}
		answer = PushAnswer{SchemaVersion: 1, Items: []PushedItem{{Key: itemKey, Zone: item.Zone, Created: landed.ItemAdded}}}
		if err := h.keep(r, tx, caller, key, "items", c.Name, fingerprint, http.StatusCreated, answer); err != nil {
			return err
		}
		return connectors.Delivered(r.Context(), tx, c)
	})
	var undeclared *route.NotDeclaredError
	switch {
	case errors.As(err, &undeclared):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationNotDeclared, "%v, so nothing was stored", undeclared)
		return
	case err != nil:
		h.internal(w, r, "land the push", err)
		return
	case replay != nil:
		h.answerReplay(w, *replay, fingerprint)
		return
	}
	h.cfg.Log.Info("landed a push", "connector", c.Name, "item", itemKey, "created", answer.Items[0].Created, "destination", body.Destination)
	JSON(w, http.StatusCreated, answer)
}

// kindsRead names each kind of link that an extractor reads, in a sentence.
func kindsRead(links LinkExtractors) string {
	if links == nil || len(links.LinkKinds()) == 0 {
		return "This build lands no pushed link."
	}
	return "This build lands a pushed " + strings.Join(links.LinkKinds(), " or ") + " link alone."
}
