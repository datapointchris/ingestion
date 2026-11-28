package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/store"
)

// ScopeItemsPush admits a caller to push a link or a memo, such as a phone's
// client, which holds no other scope.
const ScopeItemsPush = "ingestion.items.push"

// Refusal codes of the push route.
const (
	// CodeInvalidLink is a url that is not an http or https URL naming a
	// host, or one that names its artifact only through a redirect, such as a
	// Reddit share link.
	CodeInvalidLink = "invalid_link"
	// CodeLinkNotRead is a link of a kind that no extractor of this build
	// reads at its source, or a page that no extractor fetches, so the push
	// stores nothing.
	CodeLinkNotRead = "link_not_read"
	// CodeMediaNotRead is a body that is neither a JSON link nor the audio of
	// a memo in a container that store.MemoMediaTypes names.
	CodeMediaNotRead = "media_not_read"
	// CodeMemoTooLarge is a memo past MaxMemoBytes.
	CodeMemoTooLarge = "memo_too_large"
)

// MaxMemoBytes bounds one pushed memo's body, and a body past it answers 413.
// It holds about 3.5 hours of audio at 64 kbps. The route streams the body to
// the blob volume, so serve's memory stays flat in the memo's size.
const MaxMemoBytes = 100 << 20

// sniffBytes bounds the opening that the route reads to tell a link from a
// memo.
const sniffBytes = 512

// mp4AudioBrands are the brands of an ISO base media file that hold audio
// alone, such as the M4A of a shared voice memo. A video's brands are none of
// them, so a pushed video is refused.
var mp4AudioBrands = []string{"M4A ", "M4B ", "M4P ", "F4A ", "F4B "}

// SniffMemo is the media type of a memo whose body opens with head, and false
// where head opens none of store.MemoMediaTypes: an MP4 whose ftyp box names an
// audio brand, an MP3 that opens with its ID3 tag or a frame, or a RIFF WAVE.
// The route reads the bytes rather than the Content-Type, because a phone's
// share may name no type or the wrong one.
func SniffMemo(head []byte) (string, bool) {
	switch {
	case mp4Audio(head):
		return store.MediaTypeAudioMP4, true
	case bytes.HasPrefix(head, []byte("ID3")) || mpegFrame(head):
		return store.MediaTypeAudioMPEG, true
	case len(head) >= 12 && string(head[:4]) == "RIFF" && string(head[8:12]) == "WAVE":
		return store.MediaTypeAudioWAV, true
	}
	return "", false
}

// mp4Audio says whether head opens an ftyp box whose major brand, or one of
// its compatible brands, is an audio brand.
func mp4Audio(head []byte) bool {
	if len(head) < 16 || string(head[4:8]) != "ftyp" {
		return false
	}
	end := min(int(binary.BigEndian.Uint32(head[:4])), len(head))
	brands := []string{string(head[8:12])}
	for at := 16; at+4 <= end; at += 4 {
		brands = append(brands, string(head[at:at+4]))
	}
	return slices.ContainsFunc(brands, func(b string) bool { return slices.Contains(mp4AudioBrands, b) })
}

// mpegFrame says whether head opens an MPEG audio frame: its sync bits, a
// version and a layer that are not reserved, and a bitrate and a sample rate
// that are not invalid. An AAC stream's ADTS header names layer 0, which is
// reserved here, so it is no MP3.
func mpegFrame(head []byte) bool {
	if len(head) < 3 || head[0] != 0xFF || head[1]&0xE0 != 0xE0 {
		return false
	}
	version, layer := (head[1]>>3)&0x3, (head[1]>>1)&0x3
	bitrate, rate := head[2]>>4, (head[2]>>2)&0x3
	return version != 1 && layer != 0 && bitrate != 0xF && rate != 3
}

// LinkLanding is how a link lands as an item, as the extractors of a build
// read it. The push route and a feed's poll both ask it, so a link lands
// alike whichever connector meets it first.
type LinkLanding struct {
	// Lands is LinkReadAtSource or LinkPage, and "" where the link does not
	// land.
	Lands string
	// MediaType and URL are what the item lands as, where the link lands.
	MediaType, URL string
	// Site is the kind of key whose site hosts a link that does not land,
	// such as youtube for a channel's page, and "" otherwise.
	Site string
}

// How a link lands.
const (
	// LinkReadAtSource is a link that an extractor reads at its source, such
	// as a video's, so its item holds no bytes.
	LinkReadAtSource = "read-at-source"
	// LinkPage is a link to a web page, whose item's bytes are the page that a
	// GET of it answers.
	LinkPage = "page"
)

// LinkExtractors answers whether an extractor reads a pushed link at its
// source, or fetches the web page that it names, and as what media type and
// URL it lands.
type LinkExtractors interface {
	// LinkLanding is how the link keyed key lands, where link is its
	// canonical URL.
	LinkLanding(key, link string) LinkLanding
	// LinkKinds names, in order, each kind of key whose link an extractor
	// reads.
	LinkKinds() []string
	// PageMediaType is the media type that a link to a web page lands as,
	// with no bytes, so that its parse fetches the page. It is false where no
	// extractor fetches pages.
	PageMediaType() (string, bool)
}

// PushRequest is what POST /api/v1/items reads as a link.
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

// push lands what the body carries, which it tells apart by the body's
// leading bytes rather than its Content-Type: a JSON object is a link, which
// lands outside, and a memo's audio lands as the owner's. Any other body
// answers 415 and stores nothing. It answers schemas/push.schema.json, and is
// keyed as a drop is.
func (h connectorHandlers) push(w http.ResponseWriter, r *http.Request) {
	body := bufio.NewReaderSize(http.MaxBytesReader(w, r.Body, MaxMemoBytes), sniffBytes)
	head, err := body.Peek(sniffBytes)
	if err != nil && !errors.Is(err, io.EOF) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the body could not be read: %v", err)
		return
	}
	opening := bytes.TrimLeft(head, " \t\r\n")
	if len(opening) == 0 {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a push carries a JSON link or a memo's audio, and this body carries nothing")
		return
	}
	if opening[0] == '{' {
		h.pushLink(w, r, body)
		return
	}
	mediaType, ok := SniffMemo(head)
	if !ok {
		Refuse(w, http.StatusUnsupportedMediaType, CodeMediaNotRead,
			"the body is neither a JSON link, which opens with {, nor a memo's audio as %s, so nothing was stored", strings.Join(store.MemoMediaTypes, ", "))
		return
	}
	h.pushMemo(w, r, body, mediaType)
}

// pushLink lands the link that body names as an outside item under its
// itemkey, through the push connector that it names. A link lands only where
// an extractor of this build reads it at its source, or fetches the web page
// that it names, because the service stores nothing that it did not fetch
// itself. A page lands with no bytes, under the link that keys it, and its
// parse GETs it on a worker. So the page that the same link names from a feed
// is the same item. Any other link answers 422 and stores nothing, as does a
// page of a site whose items an extractor reads, such as a channel's. A push
// that names its destination is the person's placement, written in the
// landing's write, so no gate and no model stage reads the item.
func (h connectorHandlers) pushLink(w http.ResponseWriter, r *http.Request, raw io.Reader) {
	key := requestKey(r)
	var body PushRequest
	if !decodeFrom(w, raw, &body) {
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
	var linked LinkLanding
	if h.cfg.Links != nil {
		linked = h.cfg.Links.LinkLanding(itemKey, link)
	}
	switch kind, _, _ := strings.Cut(itemKey, "/"); {
	case linked.Lands != "":
	case linked.Site != "":
		Refuse(w, http.StatusUnprocessableEntity, CodeLinkNotRead,
			"the link is on a %s site but names no single %s item, as a channel's or a playlist's page names none, so nothing was stored. %s",
			linked.Site, linked.Site, kindsRead(h.cfg.Links))
		return
	default:
		Refuse(w, http.StatusUnprocessableEntity, CodeLinkNotRead,
			"the link keys as %s, and no extractor of this build reads a %s link at its source, so nothing was stored. %s",
			itemKey, kind, kindsRead(h.cfg.Links))
		return
	}
	mediaType, landAt := linked.MediaType, linked.URL
	c, ok := h.pushConnector(w, r, body.Connector)
	if !ok {
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
	fingerprint := fingerprintOf(store.KeyedRoutePushes, c.Name, link, body.Destination)
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
			Connector: c.Name, Zone: store.Outside, Key: itemKey, MediaType: mediaType, URL: landAt, Tags: c.Tags,
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
		if err := h.keep(r, tx, caller, key, store.KeyedRoutePushes, c.Name, fingerprint, http.StatusCreated, answer); err != nil {
			return err
		}
		return connectors.Delivered(r.Context(), tx, c)
	})
	var undeclared *route.NotDeclaredError
	var playlist *route.PlaylistDestinationError
	switch {
	case errors.As(err, &undeclared):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationNotDeclared, "%v, so nothing was stored", undeclared)
		return
	case errors.As(err, &playlist):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationTakesPlaylist, "%v, so nothing was stored", playlist)
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

// pushMemo lands body, a memo's audio of mediaType, as the owner's item
// through the push connector that the query's connector names. Its key is
// itemkey.FromMemo of the caller and the Idempotency-Key, so a share of the
// same file sent again under that key answers what the first one landed. The
// fingerprint names the connector and the recording, and leaves recorded_at
// out, so a re-share that spells the time another way still replays. Landing
// streams the audio to the blob volume under its SHA-256, and queues its
// transcript. The query's recorded_at, an RFC 3339 time, is the item's source
// time, and without it the landing time stands. A memo past MaxMemoBytes
// answers 413.
func (h connectorHandlers) pushMemo(w http.ResponseWriter, r *http.Request, body io.Reader, mediaType string) {
	query := r.URL.Query()
	name, recordedAt := query.Get("connector"), query.Get("recorded_at")
	if name == "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidQuery, "a memo names its push connector in the query, as ?connector=phone, so nothing was stored")
		return
	}
	var recorded time.Time
	if recordedAt != "" {
		var err error
		if recorded, err = time.Parse(time.RFC3339, recordedAt); err != nil {
			Refuse(w, http.StatusBadRequest, CodeInvalidQuery,
				"recorded_at is an RFC 3339 time, such as 2026-11-27T14:02:11-05:00, not %q, so nothing was stored", recordedAt)
			return
		}
	}
	c, ok := h.pushConnector(w, r, name)
	if !ok {
		return
	}
	itemKey, err := itemkey.FromMemo(callerOf(r), requestKey(r))
	if err != nil {
		h.internal(w, r, "key the memo", err)
		return
	}
	read := &memoBody{r: body}
	hashed := sha256.New()
	audio := io.TeeReader(read, hashed)
	// fingerprint reads what of the recording Land left unread, then names
	// the whole recording.
	fingerprint := func() (string, bool) {
		if _, err := io.Copy(io.Discard, audio); err != nil {
			read.refuse(w)
			return "", false
		}
		return fingerprintOf(store.KeyedRoutePushes, c.Name, mediaType, hex.EncodeToString(hashed.Sum(nil))), true
	}
	answer, ok := h.landKeyed(w, r, keyedLanding{
		route: store.KeyedRoutePushes, connector: c, fingerprint: fingerprint,
		land: func() ([]landing.Landed, string, bool) {
			landed, err := h.cfg.Lander.Land(r.Context(), landing.Arrival{
				Connector: c.Name, Zone: store.Owner, Key: itemKey, MediaType: mediaType, Body: audio, SourceTime: recorded, Tags: c.Tags,
			})
			switch {
			case read.err != nil:
				read.refuse(w)
				return nil, "", false
			case err != nil:
				h.internal(w, r, "land the memo", err)
				return nil, "", false
			}
			sum, ok := fingerprint()
			return []landing.Landed{landed}, sum, ok
		},
	})
	if !ok {
		return
	}
	h.cfg.Log.Info("landed a memo", "connector", c.Name, "item", itemKey, "created", answer.Items[0].Created, "media_type", mediaType,
		"bytes", read.n, "sha256", hex.EncodeToString(hashed.Sum(nil)))
}

// memoBody reads a memo's body, and counts what it read. It keeps the first
// error that a read met other than io.EOF, so the route tells a body that
// failed from a landing that failed.
type memoBody struct {
	r   io.Reader
	n   int64
	err error
}

func (b *memoBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if err != nil && !errors.Is(err, io.EOF) && b.err == nil {
		b.err = err
	}
	return n, err
}

// refuse answers a body that could not be read: 413 where it ran past
// MaxMemoBytes, and 400 otherwise.
func (b *memoBody) refuse(w http.ResponseWriter) {
	var tooLarge *http.MaxBytesError
	if errors.As(b.err, &tooLarge) {
		Refuse(w, http.StatusRequestEntityTooLarge, CodeMemoTooLarge, "a memo holds at most %d bytes, so nothing was stored", MaxMemoBytes)
		return
	}
	Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the memo could not be read: %v", b.err)
}

// pushConnector is the push connector called name in the set in force, and
// false once it has refused the request: where no set holds one of that name,
// or the connector is of another mode.
func (h connectorHandlers) pushConnector(w http.ResponseWriter, r *http.Request, name string) (connectors.Connector, bool) {
	set, _, ok := h.applied(w, r)
	if !ok {
		return connectors.Connector{}, false
	}
	c, found := set.Connector(name)
	if !found {
		Refuse(w, http.StatusNotFound, CodeConnectorNotFound, "the declarations in force declare no connector %s, so nothing was stored", name)
		return connectors.Connector{}, false
	}
	if c.Mode != connectors.ModePush {
		Refuse(w, http.StatusConflict, CodeConnectorMode, "%s is a %s connector, so it takes no push", c.Name, c.Mode)
		return connectors.Connector{}, false
	}
	return c, true
}

// kindsOf names each kind of link that an extractor reads, and none where
// links is nil.
func kindsOf(links LinkExtractors) []string {
	if links == nil {
		return nil
	}
	return links.LinkKinds()
}

// kindsRead names each kind of link that an extractor reads, and a web page
// where an extractor fetches pages, in a sentence.
func kindsRead(links LinkExtractors) string {
	var named []string
	if kinds := kindsOf(links); len(kinds) > 0 {
		named = append(named, "one "+strings.Join(kinds, " or ")+" item")
	}
	if links != nil {
		if _, ok := links.PageMediaType(); ok {
			named = append(named, "a web page")
		}
	}
	if len(named) == 0 {
		return "This build lands no pushed link."
	}
	return "This build lands a pushed link only where it names " + strings.Join(named, " or ") + "."
}
