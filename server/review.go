package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/landing"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/schemas"
	"github.com/datapointchris/ingestion/store"
)

// ScopeReviewRead admits a caller to read the items that wait on a person's
// placement. ScopeReviewPlace admits it to place or drop an item for the
// person whose login its body names.
const (
	ScopeReviewRead  = "ingestion.review.read"
	ScopeReviewPlace = "ingestion.review.place"
)

// ScopeCommentsRead admits a caller to ask for the read of what an item's top
// comments say, which costs one model call per item.
const ScopeCommentsRead = "ingestion.comments.read"

// CodeCommentsNotRead refuses a read of the comments of an item whose media
// type no source that this build carries reads comments of.
const CodeCommentsNotRead = "comments_not_read"

// ItemCommentsAnswer is what POST /api/v1/items/{key}/comments answers:
// schemas/item-comments.schema.json.
type ItemCommentsAnswer struct {
	SchemaVersion int            `json:"schema_version"`
	Key           string         `json:"key"`
	Comments      ReviewComments `json:"comments"`
}

// CodeDestinationNotDeclared refuses a placement on a destination that the
// set in force does not declare. CodeActingForAnother refuses a person's
// token whose body names another person's login. CodeChapterOfBook refuses a
// placement of a book's chapter, which takes its book's placement.
// CodeDestinationPushOnly refuses a placement from review on a push-only
// destination, which only a push places an item at.
// CodeDestinationTakesPlaylist refuses a placement on a destination whose
// deliverer adds the item to the playlist that a person picks, which only the
// watch choice places an item at. CodeChoiceRefused refuses a pick that its
// choice cannot carry out on the item, such as a watch that names no playlist
// where serve names no default.
const (
	CodeDestinationNotDeclared   = "destination_not_declared"
	CodeActingForAnother         = "acting_for_another"
	CodeChapterOfBook            = "chapter_of_book"
	CodeDestinationPushOnly      = "destination_push_only"
	CodeDestinationTakesPlaylist = "destination_takes_playlist"
	CodeChoiceRefused            = "choice_refused"
)

var errActingForAnother = errors.New("a person's token acts only as that person")

// causeSpelling is how a decision's cause is spelled, as
// schemas/ingestion-record.schema.json holds it.
var causeSpelling = regexp.MustCompile(`^[a-z][a-z-]{0,63}$`)

// ValidCause reports whether cause is spelled as the place and drop routes
// take one.
func ValidCause(cause string) bool { return causeSpelling.MatchString(cause) }

// ReviewAnswer is what GET /api/v1/review answers:
// schemas/review-list.schema.json. Destinations are those a placement by
// destination takes, in the order the set in force declares them, and Choices
// each of review's choices with the destination it places on. Both are nil
// where the set in force could not be read. WatchDefaultPlaylist is the
// playlist a watch takes where the pick names none, and nil where serve names
// none.
type ReviewAnswer struct {
	SchemaVersion        int                 `json:"schema_version"`
	Items                []ReviewItem        `json:"items"`
	Destinations         []ReviewDestination `json:"destinations"`
	Choices              []ReviewChoice      `json:"choices"`
	WatchDefaultPlaylist *string             `json:"watch_default_playlist"`
}

// ReviewChoice is one of review's choices under the set in force: the side and
// the destination it places an item on, each nil for archive and drop, whether
// its pick names a playlist, whether the client that picks it runs psyche
// relate on its own machine, and why it places nothing where the set declares
// no destination it takes.
type ReviewChoice struct {
	Choice      string  `json:"choice"`
	Side        *string `json:"side"`
	Destination *string `json:"destination"`
	Playlist    bool    `json:"playlist"`
	RunsHere    bool    `json:"runs_here"`
	Missing     *string `json:"missing"`
}

// ReviewItem is one item that waits on a person's placement, and why.
type ReviewItem struct {
	Key   string     `json:"key"`
	Zone  store.Zone `json:"zone"`
	Title *string    `json:"title"`
	URL   *string    `json:"url"`
	// Reason is why the item waits, as schemas/review-list.schema.json
	// enumerates it.
	Reason string `json:"reason"`
	// Rule is the routing rule that would have decided an item that waits as
	// rule-would-hide, and nil under any other reason.
	Rule  *ReviewRule  `json:"rule"`
	Guess *ReviewGuess `json:"guess"`
	// QualityAnswer is the quality gate's newest done answer as the store
	// keeps it, and null where the gate gave none.
	QualityAnswer json.RawMessage `json:"quality_answer"`
	// NoteDraft is the note stage's newest done draft as the store keeps it,
	// and null where none was drafted.
	NoteDraft json.RawMessage `json:"note_draft"`
	// MediaType is the item's, and SourceTS when its artifact was made at its
	// source, such as a video's upload, and null where the source gave none.
	MediaType string  `json:"media_type"`
	SourceTS  *string `json:"source_ts"`
	// Channel is who published the item, as its sightings name it, such as a
	// video's channel or a save's subreddit as r/<name>, and null where none
	// does. ChannelFields are the sighting fields that a drop decided by the
	// channel stands for, empty where a drop names the channel's field as its
	// own cause.
	Channel       *string  `json:"channel"`
	ChannelFields []string `json:"channel_fields"`
	// LengthS is how long the item runs in seconds, and Views, Likes and
	// CommentCount how it was received, each null where no sighting carries
	// it.
	LengthS      *int64 `json:"length_s"`
	Views        *int64 `json:"views"`
	Likes        *int64 `json:"likes"`
	CommentCount *int64 `json:"comment_count"`
	// Comments is how the read of what the item's top comments say stands,
	// and nil where no source of this build reads the item's comments.
	Comments       *ReviewComments  `json:"comments"`
	Connectors     []string         `json:"connectors"`
	Sightings      []ReviewSighting `json:"sightings"`
	WaitingSinceTS string           `json:"waiting_since_ts"`
}

// The states of a read of an item's comments. Unread is an item whose
// comments a source reads and that nobody asked about. Unclaimed is a read
// queued while no worker heard from lately claims its tasks.
const (
	CommentsUnread    = "unread"
	CommentsReading   = "reading"
	CommentsUnclaimed = "unclaimed"
	CommentsDone      = "done"
	CommentsFailed    = "failed"
)

// ReviewComments is how the read of an item's comments stands, and its
// answer once done. The answer is a model's over outsider text.
type ReviewComments struct {
	State  string          `json:"state"`
	Answer json.RawMessage `json:"answer"`
	TaskID *string         `json:"task_id"`
	Error  *string         `json:"error"`
}

// CommentsState is how the read of an item's comments stands in its records,
// given each capability that a live worker claims, and nil where no source
// of this build reads the item's comments. The comments stage answers it,
// so the review list reports the state that its route acts on.
type CommentsState func(r store.ItemRecords, claimed map[string]bool) *ReviewComments

// ReviewRule is the routing rule whose review share drew an item: its name,
// and the destination that a keep would place it on, which is nil where a
// block would drop it.
type ReviewRule struct {
	Name        string  `json:"name"`
	Destination *string `json:"destination"`
}

// ReviewGuess is the quality gate's side guess on an item.
type ReviewGuess struct {
	Side       string  `json:"side"`
	Confidence float64 `json:"confidence"`
}

// ReviewSighting is one connector's arrival of a waiting item: the tags the
// side gate read and the source's fields, such as a Reddit save's subreddit.
type ReviewSighting struct {
	Connector string            `json:"connector"`
	Tags      []string          `json:"tags"`
	Fields    map[string]string `json:"fields"`
	Metrics   map[string]int64  `json:"metrics"`
}

// ReviewDestination is a declared destination and the side it gives an item.
// Side is null where the destination drops what it takes.
type ReviewDestination struct {
	Name string  `json:"name"`
	Side *string `json:"side"`
}

// PlaceRequest places an item on Destination, one the set in force declares,
// or by Choice, one of review's placing choices, and names one of the two.
// Playlist is the playlist that a watch adds the video to, and a watch that
// names none takes serve's [watch] default_playlist. Cause names the aspect
// that decided it, such as subreddit, channel, theme or title, and Note the
// questions and why the item was saved. A service placing for a person names
// the person's Login, since its token carries none. Presented is the review
// card as drawn when the person decided, and DecidedAfterMS how long after the
// drawing they decided, which a client that drew no card leaves out.
type PlaceRequest struct {
	Destination    string           `json:"destination,omitempty"`
	Choice         string           `json:"choice,omitempty"`
	Playlist       string           `json:"playlist,omitempty"`
	Cause          string           `json:"cause,omitempty"`
	Note           *string          `json:"note,omitempty"`
	Login          string           `json:"login,omitempty"`
	Presented      *store.Presented `json:"presented,omitempty"`
	DecidedAfterMS *int64           `json:"decided_after_ms,omitempty"`
}

// ArchiveRequest archives an item: done, its record kept with no destination,
// and the read mark written where the caller may write marks. Liked names what
// the person liked, one of route.Likes, and an archive that names none is kept
// as unstated. Login, Presented and DecidedAfterMS are a PlaceRequest's.
type ArchiveRequest struct {
	Liked          string           `json:"liked,omitempty"`
	Login          string           `json:"login,omitempty"`
	Presented      *store.Presented `json:"presented,omitempty"`
	DecidedAfterMS *int64           `json:"decided_after_ms,omitempty"`
}

// DropRequest drops an item, which keeps its row. Cause names the aspect that
// decided it, and a drop that names none is kept as unstated. A drop for
// low-quality also writes the low-quality mark where the caller may write
// marks. Presented and DecidedAfterMS are a PlaceRequest's.
type DropRequest struct {
	Cause          string           `json:"cause,omitempty"`
	Note           *string          `json:"note,omitempty"`
	Login          string           `json:"login,omitempty"`
	Presented      *store.Presented `json:"presented,omitempty"`
	DecidedAfterMS *int64           `json:"decided_after_ms,omitempty"`
}

// presentedSchema is the record schema's presented, which a card sent with a
// placement or a drop holds to.
const presentedSchema = "ingestion-record.schema.json#/$defs/presented"

// checkPresented answers why p and after are not a card as drawn and a wait
// after it, and "" where they are, or where neither is sent. The schema states
// what a card holds. Only the pairing and the wait are checked here.
func checkPresented(p *store.Presented, after *int64) (string, error) {
	switch {
	case p == nil && after == nil:
		return "", nil
	case p == nil || after == nil:
		return "presented and decided_after_ms are sent together, or neither is", nil
	case *after < 0:
		return "decided_after_ms counts from the card's drawing, so it is 0 or more", nil
	}
	schema, err := schemas.Compiled(presentedSchema)
	if err != nil {
		return "", err
	}
	if problems := schemas.Problems(schema, p); len(problems) > 0 {
		return "presented does not hold to schemas/" + presentedSchema + ": " + strings.Join(problems, "; "), nil
	}
	return "", nil
}

// PlacementAnswer is what a placement, an archive or a drop answers:
// schemas/review-placement.schema.json. Decisions are the side decision and,
// where it places the item on a side, the read decision after it.
type PlacementAnswer struct {
	SchemaVersion int            `json:"schema_version"`
	Key           string         `json:"key"`
	Decisions     []DecisionView `json:"decisions"`
}

// ReviewRoutes are the routes for a person, or a service acting for one, to
// read the review queue, and place, archive or drop an item. A placement, an
// archive or a drop sent again with the same body appends nothing.
// watchDefault is the playlist that a watch takes where its pick names none,
// and "" where serve names none.
func ReviewRoutes(st *store.Store, comments CommentsState, watchDefault string, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	if comments == nil {
		comments = func(store.ItemRecords, map[string]bool) *ReviewComments { return nil }
	}
	h := reviewHandlers{store: st, comments: comments, watchDefault: watchDefault, log: log}
	return func(r *Router) {
		r.Handle(RouteReview, h.list)
		r.Handle(RouteReviewPlace, h.place)
		r.Handle(RouteReviewArchive, h.archive)
		r.Handle(RouteReviewDrop, h.drop)
	}
}

type reviewHandlers struct {
	store        *store.Store
	comments     CommentsState
	watchDefault string
	log          *slog.Logger
}

func (h reviewHandlers) list(w http.ResponseWriter, r *http.Request) {
	waiting, err := h.store.ReviewQueue(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the review queue", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the review queue")
		return
	}
	claimed, err := Claimed(r.Context(), h.store)
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the workers heard from lately", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the review queue")
		return
	}
	destinations, choices := h.destinations(r)
	answer := ReviewAnswer{SchemaVersion: ReviewListVersion, Items: make([]ReviewItem, 0, len(waiting)), Destinations: destinations, Choices: choices}
	if h.watchDefault != "" {
		answer.WatchDefaultPlaylist = &h.watchDefault
	}
	for _, it := range waiting {
		item := ReviewItem{
			Key: it.Item.Key, Zone: it.Item.Zone, Title: it.Item.Title, URL: it.Item.URL, Reason: it.Reason,
			MediaType: it.Item.MediaType, SourceTS: it.Item.SourceTS, Comments: h.comments(it.ItemRecords, claimed),
			Connectors: []string{}, Sightings: []ReviewSighting{}, WaitingSinceTS: it.Since,
		}
		roles := landing.ReadRoles(it.Sightings)
		item.Channel, item.ChannelFields = roles.Channel, append([]string{}, roles.ChannelFields...)
		item.LengthS, item.Views, item.Likes, item.CommentCount = roles.LengthS, roles.Views, roles.Likes, roles.Comments
		for _, s := range it.Sightings {
			item.Connectors = append(item.Connectors, s.Connector)
			item.Sightings = append(item.Sightings, ReviewSighting{Connector: s.Connector, Tags: s.Tags, Fields: s.Fields, Metrics: s.Metrics})
		}
		if side, ok := it.NewestDecision(store.GateSide); ok && it.Reason == route.CauseRuleWouldHide {
			if name, ok := side.RuleName(); ok {
				item.Rule = &ReviewRule{Name: name, Destination: side.Destination}
			}
		}
		if guess := route.GuessOf(it.Outputs); guess != nil {
			item.Guess = &ReviewGuess{Side: guess.Side, Confidence: guess.Confidence}
		}
		if quality := store.NewestDone(it.Outputs, store.StageQuality); quality != nil && len(quality.Answer) > 0 {
			item.QualityAnswer = quality.Answer
		}
		if draft := store.NewestDone(it.Outputs, store.StageNote); draft != nil && len(draft.Answer) > 0 {
			item.NoteDraft = draft.Answer
		}
		answer.Items = append(answer.Items, item)
	}
	JSON(w, http.StatusOK, answer)
}

// destinations are those the set in force declares, but for each push-only
// one and each that takes a playlist, which a placement by destination
// refuses, and each of review's choices with where it places an item under
// that set. A set that cannot be read answers nil for both, which the answer
// carries as null, so the queue itself still reads and a reader tells the
// fault from a set that declares none.
func (h reviewHandlers) destinations(r *http.Request) ([]ReviewDestination, []ReviewChoice) {
	set, err := connectors.AppliedSet(r.Context(), h.store)
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the set in force for the review queue's destinations", "error", err)
		return nil, nil
	}
	out := make([]ReviewDestination, 0, len(set.Destinations))
	for _, d := range set.Destinations {
		if !d.PushOnly && !connectors.TakesPlaylist(d.Deliverer) {
			out = append(out, ReviewDestination{Name: d.Name, Side: optional(d.Side)})
		}
	}
	choices := make([]ReviewChoice, 0, len(route.Choices))
	for _, c := range route.Choices {
		choice := ReviewChoice{Choice: c.Name, Playlist: c.TakesPlaylist(), RunsHere: c.RunsHere}
		destination, missing := c.Destination(set)
		if destination != nil {
			choice.Side, choice.Destination = &destination.Side, &destination.Name
		}
		choice.Missing = optional(missing)
		choices = append(choices, choice)
	}
	return out, choices
}

func (h reviewHandlers) place(w http.ResponseWriter, r *http.Request) {
	var body PlaceRequest
	if !decode(w, r, &body) {
		return
	}
	if (body.Destination == "") == (body.Choice == "") {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a placement names its destination or its choice, one of %s, and not both", strings.Join(route.PlacingChoiceNames(), ", "))
		return
	}
	if body.Cause != "" && !ValidCause(body.Cause) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "cause is a lowercase word or words joined by hyphens, such as subreddit, not %q", body.Cause)
		return
	}
	if h.refusedPresented(w, r, body.Presented, body.DecidedAfterMS) {
		return
	}
	if body.Choice != "" {
		h.choose(w, r, body)
		return
	}
	if body.Playlist != "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "only the %s choice adds to a playlist, so a placement by destination names none", route.ChoiceWatch)
		return
	}
	h.decide(w, r, route.Placement{
		Destination: body.Destination, Cause: body.Cause, Note: body.Note, Presented: body.Presented, DecidedAfterMS: body.DecidedAfterMS,
	}, body.Login)
}

func (h reviewHandlers) drop(w http.ResponseWriter, r *http.Request) {
	var body DropRequest
	if !decode(w, r, &body) {
		return
	}
	if body.Cause != "" && !ValidCause(body.Cause) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "cause is a lowercase word or words joined by hyphens, such as subreddit, not %q", body.Cause)
		return
	}
	if h.refusedPresented(w, r, body.Presented, body.DecidedAfterMS) {
		return
	}
	h.decide(w, r, route.Placement{Cause: body.Cause, Note: body.Note, Presented: body.Presented, DecidedAfterMS: body.DecidedAfterMS, Unmarked: !mayMark(r)}, body.Login)
}

// choose picks the item by body's choice, which place has checked but for
// the choice itself.
func (h reviewHandlers) choose(w http.ResponseWriter, r *http.Request, body PlaceRequest) {
	c, ok := route.ChoiceOf(body.Choice)
	if !ok || !c.Places() {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "choice is one of %s, not %q: archive and drop each take a route of their own",
			strings.Join(route.PlacingChoiceNames(), ", "), body.Choice)
		return
	}
	if body.Playlist != "" && !c.TakesPlaylist() {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "the %s choice adds to no playlist, so it names none", body.Choice)
		return
	}
	if body.Note != nil && strings.TrimSpace(*body.Note) == "" {
		body.Note = nil
	}
	key, login, clientID, ok := h.decidedItem(w, r, body.Login)
	if !ok {
		return
	}
	var made []store.Decision
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		made, err = route.Choose(r.Context(), tx, key, route.Pick{
			Choice: body.Choice, Playlist: body.Playlist, DefaultPlaylist: h.watchDefault, Cause: body.Cause, Note: body.Note,
			Login: login, ClientID: clientID, Presented: body.Presented, DecidedAfterMS: body.DecidedAfterMS,
		})
		return err
	})
	var refused *route.ChoiceRefusedError
	if errors.As(err, &refused) {
		Refuse(w, http.StatusUnprocessableEntity, CodeChoiceRefused, "%v", refused)
		return
	}
	if h.refusedDecision(w, r, key, err) {
		return
	}
	h.log.Info("a person chose for an item", "item", key, "choice", body.Choice, "decided_by", made[0].DecidedBy, "client", clientID, "cause", made[0].Cause)
	JSON(w, http.StatusOK, placementAnswer(key, made))
}

func (h reviewHandlers) archive(w http.ResponseWriter, r *http.Request) {
	var body ArchiveRequest
	if !decode(w, r, &body) {
		return
	}
	if body.Liked != "" && !slices.Contains(route.Likes, body.Liked) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "liked is one of %s, or left out, not %q", strings.Join(route.Likes, ", "), body.Liked)
		return
	}
	if h.refusedPresented(w, r, body.Presented, body.DecidedAfterMS) {
		return
	}
	key, login, clientID, ok := h.decidedItem(w, r, body.Login)
	if !ok {
		return
	}
	unmarked := !mayMark(r)
	var made []store.Decision
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		made, err = route.Archive(r.Context(), tx, key, route.Archival{
			Liked: body.Liked, Login: login, ClientID: clientID, Presented: body.Presented, DecidedAfterMS: body.DecidedAfterMS, Unmarked: unmarked,
		})
		return err
	})
	if h.refusedDecision(w, r, key, err) {
		return
	}
	h.log.Info("a person archived an item", "item", key, "decided_by", made[0].DecidedBy, "client", clientID, "cause", made[0].Cause, "unmarked", unmarked)
	JSON(w, http.StatusOK, placementAnswer(key, made))
}

// placementAnswer is the answer of a decision on the item under key that made
// made.
func placementAnswer(key string, made []store.Decision) PlacementAnswer {
	answer := PlacementAnswer{SchemaVersion: PlacementVersion, Key: key, Decisions: make([]DecisionView, 0, len(made))}
	for _, d := range made {
		answer.Decisions = append(answer.Decisions, decisionView(d))
	}
	return answer
}

// decidedItem reads the item that the path names and who decides it, and
// refuses the request where either does not read.
func (h reviewHandlers) decidedItem(w http.ResponseWriter, r *http.Request, body string) (key, login, clientID string, ok bool) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
		return "", "", "", false
	}
	if login, clientID, err = decider(r, body); errors.Is(err, errActingForAnother) {
		Refuse(w, http.StatusForbidden, CodeActingForAnother, "%v", err)
		return "", "", "", false
	} else if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%v", err)
		return "", "", "", false
	}
	return key, login, clientID, true
}

// refusedDecision refuses the request where err, from a decision on the item
// under key, is one, and says whether it did.
func (h reviewHandlers) refusedDecision(w http.ResponseWriter, r *http.Request, key string, err error) bool {
	var undeclared *route.NotDeclaredError
	var chapter *route.ChapterError
	var pushOnly *route.PushOnlyError
	var playlist *route.PlaylistDestinationError
	switch {
	case err == nil:
		return false
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
	case errors.As(err, &undeclared):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationNotDeclared, "%v, so nothing was placed", undeclared)
	case errors.As(err, &pushOnly):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationPushOnly, "%v, and nothing was placed", pushOnly)
	case errors.As(err, &playlist):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationTakesPlaylist, "%v, and nothing was placed", playlist)
	case errors.As(err, &chapter):
		Refuse(w, http.StatusUnprocessableEntity, CodeChapterOfBook, "%v", chapter)
	default:
		h.log.ErrorContext(r.Context(), "decide an item", "error", err, "item", key)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not decide the item")
	}
	return true
}

// refusedPresented refuses the request where p and after are not a card as
// drawn and a wait after it, and says whether it did.
func (h reviewHandlers) refusedPresented(w http.ResponseWriter, r *http.Request, p *store.Presented, after *int64) bool {
	why, err := checkPresented(p, after)
	switch {
	case err != nil:
		h.log.ErrorContext(r.Context(), "check a placement's card", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not check the card sent with the decision")
		return true
	case why != "":
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%s", why)
		return true
	}
	return false
}

// decide appends p on the item the path names, as the caller decided it.
func (h reviewHandlers) decide(w http.ResponseWriter, r *http.Request, p route.Placement, login string) {
	if p.Note != nil && strings.TrimSpace(*p.Note) == "" {
		p.Note = nil
	}
	key, login, clientID, ok := h.decidedItem(w, r, login)
	if !ok {
		return
	}
	p.Login, p.ClientID = login, clientID
	var made []store.Decision
	err := h.store.Update(r.Context(), func(tx *store.Tx) error {
		var err error
		made, err = route.Place(r.Context(), tx, key, p)
		return err
	})
	if h.refusedDecision(w, r, key, err) {
		return
	}
	answer := placementAnswer(key, made)
	h.log.Info("a person decided an item", "item", key, "verdict", made[0].Verdict, "destination", p.Destination,
		"decided_by", made[0].DecidedBy, "client", p.ClientID, "cause", made[0].Cause, "unmarked", p.Unmarked)
	JSON(w, http.StatusOK, answer)
}

// decider answers who decides a placement, a mark or a promotion, by login,
// and the client that sent it. A person's token decides as the person, and
// refuses a body naming another login. A service's token carries no person,
// so its body names the login of the person it acts for.
func decider(r *http.Request, login string) (string, string, error) {
	caller, _ := goapiauth.IdentityFrom(r.Context())
	if caller.Service {
		if !loginSpelling(login) {
			return "", "", fmt.Errorf("a service acts for a person, so its body names that person's login, not %q", login)
		}
		return login, caller.ClientID, nil
	}
	own := strings.TrimPrefix(requester(caller), store.DecidedByPerson(""))
	if login != "" && login != own {
		return "", "", fmt.Errorf("%w: this token is %s's, so a body naming %s is refused", errActingForAnother, own, login)
	}
	return own, caller.ClientID, nil
}

// loginSpelling says whether login reads as one: printed characters with no
// space, at most 200 of them.
func loginSpelling(login string) bool {
	if login == "" || len(login) > 200 {
		return false
	}
	return !strings.ContainsFunc(login, func(c rune) bool { return unicode.IsSpace(c) || !unicode.IsPrint(c) })
}
