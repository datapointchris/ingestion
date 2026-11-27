package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/datapointchris/goapiauth"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/itemkey"
	"github.com/datapointchris/ingestion/route"
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
const (
	CodeDestinationNotDeclared = "destination_not_declared"
	CodeActingForAnother       = "acting_for_another"
	CodeChapterOfBook          = "chapter_of_book"
	CodeDestinationPushOnly    = "destination_push_only"
)

var errActingForAnother = errors.New("a person's token acts only as that person")

// causeSpelling is how a decision's cause is spelled, as
// schemas/ingestion-record.schema.json holds it.
var causeSpelling = regexp.MustCompile(`^[a-z][a-z-]{0,63}$`)

// ValidCause reports whether cause is spelled as the place and drop routes
// take one.
func ValidCause(cause string) bool { return causeSpelling.MatchString(cause) }

// ReviewAnswer is what GET /api/v1/review answers:
// schemas/review-list.schema.json. Destinations are those a placement takes,
// in the order the set in force declares them, and nil where the set in force
// could not be read.
type ReviewAnswer struct {
	SchemaVersion int                 `json:"schema_version"`
	Items         []ReviewItem        `json:"items"`
	Destinations  []ReviewDestination `json:"destinations"`
}

// ReviewItem is one item that waits on a person's placement, and why.
type ReviewItem struct {
	Key   string     `json:"key"`
	Zone  store.Zone `json:"zone"`
	Title *string    `json:"title"`
	URL   *string    `json:"url"`
	// Reason is why the item waits, as schemas/review-list.schema.json
	// enumerates it.
	Reason string       `json:"reason"`
	Guess  *ReviewGuess `json:"guess"`
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
	// Comments is the read of what the item's top comments say, and nil
	// where nobody asked for one.
	Comments       *ReviewComments  `json:"comments"`
	Connectors     []string         `json:"connectors"`
	Sightings      []ReviewSighting `json:"sightings"`
	WaitingSinceTS string           `json:"waiting_since_ts"`
}

// The states of a read of an item's comments.
const (
	CommentsReading = "reading"
	CommentsDone    = "done"
	CommentsFailed  = "failed"
)

// ReviewComments is how the read of an item's comments stands: reading while
// its task or its model's run has not ended, done with the answer, or failed.
// The answer is a model's over outsider text.
type ReviewComments struct {
	State  string          `json:"state"`
	Answer json.RawMessage `json:"answer"`
	TaskID *string         `json:"task_id"`
	Error  *string         `json:"error"`
}

// CommentsOf is how the read of the item's comments stands in r, and nil
// where it holds no task of the stage. A done answer stands whatever a later
// task does.
func CommentsOf(r store.ItemRecords) *ReviewComments {
	var newest *store.Task
	for i := range r.Tasks {
		if t := &r.Tasks[i]; t.Stage == store.StageComments && (newest == nil || t.CreatedTS >= newest.CreatedTS) {
			newest = t
		}
	}
	if done := store.NewestDone(r.Outputs, store.StageComments); done != nil {
		c := &ReviewComments{State: CommentsDone, Answer: done.Answer}
		if newest != nil {
			c.TaskID = &newest.ID
		}
		return c
	}
	if newest == nil {
		return nil
	}
	c := &ReviewComments{State: CommentsReading, TaskID: &newest.ID}
	if newest.State == store.TaskFailed {
		c.State, c.Error = CommentsFailed, newest.Error
	}
	return c
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

// PlaceRequest places an item on Destination, one the set in force declares.
// Cause names the aspect that decided it, such as subreddit, channel, theme or
// title, and Note the questions and why the item was saved. A service placing
// for a person names the person's Login, since its token carries none.
// Presented is the review card as drawn when the person decided, and
// DecidedAfterMS how long after the drawing they decided, which a client that
// drew no card leaves out.
type PlaceRequest struct {
	Destination    string           `json:"destination"`
	Cause          string           `json:"cause,omitempty"`
	Note           *string          `json:"note,omitempty"`
	Login          string           `json:"login,omitempty"`
	Presented      *store.Presented `json:"presented,omitempty"`
	DecidedAfterMS *int64           `json:"decided_after_ms,omitempty"`
}

// DropRequest drops an item, which keeps its row. Cause names the aspect that
// decided it, and a drop that names none is kept as unstated. Presented and
// DecidedAfterMS are a PlaceRequest's.
type DropRequest struct {
	Cause          string           `json:"cause,omitempty"`
	Note           *string          `json:"note,omitempty"`
	Login          string           `json:"login,omitempty"`
	Presented      *store.Presented `json:"presented,omitempty"`
	DecidedAfterMS *int64           `json:"decided_after_ms,omitempty"`
}

// The sorts and groupings a review card's Presented names, as
// schemas/ingestion-record.schema.json enumerates them.
var (
	PresentedSorts     = []string{"newest", "quality", "likes-per-view", "length", "fit"}
	PresentedGroupings = []string{"reason", "connector", "channel", "verdict", "none"}
)

// checkPresented answers why p and after are not a card as drawn and a wait
// after it, and "" where they are, or where neither is sent.
func checkPresented(p *store.Presented, after *int64) string {
	switch {
	case p == nil && after == nil:
		return ""
	case p == nil || after == nil:
		return "presented and decided_after_ms are sent together, or neither is"
	case *after < 0:
		return "decided_after_ms counts from the card's drawing, so it is 0 or more"
	case !slices.Contains(PresentedSorts, p.Sort):
		return fmt.Sprintf("presented.sort is one of %s, not %q", strings.Join(PresentedSorts, ", "), p.Sort)
	case !slices.Contains(PresentedGroupings, p.Grouping):
		return fmt.Sprintf("presented.grouping is one of %s, not %q", strings.Join(PresentedGroupings, ", "), p.Grouping)
	case p.Rank < 1 || p.Position < 1 || p.Shown < 1:
		return "presented.rank, position and shown count from 1"
	case p.Quality != nil && (*p.Quality < 1 || *p.Quality > 10), p.Fit != nil && (*p.Fit < 0 || *p.Fit > 10):
		return "presented.quality is 1 to 10 and presented.fit 0 to 10"
	case p.Verdict != nil && !slices.Contains([]string{"pass", "drop", "unsure"}, *p.Verdict),
		p.Side != nil && !slices.Contains([]string{connectors.SidePersonal, connectors.SideFleet, "unsure"}, *p.Side):
		return "presented.verdict is pass, drop or unsure, and presented.side personal, fleet or unsure"
	case outsideUnit(p.VerdictConfidence) || outsideUnit(p.SideConfidence):
		return "presented's confidences are 0 to 1"
	}
	if _, err := time.Parse(time.RFC3339Nano, p.DrawnTS); err != nil {
		return "presented.drawn_ts is an RFC 3339 time, not " + strconv.Quote(p.DrawnTS)
	}
	return ""
}

func outsideUnit(c *float64) bool { return c != nil && (*c < 0 || *c > 1) }

// PlacementAnswer is what a placement or a drop answers:
// schemas/review-placement.schema.json. Decisions are the side decision and,
// where it places the item on a side, the read decision after it.
type PlacementAnswer struct {
	SchemaVersion int            `json:"schema_version"`
	Key           string         `json:"key"`
	Decisions     []DecisionView `json:"decisions"`
}

// ReviewRoutes are the routes for a person, or a service acting for one, to
// read the review queue and place or drop an item. A placement or a drop sent
// again with the same body appends nothing.
func ReviewRoutes(st *store.Store, log *slog.Logger) Routes {
	if log == nil {
		log = slog.Default()
	}
	h := reviewHandlers{store: st, log: log}
	return func(r *Router) {
		r.Handle(RouteReview, h.list)
		r.Handle(RouteReviewPlace, h.place)
		r.Handle(RouteReviewDrop, h.drop)
	}
}

type reviewHandlers struct {
	store *store.Store
	log   *slog.Logger
}

func (h reviewHandlers) list(w http.ResponseWriter, r *http.Request) {
	waiting, err := h.store.ReviewQueue(r.Context())
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the review queue", "error", err)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not read the review queue")
		return
	}
	answer := ReviewAnswer{SchemaVersion: 3, Items: make([]ReviewItem, 0, len(waiting)), Destinations: h.destinations(r)}
	for _, it := range waiting {
		item := ReviewItem{
			Key: it.Item.Key, Zone: it.Item.Zone, Title: it.Item.Title, URL: it.Item.URL, Reason: it.Reason,
			MediaType: it.Item.MediaType, SourceTS: it.Item.SourceTS, Comments: CommentsOf(it.ItemRecords),
			Connectors: []string{}, Sightings: []ReviewSighting{}, WaitingSinceTS: it.Since,
		}
		for _, s := range it.Sightings {
			item.Connectors = append(item.Connectors, s.Connector)
			item.Sightings = append(item.Sightings, ReviewSighting{Connector: s.Connector, Tags: s.Tags, Fields: s.Fields, Metrics: s.Metrics})
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
// one, which a placement from review refuses. A set that cannot be read
// answers nil, which the answer carries as null, so the queue itself still
// reads and a reader tells the fault from a set that declares none.
func (h reviewHandlers) destinations(r *http.Request) []ReviewDestination {
	set, err := connectors.AppliedSet(r.Context(), h.store)
	if err != nil {
		h.log.ErrorContext(r.Context(), "read the set in force for the review queue's destinations", "error", err)
		return nil
	}
	out := make([]ReviewDestination, 0, len(set.Destinations))
	for _, d := range set.Destinations {
		if !d.PushOnly {
			out = append(out, ReviewDestination{Name: d.Name, Side: optional(d.Side)})
		}
	}
	return out
}

func (h reviewHandlers) place(w http.ResponseWriter, r *http.Request) {
	var body PlaceRequest
	if !decode(w, r, &body) {
		return
	}
	if body.Destination == "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "a placement names its destination")
		return
	}
	if body.Cause != "" && !ValidCause(body.Cause) {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "cause is a lowercase word or words joined by hyphens, such as subreddit, not %q", body.Cause)
		return
	}
	if why := checkPresented(body.Presented, body.DecidedAfterMS); why != "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%s", why)
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
	if why := checkPresented(body.Presented, body.DecidedAfterMS); why != "" {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%s", why)
		return
	}
	h.decide(w, r, route.Placement{Cause: body.Cause, Note: body.Note, Presented: body.Presented, DecidedAfterMS: body.DecidedAfterMS}, body.Login)
}

// decide appends p on the item the path names, as the caller decided it.
func (h reviewHandlers) decide(w http.ResponseWriter, r *http.Request, p route.Placement, login string) {
	key, err := itemkey.Resolve(r.PathValue("key"))
	if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidKey, "%v", err)
		return
	}
	if p.Note != nil && strings.TrimSpace(*p.Note) == "" {
		p.Note = nil
	}
	if p.Login, p.ClientID, err = decider(r, login); errors.Is(err, errActingForAnother) {
		Refuse(w, http.StatusForbidden, CodeActingForAnother, "%v", err)
		return
	} else if err != nil {
		Refuse(w, http.StatusBadRequest, CodeInvalidBody, "%v", err)
		return
	}
	var made []store.Decision
	err = h.store.Update(r.Context(), func(tx *store.Tx) error {
		made, err = route.Place(r.Context(), tx, key, p)
		return err
	})
	var undeclared *route.NotDeclaredError
	var chapter *route.ChapterError
	var pushOnly *route.PushOnlyError
	switch {
	case errors.Is(err, store.ErrNoItem):
		Refuse(w, http.StatusNotFound, CodeItemNotFound, "no item %s", key)
		return
	case errors.As(err, &undeclared):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationNotDeclared, "%v, so nothing was placed", undeclared)
		return
	case errors.As(err, &pushOnly):
		Refuse(w, http.StatusUnprocessableEntity, CodeDestinationPushOnly, "%v, and nothing was placed", pushOnly)
		return
	case errors.As(err, &chapter):
		Refuse(w, http.StatusUnprocessableEntity, CodeChapterOfBook, "%v", chapter)
		return
	case err != nil:
		h.log.ErrorContext(r.Context(), "place an item", "error", err, "item", key)
		Refuse(w, http.StatusInternalServerError, CodeInternal, "the server could not place the item")
		return
	}
	answer := PlacementAnswer{SchemaVersion: 1, Key: key, Decisions: make([]DecisionView, 0, len(made))}
	for _, d := range made {
		answer.Decisions = append(answer.Decisions, decisionView(d))
	}
	h.log.Info("a person decided an item", "item", key, "verdict", made[0].Verdict, "destination", p.Destination,
		"decided_by", made[0].DecidedBy, "client", p.ClientID, "cause", made[0].Cause)
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
