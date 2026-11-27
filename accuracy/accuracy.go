// Package accuracy measures how often the quality gate's and the side gate's
// answers agree with a person's placement. The label is a person's placement
// of an item that the gates sent to review first, as store.ReviewPlacement
// reads it from the item's decisions. A placement that no review came before
// labels nothing. Each done answer of the quality gate on a labeled item is
// one comparison, so an item that two prompt versions answered counts once
// under each.
//
// An arm is the gate's prompt asked of another model, kept at its own stage.
// Each arm is reported by its model, beside the gate's answers to the same
// prompt on the same items, so two models compare over one set of labels.
//
// A comparison reads the model's answer as it gave it, before any floor
// turns a drop or a guess into review, so the counts by confidence band show
// where a floor would sit. The quality gate's pass agrees with a placement on
// a side, and its drop with a person's drop. The side gate's guess agrees
// where it names the person's side, and a person's drop gives it nothing to
// compare. An unsure answer neither agrees nor disagrees.
//
// The note band reads every note's draft by its confidence. A draft is kept
// where a person's placement took it, and rewritten where a person's note took
// its place. It is applied where it reached the floor and the read gate made
// it the read's note with no person, waiting where the item waits in review as
// note-unclear, and other where the item left a read by a model. Kept over kept
// and rewritten counts only what a person decided, so it shows where the note
// floor would sit.
package accuracy

import (
	"cmp"
	"encoding/json"
	"log/slog"
	"math"
	"net/http"
	"slices"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// Report is what GET /api/v1/gates/agreement answers:
// schemas/gate-agreement.schema.json.
type Report struct {
	SchemaVersion int    `json:"schema_version"`
	Labels        Labels `json:"labels"`
	// Unanswered counts the labeled items that hold no done answer of the
	// quality gate, which a quality replay with placed_by person answers.
	Unanswered int  `json:"unanswered"`
	Quality    Gate `json:"quality"`
	Side       Gate `json:"side"`
	// Arms are each arm's answers by model, the first model first.
	Arms []Arm `json:"arms"`
	// Note is how each note's draft fared, overall and by its confidence.
	Note NoteDrafts `json:"note"`
}

// NoteDrafts is how the note stage's drafts fared against the floor at or
// above which a draft becomes the read's note.
type NoteDrafts struct {
	Floor        float64    `json:"floor"`
	Overall      NoteCounts `json:"overall"`
	ByConfidence []NoteBand `json:"by_confidence"`
}

// NoteCounts is how a set of drafts fared, one draft to an item.
type NoteCounts struct {
	Drafts    int `json:"drafts"`
	Kept      int `json:"kept"`
	Applied   int `json:"applied"`
	Rewritten int `json:"rewritten"`
	Waiting   int `json:"waiting"`
	Other     int `json:"other"`
	// KeptShare is Kept over Kept and Rewritten, and nil where both are 0.
	KeptShare *float64 `json:"kept_share"`
}

// NoteBand is the drafts whose confidence lies from From up to To, To itself
// only in the top band.
type NoteBand struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
	NoteCounts
}

// How one draft fared.
const (
	NoteKept      = "kept"
	NoteApplied   = "applied"
	NoteRewritten = "rewritten"
	NoteWaiting   = "waiting"
	NoteOther     = "other"
)

// Arm is one model's answers to the gate's prompt, beside the gate's own.
type Arm struct {
	Model string `json:"model"`
	// Items counts the labeled items that the arm answered.
	Items int `json:"items"`
	// Arm is the arm's answers, and Gate the gate's answers to the same
	// prompt versions on the same items.
	Arm  Answers `json:"arm"`
	Gate Answers `json:"gate"`
}

// Answers is one set of answers measured as each gate reads them.
type Answers struct {
	Quality Gate `json:"quality"`
	Side    Gate `json:"side"`
}

// Labels counts the labeled items, by where the person placed each.
type Labels struct {
	Items    int `json:"items"`
	Personal int `json:"personal"`
	Fleet    int `json:"fleet"`
	Drop     int `json:"drop"`
}

// Gate is one gate's agreement overall and broken down.
type Gate struct {
	// Floor is the confidence at or above which the gate acts on the answer
	// alone: the quality gate's drop, or the side gate's guess.
	Floor           float64           `json:"floor"`
	Overall         Counts            `json:"overall"`
	ByConfidence    []Band            `json:"by_confidence"`
	ByConnector     []ByConnector     `json:"by_connector"`
	ByModel         []ByModel         `json:"by_model"`
	ByPromptVersion []ByPromptVersion `json:"by_prompt_version"`
}

// Counts is how a set of comparisons came out.
type Counts struct {
	Comparisons int `json:"comparisons"`
	Agreed      int `json:"agreed"`
	Disagreed   int `json:"disagreed"`
	Unsure      int `json:"unsure"`
	// Agreement is Agreed over Agreed and Disagreed, and nil where both are 0.
	Agreement *float64 `json:"agreement"`
	// Answers counts each pair of the model's answer and the person's
	// placement.
	Answers []Pair `json:"answers"`
}

// Pair is how many comparisons paired one answer with one placement.
type Pair struct {
	Answer string `json:"answer"`
	Label  string `json:"label"`
	Count  int    `json:"count"`
}

// Band is the comparisons whose answer's confidence lies from From up to To,
// To itself only in the top band.
type Band struct {
	From float64 `json:"from"`
	To   float64 `json:"to"`
	Counts
}

// ByConnector is the comparisons on the items that one connector sighted.
type ByConnector struct {
	Connector string `json:"connector"`
	Counts
}

// ByModel is the comparisons on one model's answers.
type ByModel struct {
	Model string `json:"model"`
	Counts
}

// ByPromptVersion is the comparisons on one prompt version's answers.
type ByPromptVersion struct {
	PromptVersion string `json:"prompt_version"`
	Counts
}

// bands is how many bands of equal width cut the confidence from 0 to 1.
const bands = 10

// Outcomes of one comparison.
const (
	Agreed    = "agreed"
	Disagreed = "disagreed"
	Unsure    = "unsure"
)

// comparison is one answer set beside one placement.
type comparison struct {
	answer, label, outcome string
	confidence             float64
	connectors             []string
	model, promptVersion   string
}

// SchemaVersion is the version of schemas/gate-agreement.schema.json that a
// report holds to.
const SchemaVersion = 3

// Measure is the report over records, of which it counts each labeled item,
// and over drafted, each item holding a note's draft, under noteFloor.
func Measure(records, drafted []store.ItemRecords, noteFloor float64) Report {
	report := Report{SchemaVersion: SchemaVersion, Arms: []Arm{}, Note: MeasureNotes(drafted, noteFloor)}
	var quality, side []comparison
	type armComparisons struct {
		items                                map[string]bool
		quality, side, gateQuality, gateSide []comparison
	}
	arms := map[string]*armComparisons{}
	for _, r := range records {
		placement, ok := r.ReviewPlacement()
		if !ok {
			continue
		}
		label := placement.Verdict
		report.Labels.Items++
		switch label {
		case connectors.SidePersonal:
			report.Labels.Personal++
		case connectors.SideFleet:
			report.Labels.Fleet++
		case route.VerdictDrop:
			report.Labels.Drop++
		}
		var names []string
		for _, s := range r.Sightings {
			names = append(names, s.Connector)
		}
		gateAnswers := map[string][2][]comparison{}
		answered := false
		for _, o := range AnswersAt(r.Outputs, store.StageQuality) {
			q, s := compare(o, label, names)
			answered = true
			quality, side = append(quality, q), append(side, s...)
			gateAnswers[o.ProducerVersion] = [2][]comparison{{q}, s}
		}
		if !answered {
			report.Unanswered++
		}
		for _, o := range AnswersAt(r.Outputs, store.StageQualityArm) {
			theGate, ok := gateAnswers[o.ProducerVersion]
			if !ok {
				continue
			}
			model := modelOf(o)
			arm := arms[model]
			if arm == nil {
				arm = &armComparisons{items: map[string]bool{}}
				arms[model] = arm
			}
			q, s := compare(o, label, names)
			arm.items[r.Item.Key] = true
			arm.quality, arm.side = append(arm.quality, q), append(arm.side, s...)
			arm.gateQuality, arm.gateSide = append(arm.gateQuality, theGate[0]...), append(arm.gateSide, theGate[1]...)
		}
	}
	report.Quality = gate(qualitygate.DropFloor, quality)
	report.Side = gate(route.GuessFloor, side)
	for _, model := range sortedKeys(arms) {
		a := arms[model]
		report.Arms = append(report.Arms, Arm{
			Model: model, Items: len(a.items),
			Arm:  Answers{Quality: gate(qualitygate.DropFloor, a.quality), Side: gate(route.GuessFloor, a.side)},
			Gate: Answers{Quality: gate(qualitygate.DropFloor, a.gateQuality), Side: gate(route.GuessFloor, a.gateSide)},
		})
	}
	return report
}

// MeasureNotes is how the newest done draft of each of records fared, under
// floor.
func MeasureNotes(records []store.ItemRecords, floor float64) NoteDrafts {
	n := NoteDrafts{Floor: floor, ByConfidence: []NoteBand{}}
	byBand := map[int][]string{}
	var all []string
	for _, r := range records {
		newest := store.NewestDone(r.Outputs, store.StageNote)
		if newest == nil {
			continue
		}
		d, err := route.DraftOf(*newest)
		if err != nil {
			continue
		}
		outcome := NoteOutcome(r)
		all = append(all, outcome)
		band := bandOf(d.Confidence)
		byBand[band] = append(byBand[band], outcome)
	}
	n.Overall = countNotes(all)
	for _, band := range sortedKeys(byBand) {
		n.ByConfidence = append(n.ByConfidence, NoteBand{From: float64(band) / bands, To: float64(band+1) / bands, NoteCounts: countNotes(byBand[band])})
	}
	return n
}

// NoteOutcome is how the item's draft fared, by its newest decisions.
func NoteOutcome(r store.ItemRecords) string {
	read, ok := r.NewestDecision(store.GateRead)
	if _, onASide := r.Side(); !ok || !onASide {
		return NoteOther
	}
	switch {
	case store.NoteDigest(read.Note) != nil:
		return NoteRewritten
	case read.Verdict == connectors.ReadModel && read.Cause == route.CauseNoteDraft && read.ByPerson():
		return NoteKept
	case read.Verdict == connectors.ReadModel && read.Cause == route.CauseNoteDraft:
		return NoteApplied
	case read.Verdict == route.VerdictReview && read.Cause == route.CauseNoteUnclear:
		return NoteWaiting
	}
	return NoteOther
}

func countNotes(outcomes []string) NoteCounts {
	c := NoteCounts{Drafts: len(outcomes)}
	for _, o := range outcomes {
		switch o {
		case NoteKept:
			c.Kept++
		case NoteApplied:
			c.Applied++
		case NoteRewritten:
			c.Rewritten++
		case NoteWaiting:
			c.Waiting++
		default:
			c.Other++
		}
	}
	if decided := c.Kept + c.Rewritten; decided > 0 {
		share := float64(c.Kept) / float64(decided)
		c.KeptShare = &share
	}
	return c
}

// bandOf is the band of bands that confidence lies in. The epsilon keeps a
// confidence such as 0.7 in its own band.
func bandOf(confidence float64) int {
	return min(max(int(math.Floor(confidence*bands+1e-9)), 0), bands-1)
}

// AnswersAt is each done answer of the gate's prompt among outputs at stage,
// the quality gate's own or an arm's, that decodes.
func AnswersAt(outputs []store.Output, stage string) []store.Output {
	var out []store.Output
	for _, o := range outputs {
		if o.Stage != stage || o.Producer != qualitygate.Producer || o.State != store.OutputDone {
			continue
		}
		if _, err := answerOf(o); err == nil {
			out = append(out, o)
		}
	}
	return out
}

func answerOf(o store.Output) (qualitygate.Answer, error) {
	var a qualitygate.Answer
	err := json.Unmarshal(o.Answer, &a)
	return a, err
}

// compare sets o's verdict beside label, and its side guess where the label
// is a side.
func compare(o store.Output, label string, connectorNames []string) (comparison, []comparison) {
	a, _ := answerOf(o)
	base := comparison{label: label, connectors: connectorNames, model: modelOf(o), promptVersion: o.ProducerVersion}
	verdict, side := Outcomes(a, label)
	q := base
	q.answer, q.confidence, q.outcome = a.Verdict, a.VerdictConfidence, verdict
	if side == "" {
		return q, nil
	}
	s := base
	s.answer, s.confidence, s.outcome = a.Side, a.SideConfidence, side
	return q, []comparison{s}
}

// Outcomes compares the quality gate's answer a with a person's label: its
// verdict always, and its side guess only where the label is a side. side is
// "" where it compares nothing.
func Outcomes(a qualitygate.Answer, label string) (verdict, side string) {
	verdict = qualityOutcome(a.Verdict, label)
	if label == connectors.SidePersonal || label == connectors.SideFleet {
		side = sideOutcome(a.Side, label)
	}
	return verdict, side
}

// qualityOutcome compares the quality gate's verdict with a person's
// placement: a pass agrees with a side, and a drop with a drop.
func qualityOutcome(verdict, label string) string {
	onASide := label == connectors.SidePersonal || label == connectors.SideFleet
	switch {
	case verdict == qualitygate.AnswerPass && onASide, verdict == qualitygate.AnswerDrop && label == route.VerdictDrop:
		return Agreed
	case verdict == qualitygate.AnswerPass, verdict == qualitygate.AnswerDrop:
		return Disagreed
	}
	return Unsure
}

// sideOutcome compares the side guess with the person's side.
func sideOutcome(guess, label string) string {
	switch guess {
	case label:
		return Agreed
	case connectors.SidePersonal, connectors.SideFleet:
		return Disagreed
	}
	return Unsure
}

// modelOf is the model that answered o, else the one it asked for.
func modelOf(o store.Output) string {
	switch {
	case o.AnsweredModel != nil:
		return *o.AnsweredModel
	case o.Model != nil:
		return *o.Model
	}
	return "unknown"
}

// gate counts comparisons overall and by band, connector, model and prompt
// version, each breakdown in ascending order.
func gate(floor float64, comparisons []comparison) Gate {
	g := Gate{
		Floor: floor, Overall: count(comparisons), ByConfidence: []Band{}, ByConnector: []ByConnector{}, ByModel: []ByModel{},
		ByPromptVersion: []ByPromptVersion{},
	}
	byBand := map[int][]comparison{}
	byConnector, byModel, byVersion := map[string][]comparison{}, map[string][]comparison{}, map[string][]comparison{}
	for _, c := range comparisons {
		band := bandOf(c.confidence)
		byBand[band] = append(byBand[band], c)
		for _, name := range c.connectors {
			byConnector[name] = append(byConnector[name], c)
		}
		byModel[c.model] = append(byModel[c.model], c)
		byVersion[c.promptVersion] = append(byVersion[c.promptVersion], c)
	}
	for _, band := range sortedKeys(byBand) {
		from, to := float64(band)/bands, float64(band+1)/bands
		g.ByConfidence = append(g.ByConfidence, Band{From: from, To: to, Counts: count(byBand[band])})
	}
	for _, name := range sortedKeys(byConnector) {
		g.ByConnector = append(g.ByConnector, ByConnector{Connector: name, Counts: count(byConnector[name])})
	}
	for _, name := range sortedKeys(byModel) {
		g.ByModel = append(g.ByModel, ByModel{Model: name, Counts: count(byModel[name])})
	}
	for _, version := range sortedKeys(byVersion) {
		g.ByPromptVersion = append(g.ByPromptVersion, ByPromptVersion{PromptVersion: version, Counts: count(byVersion[version])})
	}
	return g
}

// count is how comparisons came out.
func count(comparisons []comparison) Counts {
	c := Counts{Comparisons: len(comparisons), Answers: []Pair{}}
	pairs := map[[2]string]int{}
	for _, one := range comparisons {
		switch one.outcome {
		case Agreed:
			c.Agreed++
		case Disagreed:
			c.Disagreed++
		default:
			c.Unsure++
		}
		pairs[[2]string{one.answer, one.label}]++
	}
	if decided := c.Agreed + c.Disagreed; decided > 0 {
		agreement := float64(c.Agreed) / float64(decided)
		c.Agreement = &agreement
	}
	for pair, n := range pairs {
		c.Answers = append(c.Answers, Pair{Answer: pair[0], Label: pair[1], Count: n})
	}
	slices.SortFunc(c.Answers, func(a, b Pair) int { return cmp.Or(cmp.Compare(a.Answer, b.Answer), cmp.Compare(a.Label, b.Label)) })
	return c
}

func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Routes is the route that reads the gates' agreement with every label, and
// how each note's draft fared under noteFloor.
func Routes(st *store.Store, noteFloor float64, log *slog.Logger) server.Routes {
	if log == nil {
		log = slog.Default()
	}
	return func(r *server.Router) {
		r.Handle(server.RouteGateAgreement, func(w http.ResponseWriter, req *http.Request) {
			records, err := st.ReviewPlacedItems(req.Context())
			if err != nil {
				log.ErrorContext(req.Context(), "read the items a person placed from review", "error", err)
				server.Refuse(w, http.StatusInternalServerError, server.CodeInternal, "the server could not read the items a person placed from review")
				return
			}
			drafted, err := st.DraftedItems(req.Context())
			if err != nil {
				log.ErrorContext(req.Context(), "read the items holding a note's draft", "error", err)
				server.Refuse(w, http.StatusInternalServerError, server.CodeInternal, "the server could not read the items holding a note's draft")
				return
			}
			server.JSON(w, http.StatusOK, Measure(records, drafted, noteFloor))
		})
	}
}
