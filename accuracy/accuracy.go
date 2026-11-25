// Package accuracy measures how often the quality gate's and the side gate's
// answers agree with a person's placement. A person's placement is the label:
// an item whose newest side decision a person made, as decided_by
// person:<login> records. Each done answer of the quality gate on such an item
// is one comparison, so an item that two prompt versions answered counts once
// under each.
//
// A comparison reads the model's answer as it gave it, before any floor
// turns a drop or a guess into review, so the counts by confidence band show
// where a floor would sit. The quality gate's pass agrees with a placement on
// a side, and its drop with a person's drop. The side gate's guess agrees
// where it names the person's side, and a person's drop gives it nothing to
// compare. An unsure answer neither agrees nor disagrees.
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
}

// Labels counts the items a person placed, by where the person placed each.
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
	agreed    = "agreed"
	disagreed = "disagreed"
	unsure    = "unsure"
)

// comparison is one answer set beside one placement.
type comparison struct {
	answer, label, outcome string
	confidence             float64
	connectors             []string
	model, promptVersion   string
}

// Measure is the report over records, each an item whose newest side
// decision a person made.
func Measure(records []store.ItemRecords) Report {
	report := Report{SchemaVersion: 1}
	var quality, side []comparison
	for _, r := range records {
		placement, ok := r.NewestDecision(store.GateSide)
		if !ok || !placement.ByPerson() {
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
		answered := false
		for _, o := range r.Outputs {
			if o.Stage != store.StageQuality || o.Producer != qualitygate.Producer || o.State != store.OutputDone {
				continue
			}
			var a qualitygate.Answer
			if json.Unmarshal(o.Answer, &a) != nil {
				continue
			}
			answered = true
			base := comparison{label: label, connectors: names, model: modelOf(o), promptVersion: o.ProducerVersion}
			q := base
			q.answer, q.confidence, q.outcome = a.Verdict, a.VerdictConfidence, qualityOutcome(a.Verdict, label)
			quality = append(quality, q)
			if label == connectors.SidePersonal || label == connectors.SideFleet {
				s := base
				s.answer, s.confidence, s.outcome = a.Side, a.SideConfidence, sideOutcome(a.Side, label)
				side = append(side, s)
			}
		}
		if !answered {
			report.Unanswered++
		}
	}
	report.Quality = gate(qualitygate.DropFloor, quality)
	report.Side = gate(route.GuessFloor, side)
	return report
}

// qualityOutcome compares the quality gate's verdict with a person's
// placement: a pass agrees with a side, and a drop with a drop.
func qualityOutcome(verdict, label string) string {
	onASide := label == connectors.SidePersonal || label == connectors.SideFleet
	switch {
	case verdict == qualitygate.AnswerPass && onASide, verdict == qualitygate.AnswerDrop && label == route.VerdictDrop:
		return agreed
	case verdict == qualitygate.AnswerPass, verdict == qualitygate.AnswerDrop:
		return disagreed
	}
	return unsure
}

// sideOutcome compares the side guess with the person's side.
func sideOutcome(guess, label string) string {
	switch guess {
	case label:
		return agreed
	case connectors.SidePersonal, connectors.SideFleet:
		return disagreed
	}
	return unsure
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
		// The epsilon keeps a confidence such as 0.7 in its own band.
		band := min(max(int(math.Floor(c.confidence*bands+1e-9)), 0), bands-1)
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
		case agreed:
			c.Agreed++
		case disagreed:
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

// Routes is the route that reads the gates' agreement with every person's
// placement.
func Routes(st *store.Store, log *slog.Logger) server.Routes {
	if log == nil {
		log = slog.Default()
	}
	return func(r *server.Router) {
		r.Handle(server.RouteGateAgreement, func(w http.ResponseWriter, req *http.Request) {
			records, err := st.PersonPlacedItems(req.Context())
			if err != nil {
				log.ErrorContext(req.Context(), "read the items a person placed", "error", err)
				server.Refuse(w, http.StatusInternalServerError, server.CodeInternal, "the server could not read the items a person placed")
				return
			}
			server.JSON(w, http.StatusOK, Measure(records))
		})
	}
}
