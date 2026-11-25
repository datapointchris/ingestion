// Package connectors runs each source's connector by its mode: poll, push,
// folder or drop. A person keeps each connector's declaration in git beside
// the routing rules and the destinations, and applies the set to the service
// whole, as kubectl apply sends a manifest. The service keeps the set with its commit
// and reconciles each connector's cursor toward it. Its scheduler owns when
// each connector is next due.
package connectors

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/datapointchris/ingestion/store"
)

// Modes, one per way a connector receives its source's items.
const (
	ModePoll   = "poll"
	ModePush   = "push"
	ModeFolder = "folder"
	ModeDrop   = "drop"
)

// Read rules, which choose how an item is read when no tag's rule does.
const (
	ReadAsIs  = "as-is"
	ReadModel = "model"
)

// Sides that a destination takes.
const (
	SidePersonal = "personal"
	SideFleet    = "fleet"
)

// The files of a declaration set, below the directory applied.
const (
	ConnectorsDir    = "connectors"
	RoutingRulesFile = "routing-rules.yml"
	DestinationsFile = "destinations.yml"
)

var (
	namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	tagPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]*$`)
)

// Connector is one connector's declaration, as connectors/<name>.yml holds
// it: schemas/connector.schema.json.
type Connector struct {
	Name string `json:"name" yaml:"name"`
	Mode string `json:"mode" yaml:"mode"`
	// Source names the external feed the connector reads, as the data
	// catalog names it.
	Source string `json:"source" yaml:"source"`
	// Contract names the contract the source's payloads hold to.
	Contract string `json:"contract,omitempty" yaml:"contract"`
	// Schedule is a poll's or a folder's cron expression.
	Schedule string `json:"schedule,omitempty" yaml:"schedule"`
	// DueInterval is how long after its last delivery a drop or a push is
	// due again, such as 30d.
	DueInterval string `json:"due_interval,omitempty" yaml:"due_interval"`
	// Freshness is how long a poll or a folder may go without a delivery
	// before it reads overdue.
	Freshness string     `json:"freshness,omitempty" yaml:"freshness"`
	Tags      []string   `json:"tags" yaml:"tags"`
	Zone      store.Zone `json:"zone" yaml:"zone"`
	ReadRule  string     `json:"read_rule,omitempty" yaml:"read_rule"`
	// Root names the [serve.homes] root a folder connector reads, and Path
	// the directory below it.
	Root string `json:"root,omitempty" yaml:"root"`
	Path string `json:"path,omitempty" yaml:"path"`
}

// RoutingRule places each item its match holds for on a destination, before
// any model reads it. A decision it makes is by rule:<name>.
type RoutingRule struct {
	Name        string `json:"name" yaml:"name"`
	Match       Match  `json:"match" yaml:"match"`
	Destination string `json:"destination" yaml:"destination"`
}

// Match is what a routing rule holds for. Each condition it names must hold.
type Match struct {
	// Connector holds for an item that connector sighted.
	Connector string `json:"connector,omitempty" yaml:"connector"`
	// Tag holds for an item a sighting tagged with it.
	Tag string `json:"tag,omitempty" yaml:"tag"`
	// Field names a field of the source's payload, such as a Reddit save's
	// subreddit, and Values the values it holds for.
	Field  string   `json:"field,omitempty" yaml:"field"`
	Values []string `json:"values,omitempty" yaml:"values"`
}

// Destination names a place that delivery writes items to, the side it
// takes, and the tags that place an item there.
type Destination struct {
	Name string `json:"name" yaml:"name"`
	// Side is personal or fleet, and empty for a destination on neither,
	// such as one that drops what it takes.
	Side string   `json:"side,omitempty" yaml:"side"`
	Tags []string `json:"tags" yaml:"tags"`
}

// Set is every declaration applied together.
type Set struct {
	Connectors   []Connector   `json:"connectors"`
	RoutingRules []RoutingRule `json:"routing_rules"`
	Destinations []Destination `json:"destinations"`
}

// Connector is the connector named name in s, and false where s holds none.
func (s Set) Connector(name string) (Connector, bool) {
	for _, c := range s.Connectors {
		if c.Name == name {
			return c, true
		}
	}
	return Connector{}, false
}

// Invalid is one declaration that does not hold, and each reason it does not.
type Invalid struct {
	// Declaration names it, such as connector reddit-exports, or the file
	// that did not parse.
	Declaration string   `json:"declaration"`
	Problems    []string `json:"problems"`
}

// ErrInvalid is a set holding an invalid declaration, which applies none.
var ErrInvalid = errors.New("the set holds invalid declarations, so none of it is applied")

// InvalidError is the set's every invalid declaration.
type InvalidError struct{ Invalid []Invalid }

func (e *InvalidError) Error() string {
	var b strings.Builder
	b.WriteString(ErrInvalid.Error())
	for _, inv := range e.Invalid {
		b.WriteString("\n  " + inv.Declaration)
		for _, p := range inv.Problems {
			b.WriteString("\n    " + p)
		}
	}
	return b.String()
}

func (e *InvalidError) Unwrap() error { return ErrInvalid }

// Read reads the set below dir: each connectors/*.yml, routing-rules.yml and
// destinations.yml, where they exist. A file that does not parse, or that
// names a field its schema does not, is an InvalidError, and so is any
// declaration that Validate refuses. roots names the [serve.homes] roots a
// folder may read, and nil skips that check.
func Read(dir string, roots []string) (Set, error) {
	if info, err := os.Stat(dir); err != nil {
		return Set{}, err
	} else if !info.IsDir() {
		return Set{}, fmt.Errorf("%s is not a directory", dir)
	}
	set := Set{Connectors: []Connector{}, RoutingRules: []RoutingRule{}, Destinations: []Destination{}}
	var invalid []Invalid
	files, err := filepath.Glob(filepath.Join(dir, ConnectorsDir, "*.y*ml"))
	if err != nil {
		return set, err
	}
	sort.Strings(files)
	for _, file := range files {
		name := path.Join(ConnectorsDir, filepath.Base(file))
		var c Connector
		if err := decodeFile(file, &c); err != nil {
			invalid = append(invalid, Invalid{Declaration: name, Problems: []string{err.Error()}})
			continue
		}
		if stem := strings.TrimSuffix(strings.TrimSuffix(filepath.Base(file), ".yml"), ".yaml"); c.Name != stem {
			invalid = append(invalid, Invalid{Declaration: name, Problems: []string{
				fmt.Sprintf("declares connector %q, and a connector's file is named for it, as %s.yml", c.Name, c.Name),
			}})
			continue
		}
		set.Connectors = append(set.Connectors, c)
	}
	var rules struct {
		Rules []RoutingRule `yaml:"rules"`
	}
	if err := decodeFile(filepath.Join(dir, RoutingRulesFile), &rules); err != nil && !errors.Is(err, fs.ErrNotExist) {
		invalid = append(invalid, Invalid{Declaration: RoutingRulesFile, Problems: []string{err.Error()}})
	} else if rules.Rules != nil {
		set.RoutingRules = rules.Rules
	}
	var destinations struct {
		Destinations []Destination `yaml:"destinations"`
	}
	if err := decodeFile(filepath.Join(dir, DestinationsFile), &destinations); err != nil && !errors.Is(err, fs.ErrNotExist) {
		invalid = append(invalid, Invalid{Declaration: DestinationsFile, Problems: []string{err.Error()}})
	} else if destinations.Destinations != nil {
		set.Destinations = destinations.Destinations
	}
	invalid = append(invalid, Validate(set, roots)...)
	if len(invalid) > 0 {
		return set, &InvalidError{Invalid: invalid}
	}
	return set, nil
}

// decodeFile reads the YAML file at file into into, refusing a field into
// does not declare.
func decodeFile(file string, into any) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(into); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// Validate names each declaration in s that does not hold, with every reason.
// It checks each connector's mode, zone and timing, each rule's match and
// destination, and that no two declarations of a kind share a name. roots
// names the [serve.homes] roots a folder may read, and nil skips that check.
func Validate(s Set, roots []string) []Invalid {
	var out []Invalid
	add := func(declaration string, problems []string) {
		if len(problems) > 0 {
			out = append(out, Invalid{Declaration: declaration, Problems: problems})
		}
	}
	connectors := map[string]bool{}
	for _, c := range s.Connectors {
		problems := c.problems(roots)
		if connectors[c.Name] {
			problems = append(problems, "another connector has this name")
		}
		connectors[c.Name] = true
		add("connector "+c.Name, problems)
	}
	destinations := map[string]bool{}
	tagged := map[string]string{}
	for _, d := range s.Destinations {
		var problems []string
		if !namePattern.MatchString(d.Name) {
			problems = append(problems, fmt.Sprintf("a destination is named in lowercase letters, digits and hyphens, not %q", d.Name))
		}
		if destinations[d.Name] {
			problems = append(problems, "another destination has this name")
		}
		destinations[d.Name] = true
		if d.Side != "" && d.Side != SidePersonal && d.Side != SideFleet {
			problems = append(problems, fmt.Sprintf("side is personal or fleet, or left out for neither, not %q", d.Side))
		}
		for _, tag := range d.Tags {
			problems = append(problems, tagProblems(tag)...)
			if other, taken := tagged[tag]; taken && other != d.Name {
				problems = append(problems, fmt.Sprintf("tag %s already places an item on %s", tag, other))
			}
			tagged[tag] = d.Name
		}
		add("destination "+d.Name, problems)
	}
	rules := map[string]bool{}
	for _, r := range s.RoutingRules {
		var problems []string
		if !namePattern.MatchString(r.Name) {
			problems = append(problems, fmt.Sprintf("a routing rule is named in lowercase letters, digits and hyphens, not %q", r.Name))
		}
		if rules[r.Name] {
			problems = append(problems, "another routing rule has this name")
		}
		rules[r.Name] = true
		m := r.Match
		if m.Connector == "" && m.Tag == "" && m.Field == "" {
			problems = append(problems, "match names a connector, a tag or a field, or it would hold for every item")
		}
		if m.Connector != "" && !connectors[m.Connector] {
			problems = append(problems, fmt.Sprintf("match names connector %s, which the set does not declare", m.Connector))
		}
		if m.Tag != "" {
			problems = append(problems, tagProblems(m.Tag)...)
		}
		if (m.Field == "") != (len(m.Values) == 0) {
			problems = append(problems, "match names a field and the values it holds for, or neither")
		}
		if slices.Contains(m.Values, "") {
			problems = append(problems, "match's values are not empty")
		}
		switch {
		case r.Destination == "":
			problems = append(problems, "a routing rule names the destination it places an item on")
		case !destinations[r.Destination]:
			problems = append(problems, fmt.Sprintf("destination %s is not one destinations.yml declares", r.Destination))
		}
		add("routing rule "+r.Name, problems)
	}
	return out
}

// problems is every reason c does not hold.
func (c Connector) problems(roots []string) []string {
	var out []string
	if !namePattern.MatchString(c.Name) {
		out = append(out, fmt.Sprintf("a connector is named in lowercase letters, digits and hyphens, not %q", c.Name))
	}
	if !namePattern.MatchString(c.Source) {
		out = append(out, fmt.Sprintf("source names the catalog's external feed in lowercase letters, digits and hyphens, not %q", c.Source))
	}
	if c.Zone != store.Outside && c.Zone != store.Owner {
		out = append(out, fmt.Sprintf("zone is outside or owner, not %q", c.Zone))
	}
	for _, tag := range c.Tags {
		out = append(out, tagProblems(tag)...)
	}
	if c.ReadRule != "" && c.ReadRule != ReadAsIs && c.ReadRule != ReadModel {
		out = append(out, fmt.Sprintf("read_rule is as-is or model, or left out for the side's default, not %q", c.ReadRule))
	}
	scheduled := c.Mode == ModePoll || c.Mode == ModeFolder
	switch c.Mode {
	case ModePoll, ModeFolder, ModePush, ModeDrop:
	default:
		return append(out, fmt.Sprintf("mode is poll, push, folder or drop, not %q", c.Mode))
	}
	switch {
	case scheduled && c.Schedule == "":
		out = append(out, fmt.Sprintf("a %s connector names the schedule it runs on, as a cron expression", c.Mode))
	case scheduled:
		if _, err := parseSchedule(c.Schedule); err != nil {
			out = append(out, fmt.Sprintf("schedule %q is not a cron expression: %v", c.Schedule, err))
		}
	case c.Schedule != "":
		out = append(out, fmt.Sprintf("a %s connector runs when its items arrive, so it names a due_interval rather than a schedule", c.Mode))
	}
	switch {
	case !scheduled && c.DueInterval == "":
		out = append(out, fmt.Sprintf("a %s connector names its due_interval, how long after its last delivery it is due, such as 30d", c.Mode))
	case !scheduled:
		if _, err := ParseInterval(c.DueInterval); err != nil {
			out = append(out, "due_interval: "+err.Error())
		}
	case c.DueInterval != "":
		out = append(out, fmt.Sprintf("a %s connector is due on its schedule, so it names no due_interval", c.Mode))
	}
	if c.Freshness != "" {
		if !scheduled {
			out = append(out, fmt.Sprintf("a %s connector is overdue once its due_interval passes, so it names no freshness", c.Mode))
		} else if _, err := ParseInterval(c.Freshness); err != nil {
			out = append(out, "freshness: "+err.Error())
		}
	}
	if c.Mode != ModeFolder {
		if c.Root != "" || c.Path != "" {
			out = append(out, fmt.Sprintf("a %s connector reads no folder, so it names no root or path", c.Mode))
		}
		return out
	}
	switch {
	case c.Root == "":
		out = append(out, "a folder connector names the [serve.homes] root it reads")
	case roots != nil && !slices.Contains(roots, c.Root):
		out = append(out, fmt.Sprintf("root %s is not one this service's [serve.homes] names", c.Root))
	}
	if c.Path != "" && (!filepath.IsLocal(filepath.FromSlash(c.Path)) || path.Clean(c.Path) != c.Path) {
		out = append(out, fmt.Sprintf("path is a clean directory below the root, not %q", c.Path))
	}
	return out
}

func tagProblems(tag string) []string {
	if tagPattern.MatchString(tag) {
		return nil
	}
	return []string{fmt.Sprintf("a tag is lowercase letters, digits, dots, colons, underscores and hyphens, not %q", tag)}
}

// Decode reads the set back from an applied declarations row.
func Decode(d store.Declarations) (Set, error) {
	set := Set{Connectors: []Connector{}, RoutingRules: []RoutingRule{}, Destinations: []Destination{}}
	for _, raw := range d.Connectors {
		var c Connector
		if err := json.Unmarshal(raw, &c); err != nil {
			return set, fmt.Errorf("read applied connector: %w", err)
		}
		set.Connectors = append(set.Connectors, c)
	}
	for _, raw := range d.RoutingRules {
		var r RoutingRule
		if err := json.Unmarshal(raw, &r); err != nil {
			return set, fmt.Errorf("read applied routing rule: %w", err)
		}
		set.RoutingRules = append(set.RoutingRules, r)
	}
	for _, raw := range d.Destinations {
		var dst Destination
		if err := json.Unmarshal(raw, &dst); err != nil {
			return set, fmt.Errorf("read applied destination: %w", err)
		}
		set.Destinations = append(set.Destinations, dst)
	}
	return set, nil
}

// encode is s as JSON for the declarations row, one value per declaration.
func (s Set) encode() (connectors, rules, destinations []json.RawMessage, err error) {
	each := func(n int, at func(int) any) ([]json.RawMessage, error) {
		out := make([]json.RawMessage, 0, n)
		for i := range n {
			raw, err := json.Marshal(at(i))
			if err != nil {
				return nil, err
			}
			out = append(out, raw)
		}
		return out, nil
	}
	if connectors, err = each(len(s.Connectors), func(i int) any { return s.Connectors[i].normalized() }); err != nil {
		return
	}
	if rules, err = each(len(s.RoutingRules), func(i int) any { return s.RoutingRules[i] }); err != nil {
		return
	}
	destinations, err = each(len(s.Destinations), func(i int) any { return s.Destinations[i].normalized() })
	return
}

func (c Connector) normalized() Connector {
	if c.Tags == nil {
		c.Tags = []string{}
	}
	return c
}

func (d Destination) normalized() Destination {
	if d.Tags == nil {
		d.Tags = []string{}
	}
	return d
}
