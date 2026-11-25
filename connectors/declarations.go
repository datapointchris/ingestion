// Package connectors runs each source's connector by its mode: poll, push,
// folder or drop. A person keeps each connector's declaration in git beside
// the routing rules and the destinations, and applies the set to the service
// whole, as kubectl apply sends a manifest. The service keeps the set with its commit
// and reconciles each connector's cursor toward it. Its scheduler owns when
// each connector is next due.
package connectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
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
	// Format names the drop handler that splits a drop connector's file into
	// items, as RegisterDropHandler registered it. Left out, a dropped file
	// lands as one item.
	Format string `json:"format,omitempty" yaml:"format"`
}

// Scheduled says whether c runs on its schedule, as a poll or a folder does.
// A drop or a push runs as its items arrive, and is due its due_interval
// after its last delivery.
func (c Connector) Scheduled() bool { return c.Mode == ModePoll || c.Mode == ModeFolder }

// RoutingRule names one destination for the items that match it.
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
	// Deliverer names the deliverer that writes each item placed here, as
	// deliver.Register registered it. Left out, nothing is delivered, as for
	// a destination whose reader pulls its items itself.
	Deliverer string `json:"deliverer,omitempty" yaml:"deliverer"`
	// DefaultForSide makes this the destination of an item placed on its side
	// with no destination named, as the quality gate's guess places one.
	DefaultForSide bool `json:"default_for_side,omitempty" yaml:"default_for_side"`
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

// Destination is the destination named name in s, and false where s holds
// none.
func (s Set) Destination(name string) (Destination, bool) {
	for _, d := range s.Destinations {
		if d.Name == name {
			return d, true
		}
	}
	return Destination{}, false
}

// SideDefault is the destination that takes side's items placed with no
// destination named, and false where none is. Validate refuses two on a side.
func (s Set) SideDefault(side string) (Destination, bool) {
	for _, d := range s.Destinations {
		if d.DefaultForSide && d.Side == side {
			return d, true
		}
	}
	return Destination{}, false
}

// Tagged is the destination that tag places an item on, and false where no
// destination names it. Validate refuses a tag on two destinations.
func (s Set) Tagged(tag string) (Destination, bool) {
	for _, d := range s.Destinations {
		if slices.Contains(d.Tags, tag) {
			return d, true
		}
	}
	return Destination{}, false
}

// AppliedSource is what reads the set in force: a write, as a gate sees it, or
// the store, as a read of an item sees it.
type AppliedSource interface {
	AppliedDeclarations(ctx context.Context) (store.Declarations, bool, error)
}

// AppliedSet reads the set in force from src, and an empty set where none has
// been applied.
func AppliedSet(ctx context.Context, src AppliedSource) (Set, error) {
	applied, ok, err := src.AppliedDeclarations(ctx)
	if err != nil {
		return Set{}, err
	}
	if !ok {
		return Set{Connectors: []Connector{}, RoutingRules: []RoutingRule{}, Destinations: []Destination{}}, nil
	}
	return Decode(applied)
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

// ErrNoDeclarations is a directory holding none of a set's files, which
// Read refuses rather than reading as a set that declares nothing.
var ErrNoDeclarations = errors.New("the directory holds no connectors/ directory, " + RoutingRulesFile + " or " + DestinationsFile)

// Read reads the set below dir: each connectors/*.yml, routing-rules.yml and
// destinations.yml, where they exist. A directory holding none of the three
// is ErrNoDeclarations, so a set that retires every connector is written on
// purpose, as an empty connectors/ directory. A file that does not parse, or
// that names a field its schema does not, is an InvalidError, and so is any
// declaration that Validate refuses. roots names the [serve.homes] roots a
// folder may read, and nil skips that check.
func Read(dir string, roots []string) (Set, error) {
	if info, err := os.Stat(dir); err != nil {
		return Set{}, err
	} else if !info.IsDir() {
		return Set{}, fmt.Errorf("%s is not a directory", dir)
	}
	found := false
	for _, name := range []string{ConnectorsDir, RoutingRulesFile, DestinationsFile} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			found = true
		}
	}
	if !found {
		return Set{}, fmt.Errorf("%s: %w", dir, ErrNoDeclarations)
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
// Each declaration is checked against its schema in schemas/, the one copy of
// the rules a single declaration holds to. Validate adds only what a schema
// cannot say: that no two declarations of a kind share a name, that a rule's
// connector and destination are declared, that a tag places an item on one
// destination, that one destination at most is a side's default, that a
// schedule parses, that a drop's format and a destination's deliverer are
// registered, and that a folder's root is one of roots. nil roots skips that
// check.
func Validate(s Set, roots []string) []Invalid {
	compiled, err := compiledSchemas()
	if err != nil {
		return []Invalid{{Declaration: "the declaration schemas", Problems: []string{err.Error()}}}
	}
	var out []Invalid
	add := func(declaration string, problems []string) {
		if len(problems) > 0 {
			out = append(out, Invalid{Declaration: declaration, Problems: problems})
		}
	}
	connectors := map[string]bool{}
	for _, c := range s.Connectors {
		problems := c.problems(compiled, roots)
		if connectors[c.Name] {
			problems = append(problems, "another connector has this name")
		}
		connectors[c.Name] = true
		add("connector "+c.Name, problems)
	}
	destinations := map[string]bool{}
	tagged := map[string]string{}
	sideDefaults := map[string]string{}
	for _, d := range s.Destinations {
		problems := schemaProblems(compiled.destination, d.normalized())
		if destinations[d.Name] {
			problems = append(problems, "another destination has this name")
		}
		destinations[d.Name] = true
		if _, ok := deliverers[d.Deliverer]; d.Deliverer != "" && !ok {
			problems = append(problems, fmt.Sprintf("deliverer %s names no deliverer this build registers", d.Deliverer))
		}
		if d.DefaultForSide && d.Side != "" {
			if other, taken := sideDefaults[d.Side]; taken {
				problems = append(problems, fmt.Sprintf("%s is already the default for side %s", other, d.Side))
			}
			sideDefaults[d.Side] = d.Name
		}
		for _, tag := range d.Tags {
			if other, taken := tagged[tag]; taken && other != d.Name {
				problems = append(problems, fmt.Sprintf("tag %s already places an item on %s", tag, other))
			}
			tagged[tag] = d.Name
		}
		add("destination "+d.Name, problems)
	}
	rules := map[string]bool{}
	for _, r := range s.RoutingRules {
		problems := schemaProblems(compiled.rule, r)
		if rules[r.Name] {
			problems = append(problems, "another routing rule has this name")
		}
		rules[r.Name] = true
		if r.Match.Connector != "" && !connectors[r.Match.Connector] {
			problems = append(problems, fmt.Sprintf("match names connector %s, which the set does not declare", r.Match.Connector))
		}
		if r.Destination != "" && !destinations[r.Destination] {
			problems = append(problems, fmt.Sprintf("destination %s is not one %s declares", r.Destination, DestinationsFile))
		}
		add("routing rule "+r.Name, problems)
	}
	return out
}

// problems is every reason c does not hold.
func (c Connector) problems(compiled declarationSchemas, roots []string) []string {
	var out []string
	for _, problem := range schemaProblems(compiled.connector, c.normalized()) {
		if field, ok := strings.CutSuffix(problem, ": false schema"); ok {
			problem = fmt.Sprintf("%s: a %s connector does not take it", field, c.Mode)
		}
		out = append(out, problem)
	}
	if c.Scheduled() && c.Schedule != "" {
		if _, err := parseSchedule(c.Schedule); err != nil {
			out = append(out, fmt.Sprintf("schedule %q is not a cron expression: %v", c.Schedule, err))
		}
	}
	if c.Format != "" {
		f, ok := dropFormats[c.Format]
		switch {
		case !ok:
			out = append(out, fmt.Sprintf("format %s names no drop handler this build registers", c.Format))
		case c.Zone != "" && c.Zone != f.zone:
			out = append(out, fmt.Sprintf("zone: format %s lands every item %s, not %s", c.Format, f.zone, c.Zone))
		}
	}
	if c.Mode == ModeFolder {
		if c.Root != "" && roots != nil && !slices.Contains(roots, c.Root) {
			out = append(out, fmt.Sprintf("root %s is not one this service's [serve.homes] names", c.Root))
		}
		if c.Path != "" && (!filepath.IsLocal(filepath.FromSlash(c.Path)) || path.Clean(c.Path) != c.Path) {
			out = append(out, fmt.Sprintf("path is a clean directory below the root, not %q", c.Path))
		}
	}
	return out
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
