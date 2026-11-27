package connectors

// Source is what a set checks of a source that this build carries. The
// sources package records each from the source's one registration, so what
// a set accepts and what a worker can run come from the same record.
type Source struct {
	Name string
	// Polls says that a worker of this build polls the source, so each of its
	// connectors is a poll.
	Polls bool
	// Deletes says that a worker of this build deletes from the source what
	// its connectors land, so a connector of it may declare after_landing
	// delete.
	Deletes bool
	// RefreshesMetrics says that a worker of this build reads again, at the
	// source, the metrics of each item that its connectors sighted and that
	// waits in review, so serve queues that refresh on its schedule.
	RefreshesMetrics bool
	// Rules answers why a connector of the source breaks the source's own
	// rules beside its schema, such as the source_parameters it takes, one
	// problem each. A source with no rules takes no source_parameters,
	// because nothing would read them.
	Rules func(Connector) []string
}

var sources = map[string]Source{}

// RegisterSource records s. Registering a source twice panics, as a duplicate
// is a build error.
func RegisterSource(s Source) {
	if _, taken := sources[s.Name]; taken {
		panic("connectors: source " + s.Name + " is registered twice")
	}
	sources[s.Name] = s
}

// sourceProblems is every reason c breaks what its source, as this build
// registers it, takes.
func (c Connector) sourceProblems() []string {
	var out []string
	s := sources[c.Source]
	if s.Polls && c.Mode != ModePoll {
		out = append(out, "mode: a worker polls source "+c.Source+", so its connector's mode is poll, not "+c.Mode)
	}
	if c.AfterLanding == AfterLandingDelete && !s.Deletes {
		out = append(out, "after_landing: source "+c.Source+" registers no deleter in this build, so nothing would delete what it lands")
	}
	switch {
	case s.Rules != nil:
		out = append(out, s.Rules(c)...)
	case len(c.SourceParameters) > 0:
		out = append(out, "source_parameters: source "+c.Source+" registers no rules in this build, so nothing would read them")
	}
	return out
}
