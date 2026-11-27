package connectors

import (
	"context"
	"fmt"

	"github.com/datapointchris/ingestion/store"
)

// Admission decides, inside one claim's write, whether each task that the
// claim would lease still has work that someone should do. It reads the set
// in force once, at the first task that the set governs, so one Admission
// serves one claim.
type Admission struct {
	set *Set
}

// Admit answers why nobody should run task now, and "" where it runs. The
// set in force governs three stages, and every other task always runs:
//
//   - A poll runs while the set polls its connector from the source that the
//     task reads.
//   - A delete runs while the set declares that its connector deletes from
//     that source after landing.
//   - A refresh of a source's metrics runs while the set declares a
//     connector of the source and this build reads its metrics, and while an
//     item that such a connector sighted waits in review with metrics that
//     no read took since the task was queued. keys names those items, which
//     the lease hands the worker, so a retry reads only what is left.
func (a *Admission) Admit(ctx context.Context, tx *store.Tx, task store.Task) (why string, keys []string, err error) {
	switch {
	case task.Stage == store.StagePoll && task.Connector != nil:
		s, err := a.applied(ctx, tx)
		if err != nil {
			return "", nil, err
		}
		c, ok := s.Connector(*task.Connector)
		if !ok || c.Mode != ModePoll || store.PollCapability(c.Source) != task.Capability {
			return fmt.Sprintf("the set in force does not poll connector %s from this source, so nobody runs this poll", *task.Connector), nil, nil
		}
	case task.Stage == store.StageDelete && task.SightingConnector != nil:
		s, err := a.applied(ctx, tx)
		if err != nil {
			return "", nil, err
		}
		c, ok := s.Connector(*task.SightingConnector)
		if !ok || c.AfterLanding != AfterLandingDelete || store.DeleteCapability(c.Source) != task.Capability {
			return fmt.Sprintf("the set in force does not delete what connector %s lands from this source, so nobody runs this delete",
				*task.SightingConnector), nil, nil
		}
	case task.Stage == store.StageRefreshMetrics && task.Source != nil:
		s, err := a.applied(ctx, tx)
		if err != nil {
			return "", nil, err
		}
		names := s.SourceConnectors(*task.Source)
		if len(names) == 0 || !RefreshesMetrics(*task.Source) {
			return fmt.Sprintf("the set in force declares no connector of source %s whose metrics this build reads, so nobody runs this refresh",
				*task.Source), nil, nil
		}
		keys, err := tx.ReviewKeysSightedBy(ctx, names, task.CreatedTS)
		if err != nil {
			return "", nil, err
		}
		if len(keys) == 0 {
			return fmt.Sprintf("no item that a connector of source %s sighted waits in review unread since this refresh was queued, so nothing is read",
				*task.Source), nil, nil
		}
		return "", keys, nil
	}
	return "", nil, nil
}

// Connector is connector name's declaration in the set that the admission
// read, and false where it read none or the set lacks it. A poll that Admit
// let through was read against that set.
func (a *Admission) Connector(name string) (Connector, bool) {
	if a.set == nil {
		return Connector{}, false
	}
	return a.set.Connector(name)
}

// applied is the set in force, read in tx at the first call.
func (a *Admission) applied(ctx context.Context, tx *store.Tx) (Set, error) {
	if a.set == nil {
		s, err := AppliedSet(ctx, tx)
		if err != nil {
			return Set{}, err
		}
		a.set = &s
	}
	return *a.set, nil
}
