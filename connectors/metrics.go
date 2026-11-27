package connectors

import (
	"context"
	"maps"
	"slices"

	"github.com/datapointchris/ingestion/store"
)

// RefreshesMetrics says whether this build registers source with a worker
// that reads its items' metrics again.
func RefreshesMetrics(source string) bool { return sources[source].RefreshesMetrics }

// SourceConnectors names, in the set's order, each connector of s that reads
// source.
func (s Set) SourceConnectors(source string) []string {
	var out []string
	for _, c := range s.Connectors {
		if c.Source == source {
			out = append(out, c.Name)
		}
	}
	return out
}

// InReview reads, in key order, the key of each item in phase review that a
// connector of source in s sighted. These are the items whose metrics a
// refresh of source reads again.
func InReview(ctx context.Context, tx *store.Tx, s Set, source string) ([]string, error) {
	return tx.ReviewKeysSightedBy(ctx, s.SourceConnectors(source))
}

// MetricsRefreshed counts what a refresh's report wrote: the items it read
// metrics for, the sightings whose metrics changed, and the items that no
// connector of the source sighted, whose metrics went nowhere.
type MetricsRefreshed struct {
	Items     int
	Sightings int
	Unsighted int
}

// RefreshMetrics writes, in tx, each item's metrics in read onto every
// sighting of it by a connector of source in the set in force, as a refresh
// of source read them again. It changes the metrics alone, so it lands
// nothing, and queues no delete and no gate. It refuses a metric whose name
// is not a metric's with store.ErrInvalidMetric.
func RefreshMetrics(ctx context.Context, tx *store.Tx, source string, read map[string]map[string]int64) (MetricsRefreshed, error) {
	out := MetricsRefreshed{Items: len(read)}
	if len(read) == 0 {
		return out, nil
	}
	set, err := AppliedSet(ctx, tx)
	if err != nil {
		return out, err
	}
	ours := map[string]bool{}
	for _, name := range set.SourceConnectors(source) {
		ours[name] = true
	}
	for _, key := range slices.Sorted(maps.Keys(read)) {
		if err := store.CheckMetrics(read[key]); err != nil {
			return out, err
		}
		sightings, err := tx.Sightings(ctx, key)
		if err != nil {
			return out, err
		}
		sighted := false
		for _, s := range sightings {
			if !ours[s.Connector] {
				continue
			}
			sighted = true
			changed, err := tx.PutSightingMetrics(ctx, key, s.Connector, read[key])
			if err != nil {
				return out, err
			}
			if changed {
				out.Sightings++
			}
		}
		if !sighted {
			out.Unsighted++
		}
	}
	return out, nil
}
