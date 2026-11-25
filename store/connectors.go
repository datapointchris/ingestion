package store

import (
	"context"
	"fmt"
	"strings"
)

// AppliedDeclarations reads the set in force, the one applied last, and false
// where no set has been applied.
func (s *Store) AppliedDeclarations(ctx context.Context) (Declarations, bool, error) {
	return first[Declarations](ctx, s.read, table("declarations"), `1 ORDER BY applied_ts DESC`)
}

// Cursor reads connector's cursor, and false where the store holds none.
func (s *Store) Cursor(ctx context.Context, connector string) (Cursor, bool, error) {
	return first[Cursor](ctx, s.read, table("cursors"), `connector = ?`, connector)
}

// ConnectorCounts counts the items each connector sighted: all of them, and
// those that wait at a model stage, wait on a person's placement, or failed.
type ConnectorCounts struct {
	Items   int
	Waiting int
	Review  int
	Failed  int
	// Held counts the items whose model stage the connector's own first fill
	// holds.
	Held int
}

// connectorCounts counts each item a connector sighted by the phase on its
// row, which the outbox ships, so a reader of the records counts the same.
const connectorCounts = `
SELECT s.connector,
	count(*),
	coalesce(sum(i.phase = 'waiting'), 0),
	coalesce(sum(i.phase = 'review'), 0),
	coalesce(sum(i.phase = 'failed'), 0),
	coalesce(sum(
		EXISTS (SELECT 1 FROM tasks t WHERE t.item_key = s.item_key AND t.state = 'queued' AND t.held_by = s.connector)
	), 0)
FROM sightings s JOIN items i ON i.key = s.item_key
GROUP BY s.connector`

// ConnectorCounts reads every connector's counts, by its name. A connector
// that sighted nothing is absent.
func (s *Store) ConnectorCounts(ctx context.Context) (map[string]ConnectorCounts, error) {
	rows, err := s.read.QueryContext(ctx, connectorCounts)
	if err != nil {
		return nil, fmt.Errorf("count each connector's items: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]ConnectorCounts{}
	for rows.Next() {
		var name string
		var c ConnectorCounts
		if err := rows.Scan(&name, &c.Items, &c.Waiting, &c.Review, &c.Failed, &c.Held); err != nil {
			return nil, fmt.Errorf("count each connector's items: %w", err)
		}
		out[name] = c
	}
	return out, rows.Err()
}

// ItemZones reads each of keys' zone, by key, and leaves out each key with
// no item.
func (s *Store) ItemZones(ctx context.Context, keys []string) (map[string]Zone, error) {
	out := map[string]Zone{}
	if len(keys) == 0 {
		return out, nil
	}
	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}
	items, err := readInto[Item](ctx, s.read, table("items"), `key IN (?`+strings.Repeat(", ?", len(keys)-1)+`)`, args...)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		out[it.Key] = it.Zone
	}
	return out, nil
}

// Outputs reads every output of the item under key, oldest first.
func (s *Store) Outputs(ctx context.Context, key string) ([]Output, error) {
	return readInto[Output](ctx, s.read, table("outputs"), `item_key = ? ORDER BY created_ts, rowid`, key)
}

// LiveHomesAt reads every home at root and pathSHA256 that is not gone,
// whatever blob it names.
func (s *Store) LiveHomesAt(ctx context.Context, root, pathSHA256 string) ([]Home, error) {
	return readInto[Home](ctx, s.read, table("homes"), `root = ? AND path_sha256 = ? AND gone_ts IS NULL`, root, pathSHA256)
}

// Sighted says whether connector sighted the item under key.
func (s *Store) Sighted(ctx context.Context, key, connector string) (bool, error) {
	_, ok, err := first[Sighting](ctx, s.read, table("sightings"), `item_key = ? AND connector = ?`, key, connector)
	return ok, err
}

// AppliedDeclarations reads the set in force as this write sees it, and false
// where no set has been applied.
func (t *Tx) AppliedDeclarations(ctx context.Context) (Declarations, bool, error) {
	return first[Declarations](ctx, t.tx, table("declarations"), `1 ORDER BY applied_ts DESC`)
}

// KeyedRequest reads the request caller sent under key, and false where the
// store holds none.
func (s *Store) KeyedRequest(ctx context.Context, caller, key string) (KeyedRequest, bool, error) {
	return first[KeyedRequest](ctx, s.read, table("keyed_requests"), `caller = ? AND idempotency_key = ?`, caller, key)
}
