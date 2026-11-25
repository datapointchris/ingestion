-- One row per replay that queued a stage again over landed items. Its id
-- derives from the caller and its Idempotency-Key, so a retry after a lost
-- answer finds the replay the first request queued rather than queueing the
-- stage twice.

CREATE TABLE IF NOT EXISTS replays (
    id           TEXT PRIMARY KEY,
    stage        TEXT NOT NULL,
    connector    TEXT,
    item_key     TEXT,
    placed_by    TEXT,
    items        INTEGER NOT NULL,
    requested_by TEXT NOT NULL,
    requested_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS replays_by_stamp ON replays (requested_ts);
