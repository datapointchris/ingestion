-- A cursor is one connector's position and when the scheduler next runs it.
-- A declarations row is one applied set of connector declarations, appended.

CREATE TABLE IF NOT EXISTS cursors (
    connector           TEXT PRIMARY KEY,
    position            TEXT,
    due_at              TEXT NOT NULL,
    last_run_ts         TEXT,
    last_delivery_ts    TEXT,
    first_fill          TEXT NOT NULL CHECK (first_fill IN ('held', 'released', 'none')),
    first_fill_item_key TEXT,
    updated_ts          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cursors_by_stamp ON cursors (updated_ts);

CREATE TABLE IF NOT EXISTS declarations (
    "commit"   TEXT PRIMARY KEY,
    applied_ts TEXT NOT NULL,
    applied_by TEXT NOT NULL,
    connectors TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS declarations_by_stamp ON declarations (applied_ts);
