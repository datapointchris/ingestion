-- A cursor is one connector's position and when the scheduler next makes it
-- due. first_fill is none until an item reaches a model stage, held once one
-- has and every later item waits there, and released once a person lets the
-- rest through. last_error and last_error_ts hold the newest failed run, and
-- failures counts the runs that failed in a row. stale_at is the stale time
-- that connectors.StaleAt writes on each cursor put. finished_ts is when a
-- connector scheduled once ran a poll that ended done, after which it never
-- runs again. A cursor with no recorded creation reads as made at the Unix
-- epoch, so it comes due at once.
-- A declarations row is one applied set of connectors, routing rules and
-- destinations, under the commit it was applied from. Applying a commit again
-- stamps it again, and the newest applied_ts is the set in force.

CREATE TABLE IF NOT EXISTS cursors (
    connector           TEXT PRIMARY KEY,
    position            TEXT,
    due_at              TEXT NOT NULL,
    stale_at            TEXT,
    last_run_ts         TEXT,
    last_delivery_ts    TEXT,
    last_error          TEXT,
    last_error_ts       TEXT,
    failures            INTEGER NOT NULL DEFAULT 0,
    first_fill          TEXT NOT NULL CHECK (first_fill IN ('held', 'released', 'none')),
    first_fill_item_key TEXT,
    finished_ts         TEXT,
    created_ts         TEXT NOT NULL DEFAULT '1970-01-01T00:00:00.000Z',
    updated_ts          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cursors_by_stamp ON cursors (updated_ts);

CREATE TABLE IF NOT EXISTS declarations (
    "commit"      TEXT PRIMARY KEY,
    applied_ts    TEXT NOT NULL,
    applied_by    TEXT NOT NULL,
    connectors    TEXT NOT NULL DEFAULT '[]',
    routing_rules TEXT NOT NULL DEFAULT '[]',
    destinations  TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS declarations_by_stamp ON declarations (applied_ts);
