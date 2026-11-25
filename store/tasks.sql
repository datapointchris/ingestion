-- A task is one stage's work on one item, leased by a worker holding its
-- capability. lease_token rises with every claim, so a report under an older
-- token is refused. error is outsider-written, because an extractor's stderr
-- can quote the item, so the outbox ships it as ingestion.text.

CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    item_key         TEXT NOT NULL REFERENCES items (key),
    stage            TEXT NOT NULL,
    capability       TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'done', 'failed')),
    lease_holder     TEXT,
    lease_token      INTEGER,
    lease_expires_ts TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    not_before_ts    TEXT,
    error            TEXT,
    created_ts       TEXT NOT NULL,
    updated_ts       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_by_stamp ON tasks (updated_ts);
CREATE INDEX IF NOT EXISTS tasks_claimable ON tasks (capability, state, created_ts);
CREATE INDEX IF NOT EXISTS tasks_by_item ON tasks (item_key, stage);

CREATE TABLE IF NOT EXISTS workers (
    id           TEXT PRIMARY KEY,
    capabilities TEXT NOT NULL DEFAULT '[]',
    version      TEXT NOT NULL,
    started_ts   TEXT NOT NULL,
    heartbeat_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS workers_by_stamp ON workers (heartbeat_ts);
