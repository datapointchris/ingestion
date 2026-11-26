-- A task is one stage's work on one item, one poll of a connector, or the
-- draft of one prompt proposal, leased by a worker holding its capability. It
-- names exactly one of the three. lease_token rises with every claim, so a
-- report under an older token is refused. held_by names the connector whose
-- first fill holds a model stage's task, which no claim takes until a person
-- releases the fill. destination names the destination a deliver task writes
-- its item to. model names the model that a measurement arm's task asks its
-- run for. sighting_connector names the connector whose sighting a delete
-- task removes from that connector's source. attempts counts every claim, and
-- lapses the leases in a row that lapsed with no report accepted, which any
-- accepted report resets. error is outsider-written,
-- because an extractor's stderr can quote the item, so the outbox ships it as
-- ingestion.text.

CREATE TABLE IF NOT EXISTS tasks (
    id               TEXT PRIMARY KEY,
    item_key         TEXT REFERENCES items (key),
    connector        TEXT,
    destination      TEXT,
    model            TEXT,
    sighting_connector TEXT,
    proposal_id      TEXT REFERENCES prompt_proposals (id),
    stage            TEXT NOT NULL,
    capability       TEXT NOT NULL,
    state            TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'done', 'failed')),
    held_by          TEXT,
    lease_holder     TEXT,
    lease_token      INTEGER,
    lease_expires_ts TEXT,
    attempts         INTEGER NOT NULL DEFAULT 0,
    lapses           INTEGER NOT NULL DEFAULT 0,
    not_before_ts    TEXT,
    error            TEXT,
    created_ts       TEXT NOT NULL,
    updated_ts       TEXT NOT NULL,
    CHECK ((item_key IS NOT NULL) + (connector IS NOT NULL) + (proposal_id IS NOT NULL) = 1)
);
CREATE INDEX IF NOT EXISTS tasks_by_stamp ON tasks (updated_ts);
CREATE INDEX IF NOT EXISTS tasks_claimable ON tasks (capability, state, created_ts);
CREATE INDEX IF NOT EXISTS tasks_by_item ON tasks (item_key, stage);
CREATE INDEX IF NOT EXISTS tasks_by_connector ON tasks (connector, state);
CREATE INDEX IF NOT EXISTS tasks_held ON tasks (held_by) WHERE held_by IS NOT NULL;

CREATE TABLE IF NOT EXISTS workers (
    id           TEXT PRIMARY KEY,
    capabilities TEXT NOT NULL DEFAULT '[]',
    version      TEXT NOT NULL,
    started_ts   TEXT NOT NULL,
    heartbeat_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS workers_by_stamp ON workers (heartbeat_ts);
