-- A mark is a person's word on an item: low-quality where it held nothing worth
-- keeping, and read where it was read and gave what was wanted. Each is
-- appended. It keeps the quality gate's newest decision on the item when it was
-- made, the prompt version behind that decision, whether the mark agreed with
-- it, and each family it counted toward.
--
-- A gate_trust row is one family's standing on the trust ladder: one connector
-- under one version of the quality gate's prompt. rung, run, straight_run and
-- share are the ladder's, threshold is the straight run that earns asking, and
-- first_share the share that rung 1 shows. asks_promotion is set on a family
-- not yet promoted whose straight run reached its threshold. promoted_by and
-- promoted_ts record the person's yes. last_change and last_item_key name what
-- moved the row last.

CREATE TABLE IF NOT EXISTS marks (
    item_key        TEXT NOT NULL REFERENCES items (key),
    sequence        INTEGER NOT NULL,
    mark            TEXT NOT NULL CHECK (mark IN ('low-quality', 'read')),
    decided_by      TEXT NOT NULL,
    client_id       TEXT,
    gate_verdict    TEXT,
    gate_decided_by TEXT,
    prompt_version  TEXT,
    agreement       TEXT NOT NULL CHECK (agreement IN ('agreed', 'disagreed', 'neither')),
    families        TEXT NOT NULL DEFAULT '[]',
    marked_ts       TEXT NOT NULL,
    PRIMARY KEY (item_key, sequence)
);
CREATE INDEX IF NOT EXISTS marks_by_stamp ON marks (marked_ts);

CREATE TABLE IF NOT EXISTS gate_trust (
    family             TEXT PRIMARY KEY,
    connector          TEXT NOT NULL,
    prompt_version     TEXT NOT NULL,
    rung               INTEGER NOT NULL,
    run                INTEGER NOT NULL,
    straight_run       INTEGER NOT NULL,
    share              REAL NOT NULL,
    threshold          INTEGER NOT NULL,
    first_share        REAL NOT NULL,
    asks_promotion     INTEGER NOT NULL,
    promoted_by        TEXT,
    promoted_client_id TEXT,
    promoted_ts        TEXT,
    last_change        TEXT NOT NULL,
    last_item_key      TEXT,
    created_ts         TEXT NOT NULL,
    updated_ts         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gate_trust_by_stamp ON gate_trust (updated_ts);
