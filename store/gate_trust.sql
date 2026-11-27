-- A mark is a person's word on an item: low-quality where it held nothing worth
-- keeping, and read where it was read and gave what was wanted. Each is
-- appended. It keeps the quality gate's newest decision on the item when it was
-- made, the prompt version behind that decision, whether the mark agreed with
-- it, and each family it counted toward.
--
-- A gate_trust row is one family's standing on the trust ladder: one connector
-- under one version of the quality gate's prompt, answered by one model at one
-- effort. model is the model that a run reports answering on, else the model
-- it was asked for, and default where it named none. effort is the effort it
-- was asked at, and default where it named none. Both decision rows and the
-- family row carry them. rung, run, straight_run and share are the ladder's,
-- threshold is the straight run that earns asking, and
-- first_share the share that rung 1 shows. asks_promotion is set on a family
-- not yet promoted whose straight run reached its threshold. promoted_by and
-- promoted_ts record the person's yes. last_change and last_item_key name what
-- moved the row last. The row is a cache of the standing that earnedtrust
-- reads from the family's gate_trust_decisions and its promotion, written
-- again after each of them.
--
-- A gate_trust_decisions row is one person's word that counts on a family's
-- ladder, appended in its order: a mark that agrees with the gate's drop, a
-- mark that disagrees with its answer, or a placement on a destination of an
-- item whose drop the gate showed or hid. promoted records whether the family
-- was promoted when the gate answered the item, so only a drop that the gate
-- answered after a person's yes climbs past it. Where the family answered an
-- item again, only the words against its newest answer count.

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
    model              TEXT NOT NULL,
    effort             TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS gate_trust_decisions (
    family          TEXT NOT NULL REFERENCES gate_trust (family),
    model           TEXT NOT NULL,
    effort          TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    item_key        TEXT NOT NULL REFERENCES items (key),
    kind            TEXT NOT NULL CHECK (kind IN ('mark', 'placement')),
    agreement       TEXT NOT NULL CHECK (agreement IN ('agreed', 'disagreed')),
    promoted        INTEGER NOT NULL,
    decided_by      TEXT NOT NULL,
    client_id       TEXT,
    gate_decided_by TEXT NOT NULL,
    decided_ts      TEXT NOT NULL,
    PRIMARY KEY (family, sequence)
);
CREATE INDEX IF NOT EXISTS gate_trust_decisions_by_stamp ON gate_trust_decisions (decided_ts);
