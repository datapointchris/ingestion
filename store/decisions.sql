-- Each gate decision is appended, and the newest at a gate decides. A row is
-- never updated, so the outbox ships each once.

CREATE TABLE IF NOT EXISTS decisions (
    item_key    TEXT NOT NULL REFERENCES items (key),
    gate        TEXT NOT NULL CHECK (gate IN ('quality', 'side', 'read')),
    sequence    INTEGER NOT NULL,
    verdict     TEXT NOT NULL,
    decided_by  TEXT NOT NULL,
    confidence  REAL,
    cause       TEXT NOT NULL,
    destination TEXT,
    note        TEXT,
    decided_ts  TEXT NOT NULL,
    PRIMARY KEY (item_key, gate, sequence)
);
CREATE INDEX IF NOT EXISTS decisions_by_stamp ON decisions (decided_ts);
