-- Each gate decision is appended, and the newest at a gate decides. A row is
-- never updated, so the outbox ships each once. A model read's lens names how
-- the model reads the item. client_id names the client that sent a person's
-- decision, which for a service sending it on a person's behalf is the only
-- record of the service.

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
    lens        TEXT,
    client_id   TEXT,
    decided_ts  TEXT NOT NULL,
    PRIMARY KEY (item_key, gate, sequence)
);
CREATE INDEX IF NOT EXISTS decisions_by_stamp ON decisions (decided_ts);
