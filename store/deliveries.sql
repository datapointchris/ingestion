-- One row per item and destination. refused is a 409 naming an existing
-- record, kept and never retried.

CREATE TABLE IF NOT EXISTS deliveries (
    item_key       TEXT NOT NULL REFERENCES items (key),
    destination    TEXT NOT NULL,
    destination_id TEXT,
    outcome        TEXT NOT NULL CHECK (outcome IN ('pending', 'delivered', 'delivered-to-existing', 'refused')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    updated_ts     TEXT NOT NULL,
    PRIMARY KEY (item_key, destination)
);
CREATE INDEX IF NOT EXISTS deliveries_by_stamp ON deliveries (updated_ts);
