-- One row per item and destination, written pending when its deliver task is
-- queued. refused is a destination's answer naming an existing record that
-- conflicts with the item, kept with that record's id and never retried.
-- title and url are the item's when the delivery was first queued, and every
-- attempt sends them, so a retry repeats the first request. last_error is the
-- destination's answer, or why the delivery was withdrawn. Each of the three
-- can quote the item, so the outbox ships them as ingestion.text.

CREATE TABLE IF NOT EXISTS deliveries (
    item_key       TEXT NOT NULL REFERENCES items (key),
    destination    TEXT NOT NULL,
    destination_id TEXT,
    outcome        TEXT NOT NULL CHECK (outcome IN ('pending', 'delivered', 'delivered-to-existing', 'refused')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    title          TEXT,
    url            TEXT,
    updated_ts     TEXT NOT NULL,
    PRIMARY KEY (item_key, destination)
);
CREATE INDEX IF NOT EXISTS deliveries_by_stamp ON deliveries (updated_ts);
