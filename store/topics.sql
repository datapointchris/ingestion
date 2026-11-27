-- One row per topic of one memo, as the topics stage split its transcript.
-- A memo's topics are written once, in the write that lands each as a child
-- item. n numbers the topics from 1 in their order, and child_key names the
-- topic's item, memo/<hash>#n. first_unit and last_unit are the units of the
-- memo's parse that the topic holds, counted from 1, and the topics of one
-- memo cover every unit in order. producer and producer_version name the
-- topics output that answered, and input_producer and input_version the parse
-- that it split. title and idea are a model's words over the transcript, so
-- the outbox ships them as ingestion.text.

CREATE TABLE IF NOT EXISTS topics (
    item_key         TEXT NOT NULL REFERENCES items (key),
    n                INTEGER NOT NULL CHECK (n >= 1),
    child_key        TEXT NOT NULL,
    first_unit       INTEGER NOT NULL CHECK (first_unit >= 1),
    last_unit        INTEGER NOT NULL,
    title            TEXT NOT NULL,
    idea             TEXT NOT NULL,
    producer         TEXT NOT NULL,
    producer_version TEXT NOT NULL,
    input_producer   TEXT NOT NULL,
    input_version    TEXT NOT NULL,
    created_ts       TEXT NOT NULL,
    PRIMARY KEY (item_key, n),
    CHECK (last_unit >= first_unit)
);
CREATE INDEX IF NOT EXISTS topics_by_stamp ON topics (created_ts);
CREATE INDEX IF NOT EXISTS topics_by_child ON topics (child_key);
