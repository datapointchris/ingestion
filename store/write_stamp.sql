-- The stamp of the write in flight. Update sets it as each transaction begins,
-- and each logged table's triggers refuse a row stamped otherwise, so a write
-- that forgets its stamp fails rather than hiding behind the outbox's mark.

CREATE TABLE IF NOT EXISTS write_stamp (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    stamp TEXT NOT NULL
);
