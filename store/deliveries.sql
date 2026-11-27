-- One row per item and destination, written pending when its deliver task is
-- queued. refused is a destination's answer naming an existing record that
-- conflicts with the item, kept with that record's id and never retried.
-- title, url and source_ts are the item's when the delivery was first queued,
-- and every attempt sends them, so a retry repeats the first request. path is
-- kept the same way, for a deliverer that writes an item by its file: the
-- path of the item's live home below the root its config names, and null for
-- every other deliverer. A book's chapter keeps the same way where it sits in
-- its book: book_destination_id is the destination's id for the book's record,
-- from the book's settled delivery, and the pages are the newest sections'
-- that name the chapter. Each is null for every other item. playlist is the
-- playlist that a watch adds the video to, from the side decision, kept the
-- same way, and null for every other deliverer. last_error is the
-- destination's answer, or why the delivery was withdrawn. It, title, url and
-- path can quote the item, so the outbox ships them as ingestion.text.

CREATE TABLE IF NOT EXISTS deliveries (
    item_key            TEXT NOT NULL REFERENCES items (key),
    destination         TEXT NOT NULL,
    destination_id      TEXT,
    outcome             TEXT NOT NULL CHECK (outcome IN ('pending', 'delivered', 'delivered-to-existing', 'refused')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    title               TEXT,
    url                 TEXT,
    source_ts           TEXT,
    path                TEXT,
    book_destination_id TEXT,
    pdf_page_start      INTEGER,
    pdf_page_end        INTEGER,
    printed_page_start  TEXT,
    printed_page_end    TEXT,
    playlist            TEXT,
    updated_ts          TEXT NOT NULL,
    PRIMARY KEY (item_key, destination)
);
CREATE INDEX IF NOT EXISTS deliveries_by_stamp ON deliveries (updated_ts);
