-- One row per entry of one item's sections, as one producer at one version
-- found them: outline from the PDF's own outline, contents from its printed
-- contents pages. A version's rows are written once, beside every other
-- version's. n numbers the entries from 1 in their order, and parent_n names
-- the entry that holds this one, null at the top. A part holds chapters, and
-- a chapter holds sections. Front and back matter, such as a preface or an
-- index, keep their kind and are no chapter. pdf_page_start and pdf_page_end
-- count from 1. printed_page_start and printed_page_end are the pages as the
-- book prints them, from its page labels, else its measured offset, and null
-- where neither gives one. start_placed_by says how the start page was found,
-- such as outline or whole-file. child_key names the item made for a chapter,
-- and is null on every row of a version that made no children. title is
-- outsider-written, so the outbox ships it as ingestion.text.

CREATE TABLE IF NOT EXISTS sections (
    item_key           TEXT NOT NULL REFERENCES items (key),
    producer           TEXT NOT NULL CHECK (producer IN ('outline', 'contents')),
    producer_version   TEXT NOT NULL,
    n                  INTEGER NOT NULL CHECK (n >= 1),
    parent_n           INTEGER,
    level              INTEGER NOT NULL CHECK (level >= 1),
    kind               TEXT NOT NULL CHECK (kind IN ('part', 'chapter', 'section', 'front-matter', 'back-matter')),
    title              TEXT,
    pdf_page_start     INTEGER NOT NULL CHECK (pdf_page_start >= 1),
    pdf_page_end       INTEGER NOT NULL,
    printed_page_start TEXT,
    printed_page_end   TEXT,
    start_placed_by    TEXT NOT NULL,
    child_key          TEXT,
    created_ts         TEXT NOT NULL,
    PRIMARY KEY (item_key, producer, producer_version, n)
);
CREATE INDEX IF NOT EXISTS sections_by_stamp ON sections (created_ts);
CREATE INDEX IF NOT EXISTS sections_by_child ON sections (child_key);
