-- One row per unit of one parse: a thread's post or comment, a book's page, a
-- transcript's segment. A parse's whole text is its units' text in order,
-- joined by a blank line, so the text has one owner. An extractor version's
-- units are written once, beside every other version's. label and text are
-- outsider-written, so the outbox ships them as ingestion.text, each unit
-- apart, and no parse is too large for one record.

CREATE TABLE IF NOT EXISTS parse_units (
    item_key          TEXT NOT NULL REFERENCES items (key),
    extractor         TEXT NOT NULL,
    extractor_version TEXT NOT NULL,
    n                 INTEGER NOT NULL CHECK (n >= 1),
    label             TEXT,
    text              TEXT NOT NULL,
    created_ts        TEXT NOT NULL,
    PRIMARY KEY (item_key, extractor, extractor_version, n)
);
CREATE INDEX IF NOT EXISTS parse_units_by_stamp ON parse_units (created_ts);
