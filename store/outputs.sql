-- One row per item, stage, producer and producer version, so a new extractor,
-- rule set or prompt writes beside the old one. A parse's units are rows of
-- parse_units, so text and units stay null, kept for a store that holds them.
-- text, units, spans, answer and error are outsider-written, so the outbox
-- ships them as ingestion.text. A model stage's model and effort are what it
-- asked its run for, and answered_model is the model its run reports.
-- rules_version is the scanner rules' version that made findings: a scan's
-- over its parse, and a model read's over its suggestion sentence.

CREATE TABLE IF NOT EXISTS outputs (
    item_key         TEXT NOT NULL REFERENCES items (key),
    stage            TEXT NOT NULL,
    producer         TEXT NOT NULL,
    producer_version TEXT NOT NULL,
    lens             TEXT,
    lens_version     TEXT,
    input_producer   TEXT,
    input_version    TEXT,
    state            TEXT NOT NULL CHECK (state IN ('waiting', 'done', 'failed')),
    blob_sha256      TEXT,
    dispatch_id      TEXT,
    run_id           TEXT,
    model            TEXT,
    effort           TEXT,
    answered_model   TEXT,
    tokens           TEXT,
    findings         TEXT,
    rules_version    TEXT,
    quality          INTEGER,
    side_guess       TEXT,
    side_confidence  REAL,
    text             TEXT,
    units            TEXT,
    spans            TEXT,
    answer           TEXT,
    error            TEXT,
    created_ts       TEXT NOT NULL,
    updated_ts       TEXT NOT NULL,
    PRIMARY KEY (item_key, stage, producer, producer_version)
);
CREATE INDEX IF NOT EXISTS outputs_by_stamp ON outputs (updated_ts);
