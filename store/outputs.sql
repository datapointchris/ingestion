-- One row per item, stage, producer and producer version, so a new extractor,
-- rule set or prompt writes beside the old one. A parse's units are rows of
-- parse_units, so text and units stay null, kept for a store that holds them.
-- text, units, spans, answer and error are outsider-written, so the outbox
-- ships them as ingestion.text. A model stage's model and effort are what it
-- asked its run for, and answered_model is the model its run reports.
-- rules_version is the scanner rules' version that made findings: a scan's
-- over its parse, and a model read's over its suggestion sentence.
-- note_digest is the hex SHA-256 of the note that a model read answered, and
-- null where it answered none. input_producer and input_version name the
-- parse that a scan read, or that a model stage's prompt quoted, so a model
-- stage's answer over an older parse is due again. quality, fit, side_guess
-- and side_confidence are the quality gate's, fit null where the gate read no
-- context.

CREATE TABLE IF NOT EXISTS outputs (
    item_key         TEXT NOT NULL REFERENCES items (key),
    stage            TEXT NOT NULL,
    producer         TEXT NOT NULL,
    producer_version TEXT NOT NULL,
    lens             TEXT,
    lens_version     TEXT,
    note_digest      TEXT,
    input_producer   TEXT,
    input_version    TEXT,
    state            TEXT NOT NULL CHECK (state IN ('waiting', 'done', 'failed', 'deferred')),
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
    fit              INTEGER,
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

-- An output_replacements row is a model stage's output that is not done yet,
-- over another parse than the item's done output under the same stage,
-- producer and version. It holds the call's dispatch beside the done answer,
-- which stays the item's answer until the call answers and its output takes
-- the done one's place. The store keeps it for itself, so the outbox never
-- ships it, and the task's state and error say how the call stands.

CREATE TABLE IF NOT EXISTS output_replacements (
    item_key         TEXT NOT NULL REFERENCES items (key),
    stage            TEXT NOT NULL,
    producer         TEXT NOT NULL,
    producer_version TEXT NOT NULL,
    lens             TEXT,
    lens_version     TEXT,
    note_digest      TEXT,
    input_producer   TEXT,
    input_version    TEXT,
    state            TEXT NOT NULL CHECK (state IN ('waiting', 'failed')),
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
    fit              INTEGER,
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
