-- A prompt_proposals row is one draft of a change to a model stage's prompt,
-- made from the quality gate's misses against a person's word. serve writes it
-- when a schedule's time finds misses labeled after labeled_after, through
-- labeled_through, and queues the misses task that drafts it. misses holds the
-- projection that the draft reads, and miss_count counts it. prompt names the
-- stage whose prompt the draft changes, and prompt_version the version that
-- the draft changes.
--
-- The task's run writes the rest, as a model stage writes its output.
-- producer and producer_version name the drafting prompt, which a run writes
-- first. state, dispatch_id, run_id, model, effort, answered_model, tokens and
-- error follow the run, and answer holds the draft. zone is owner, because the
-- projection holds only keys, enums, numbers and a person's own words, and a
-- model's output takes its input's zone.

CREATE TABLE IF NOT EXISTS prompt_proposals (
    id               TEXT PRIMARY KEY,
    prompt           TEXT NOT NULL,
    prompt_version   TEXT NOT NULL,
    labeled_after    TEXT,
    labeled_through  TEXT NOT NULL,
    miss_count       INTEGER NOT NULL,
    misses           TEXT NOT NULL,
    zone             TEXT NOT NULL CHECK (zone IN ('outside', 'owner')),
    producer         TEXT,
    producer_version TEXT,
    state            TEXT NOT NULL CHECK (state IN ('waiting', 'done', 'failed')),
    dispatch_id      TEXT,
    run_id           TEXT,
    model            TEXT,
    effort           TEXT,
    answered_model   TEXT,
    tokens           TEXT,
    answer           TEXT,
    error            TEXT,
    created_ts       TEXT NOT NULL,
    updated_ts       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS prompt_proposals_by_stamp ON prompt_proposals (updated_ts);
CREATE INDEX IF NOT EXISTS prompt_proposals_by_labeled_through ON prompt_proposals (labeled_through);
