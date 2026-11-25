-- How far the outbox has shipped each table. The collector accepted every row
-- up to through_stamp and through_rowid. shipped counts the records every
-- accepted batch carried, resends included.

CREATE TABLE IF NOT EXISTS outbox_marks (
    source_table    TEXT PRIMARY KEY,
    through_stamp   TEXT NOT NULL,
    through_rowid   INTEGER NOT NULL,
    shipped         INTEGER NOT NULL,
    moved_ts        TEXT NOT NULL,
    service_version TEXT NOT NULL
);
