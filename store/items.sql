-- An item is one artifact under the key that itemkey derives. A sighting is
-- one connector's arrival of it. A blob is landed bytes under their SHA-256,
-- and a home names where a file already lives on disk. A sighting's fields
-- are values its connector read from the source's payload, such as a Reddit
-- save's subreddit, which a routing rule matches. title, url, external_key,
-- fields and path are outsider-written, so the outbox ships them as
-- ingestion.text. An item's phase is what ItemRecords.Phase decides from its
-- records, which every write that touches the item settles.

CREATE TABLE IF NOT EXISTS items (
    key            TEXT PRIMARY KEY,
    media_type     TEXT NOT NULL,
    zone           TEXT NOT NULL CHECK (zone IN ('outside', 'owner')),
    blob_sha256    TEXT,
    parent_key     TEXT,
    flow_id        TEXT NOT NULL,
    parent_flow_id TEXT,
    title          TEXT,
    url            TEXT,
    phase          TEXT,
    created_ts     TEXT NOT NULL,
    updated_ts     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS items_by_stamp ON items (updated_ts);
CREATE INDEX IF NOT EXISTS items_by_parent ON items (parent_key);

CREATE TABLE IF NOT EXISTS sightings (
    item_key        TEXT NOT NULL REFERENCES items (key),
    connector       TEXT NOT NULL,
    tags            TEXT NOT NULL DEFAULT '[]',
    fields          TEXT NOT NULL DEFAULT '{}',
    zone            TEXT NOT NULL CHECK (zone IN ('outside', 'owner')),
    cursor_position TEXT,
    external_key    TEXT NOT NULL,
    first_seen_ts   TEXT NOT NULL,
    last_seen_ts    TEXT NOT NULL,
    PRIMARY KEY (item_key, connector)
);
CREATE INDEX IF NOT EXISTS sightings_by_stamp ON sightings (last_seen_ts);

CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    md5        TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    created_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS blobs_by_stamp ON blobs (created_ts);

CREATE TABLE IF NOT EXISTS homes (
    blob_sha256 TEXT NOT NULL,
    root        TEXT NOT NULL,
    path_sha256 TEXT NOT NULL,
    path        TEXT NOT NULL,
    modified_ts TEXT,
    found_ts    TEXT NOT NULL,
    gone_ts     TEXT,
    PRIMARY KEY (blob_sha256, root, path_sha256)
);
CREATE INDEX IF NOT EXISTS homes_by_stamp ON homes (coalesce(gone_ts, found_ts));
