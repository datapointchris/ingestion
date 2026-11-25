-- A keyed request is one request a caller sent under an Idempotency-Key, with
-- the status and body it answered. A retry under the key answers the same, and
-- a request with another fingerprint under it is refused.

CREATE TABLE IF NOT EXISTS keyed_requests (
    caller          TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    route           TEXT NOT NULL,
    connector       TEXT,
    fingerprint     TEXT NOT NULL,
    status          INTEGER NOT NULL,
    answer          TEXT NOT NULL,
    created_ts      TEXT NOT NULL,
    PRIMARY KEY (caller, idempotency_key)
);
CREATE INDEX IF NOT EXISTS keyed_requests_by_stamp ON keyed_requests (created_ts);
