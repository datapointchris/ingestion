-- A routing_rules row is one version of a routing rule that a person
-- accepted, appended in its order under the rule's name. A rule written by
-- hand stays in the declaration set's routing-rules.yml. The newest version
-- of a name is in force unless it is retired, and the side gate reads each
-- rule in force at its rule rung, after every declared rule.
--
-- action is block, which drops each item that the rule matches, or keep,
-- which places it on destination. The match columns are a declared rule's
-- match: the connector that sighted the item, a tag of a sighting, and a
-- field of a sighting with the values it holds for. match_values copies a
-- source's field, so it ships as outsider text. review_share is the share of
-- matched items that wait in review as rule-would-hide instead, picked by a
-- seeded draw over the rule's name and the item's key. retired marks a
-- version that takes the rule out of force, copying the version before it.
-- proposal names the proposal that the person accepted, where one did.
-- decided_by is person:<login>, or service:<client id> where a service named
-- nobody, and client_id the client that sent it.

CREATE TABLE IF NOT EXISTS routing_rules (
    name            TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    action          TEXT NOT NULL CHECK (action IN ('block', 'keep')),
    match_connector TEXT,
    match_tag       TEXT,
    match_field     TEXT,
    match_values    TEXT NOT NULL DEFAULT '[]',
    destination     TEXT,
    review_share    REAL NOT NULL CHECK (review_share >= 0 AND review_share <= 1),
    retired         INTEGER NOT NULL,
    proposal        TEXT,
    decided_by      TEXT NOT NULL,
    client_id       TEXT,
    decided_ts      TEXT NOT NULL,
    PRIMARY KEY (name, sequence)
);
CREATE INDEX IF NOT EXISTS routing_rules_by_stamp ON routing_rules (decided_ts);
