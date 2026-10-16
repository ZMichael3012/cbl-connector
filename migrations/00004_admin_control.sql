-- +goose Up
-- Administrative contour of the connector (CBL-12.2): the routing configuration
-- revision for the optimistic concurrency of PutRoutingRules and the manual
-- provider disable list honoured by the route source.
CREATE TABLE IF NOT EXISTS routing_config (
    id         smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    revision   bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO routing_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS disabled_providers (
    provider    text PRIMARY KEY,
    reason      text NOT NULL DEFAULT '',
    disabled_at timestamptz NOT NULL DEFAULT now()
);
