-- +goose Up
-- Правила маршрутизации провайдеров: источник истины connector (3.4).
-- Redis route:config - только кеш с TTL и фолбэком на эту таблицу.
CREATE TABLE routing_rules (
    id         uuid PRIMARY KEY,
    provider   text NOT NULL,
    operation  text NOT NULL DEFAULT '' CHECK (operation IN ('', 'invoice', 'payout')),
    networks   jsonb NOT NULL DEFAULT '[]',
    assets     jsonb NOT NULL DEFAULT '[]',
    priority   int NOT NULL DEFAULT 0 CHECK (priority >= 0),
    weight     int NOT NULL DEFAULT 1 CHECK (weight > 0),
    min_amount bigint NOT NULL DEFAULT 0 CHECK (min_amount >= 0),
    max_amount bigint NOT NULL DEFAULT 0 CHECK (max_amount >= 0),
    enabled    boolean NOT NULL DEFAULT true,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (min_amount = 0 OR max_amount = 0 OR min_amount <= max_amount)
);
CREATE INDEX routing_rules_route_idx ON routing_rules (enabled, priority);

-- +goose Down
DROP TABLE IF EXISTS routing_rules;
