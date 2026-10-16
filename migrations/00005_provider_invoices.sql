-- +goose Up
-- Provider invoice mapping (CBL-17.2, closes T-CONNECTOR-03): real provider
-- webhooks do not carry the platform payment_id, so the connector resolves it
-- by (provider, gateway_id); order_id additionally serves the invoice recovery
-- after create timeouts (LookupByOrderID).
CREATE TABLE IF NOT EXISTS provider_invoices (
    id           uuid PRIMARY KEY,
    provider     text NOT NULL,
    gateway_id   text NOT NULL,
    payment_id   uuid NOT NULL,
    order_id     text NOT NULL,
    request_hash text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS provider_invoices_provider_gateway_uidx
    ON provider_invoices (provider, gateway_id);
CREATE INDEX IF NOT EXISTS provider_invoices_payment_idx
    ON provider_invoices (payment_id);
CREATE INDEX IF NOT EXISTS provider_invoices_provider_order_idx
    ON provider_invoices (provider, order_id);
