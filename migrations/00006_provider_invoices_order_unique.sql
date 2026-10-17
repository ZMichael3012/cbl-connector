-- +goose Up
-- One provider invoice per order (17.2, review finding): the order reference is
-- the platform payment id, so a retried creation must replay the stored mapping
-- instead of creating a second invoice at the provider (T-CONNECTOR-07).
CREATE UNIQUE INDEX IF NOT EXISTS provider_invoices_provider_order_uidx
    ON provider_invoices (provider, order_id);
