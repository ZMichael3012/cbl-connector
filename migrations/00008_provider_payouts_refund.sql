-- +goose Up
-- The refund marker of the mapping (17.12): a non-empty value means the row
-- tracks a provider refund created through the provider refunds API. The value
-- is the canonical platform payment UUID being refunded.
ALTER TABLE provider_payouts
    ADD COLUMN refund_invoice_id text
        CHECK (refund_invoice_id IS NULL OR refund_invoice_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');

-- +goose Down
ALTER TABLE provider_payouts
    DROP COLUMN IF EXISTS refund_invoice_id;
