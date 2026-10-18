-- +goose Up
-- 4.2: the invoice destination (deposit address or hosted checkout page) is
-- stored with the mapping so the idempotent replay of a create whose response
-- was lost returns the destination instead of leaving the payment without an
-- address.
ALTER TABLE provider_invoices
    ADD COLUMN address      text CHECK (address IS NULL OR char_length(address) <= 256),
    ADD COLUMN checkout_url text CHECK (checkout_url IS NULL OR char_length(checkout_url) <= 2048);

-- +goose Down
ALTER TABLE provider_invoices
    DROP COLUMN checkout_url,
    DROP COLUMN address;
