-- +goose Up
-- The provider payout mapping (17.11): one row per platform payout, the second
-- replay barrier behind the inbox and the source of the status polling.
CREATE TABLE provider_payouts (
    payout_id         uuid PRIMARY KEY,
    provider          text NOT NULL,
    gateway_payout_id text,
    state             text NOT NULL DEFAULT 'submitted' CHECK (state IN ('submitted', 'confirmed', 'failed')),
    amount_minor      bigint NOT NULL CHECK (amount_minor > 0),
    asset             text NOT NULL,
    decimals          smallint NOT NULL CHECK (decimals >= 0 AND decimals <= 18),
    network           text NOT NULL,
    address_hash      text NOT NULL,
    txid              text,
    fee_minor         bigint CHECK (fee_minor IS NULL OR fee_minor >= 0),
    fee_asset         text,
    fee_decimals      smallint CHECK (fee_decimals IS NULL OR (fee_decimals >= 0 AND fee_decimals <= 18)),
    reason            text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    confirmed_at      timestamptz,
    failed_at         timestamptz
);

CREATE UNIQUE INDEX provider_payouts_gateway_idx ON provider_payouts (provider, gateway_payout_id)
    WHERE gateway_payout_id IS NOT NULL;
CREATE INDEX provider_payouts_pending_idx ON provider_payouts (updated_at)
    WHERE state = 'submitted';

-- +goose Down
DROP TABLE IF EXISTS provider_payouts;
