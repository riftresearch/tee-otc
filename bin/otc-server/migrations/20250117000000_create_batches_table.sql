-- Create batches table to store market maker batch payments
-- This table tracks batch payments by chain and transaction hash

CREATE TABLE batches (
    chain VARCHAR(50) NOT NULL,
    tx_hash VARCHAR(255) NOT NULL,
    full_batch JSONB NOT NULL,
    swap_ids JSONB NOT NULL,
    market_maker_id UUID NOT NULL,
    -- Materialized from full_batch.payment_verification.batch_nonce_digest for fast joins/idempotency
    batch_nonce_digest BYTEA NOT NULL,
    -- Optional but useful: record when otc-server considers the batch fully confirmed/accepted
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain, tx_hash)
);

-- Index for querying batches by creation time
CREATE INDEX idx_batches_created_at ON batches(created_at);

-- Indexes for querying batches by market maker and for looking up by nonce digest
CREATE INDEX idx_batches_market_maker_created_at ON batches (market_maker_id, created_at DESC);
CREATE INDEX idx_batches_nonce_digest ON batches (batch_nonce_digest);

