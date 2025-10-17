-- Create batches table to store market maker batch payments
-- This table tracks batch payments by chain and transaction hash

CREATE TABLE batches (
    chain VARCHAR(50) NOT NULL,
    tx_hash VARCHAR(255) NOT NULL,
    full_batch JSONB NOT NULL,
    swap_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain, tx_hash)
);

-- Index for querying batches by creation time
CREATE INDEX idx_batches_created_at ON batches(created_at);

