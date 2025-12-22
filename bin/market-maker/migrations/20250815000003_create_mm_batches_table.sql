-- Track batch payments executed by the market maker
CREATE TABLE IF NOT EXISTS public.mm_batches (
    txid TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    swap_ids UUID[] NOT NULL,
    batch_nonce_digest BYTEA NOT NULL,
    aggregated_fee_sats BIGINT NOT NULL DEFAULT 0,
    fee_settlement_txid TEXT,
    status TEXT NOT NULL DEFAULT 'created' CHECK (status IN ('created', 'confirmed', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mm_batches_created_at
    ON public.mm_batches (created_at);

CREATE INDEX IF NOT EXISTS idx_mm_batches_status
    ON public.mm_batches (status);

CREATE INDEX IF NOT EXISTS idx_mm_batches_confirmed_unsettled
    ON public.mm_batches (created_at)
    WHERE status = 'confirmed' AND fee_settlement_txid IS NULL;

-- Fee settlement attempts created by the market maker.
CREATE TABLE IF NOT EXISTS public.mm_fee_settlements (
    txid TEXT PRIMARY KEY,
    rail TEXT NOT NULL CHECK (rail IN ('evm', 'bitcoin')),
    evm_chain TEXT,
    settlement_digest BYTEA NOT NULL,
    batch_nonce_digests BYTEA[] NOT NULL,
    referenced_fee_sats BIGINT NOT NULL,
    amount_sats BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'broadcast', 'confirmed', 'submitted', 'failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mm_fee_settlements_created_at
    ON public.mm_fee_settlements (created_at);
