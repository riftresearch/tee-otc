-- Track batch payments executed by the market maker
CREATE TABLE IF NOT EXISTS public.mm_batches (
    txid TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    swap_ids UUID[] NOT NULL,
    batch_nonce_digest BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mm_batches_created_at
    ON public.mm_batches (created_at);
