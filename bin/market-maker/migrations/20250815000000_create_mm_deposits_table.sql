-- Create table for market maker deposits in the public schema
CREATE TABLE IF NOT EXISTS public.mm_deposits (
    private_key  TEXT      NOT NULL PRIMARY KEY,
    chain        TEXT      NOT NULL,
    token        JSONB     NOT NULL,
    decimals     SMALLINT  NOT NULL,
    amount       NUMERIC   NOT NULL,
    status       TEXT      NOT NULL DEFAULT 'available',
    reserved_by  UUID,
    reserved_at  TIMESTAMPTZ,
    used_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL,
    funding_tx_hash TEXT NOT NULL
);

-- Helpful index for matching/scanning available deposits
CREATE INDEX IF NOT EXISTS idx_mm_deposits_match
ON public.mm_deposits (status, chain, decimals, created_at, private_key);
