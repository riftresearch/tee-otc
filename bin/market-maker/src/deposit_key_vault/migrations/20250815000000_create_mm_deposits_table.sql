-- Create table for market maker deposits
CREATE TABLE IF NOT EXISTS mm_deposits (
    id           BIGSERIAL PRIMARY KEY,
    private_key  TEXT      NOT NULL,
    chain        TEXT      NOT NULL,
    token        JSONB     NOT NULL,
    decimals     SMALLINT  NOT NULL,
    amount       NUMERIC   NOT NULL,
    status       TEXT      NOT NULL DEFAULT 'available',
    reserved_by  UUID,
    reserved_at  TIMESTAMPTZ,
    used_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful index for matching/scanning available deposits
CREATE INDEX IF NOT EXISTS idx_mm_deposits_match
ON mm_deposits (status, chain, decimals, created_at, id);

