-- TEE-OTC Initial Database Schema
-- This single migration creates the entire database schema from scratch

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create swap status enum with all states
CREATE TYPE swap_status AS ENUM (
    'waiting_user_deposit_initiated',
    'waiting_user_deposit_confirmed',
    'waiting_mm_deposit_initiated',
    'waiting_mm_deposit_confirmed',
    'settled',
    'refunding_user',
    'refunding_mm',
    'failed'
);

-- Create quotes table with rate-based pricing
CREATE TABLE quotes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- From currency details (what user sends)
    from_chain VARCHAR(50) NOT NULL,
    from_token JSONB NOT NULL,
    from_decimals SMALLINT NOT NULL,

    -- To currency details (what user receives)
    to_chain VARCHAR(50) NOT NULL,
    to_token JSONB NOT NULL,
    to_decimals SMALLINT NOT NULL,

    -- Rate parameters (basis points for spreads, sats for network fee)
    liquidity_fee_bps BIGINT NOT NULL,
    protocol_fee_bps BIGINT NOT NULL,
    network_fee_sats BIGINT NOT NULL,

    -- Input bounds (U256 stored as string)
    min_input TEXT NOT NULL,
    max_input TEXT NOT NULL,

    market_maker_id UUID NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL 
);

-- Create swaps table with enhanced state tracking
CREATE TABLE swaps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quote_id UUID NOT NULL REFERENCES quotes(id),
    market_maker_id UUID NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    
    -- Realized swap amounts (computed when user deposit is detected)
    realized_swap JSONB,
    
    -- Salt and nonce columns for deterministic wallet generation
    deposit_vault_salt BYTEA NOT NULL,
    deposit_vault_address VARCHAR(255) NOT NULL,
    mm_nonce BYTEA NOT NULL,
    
    -- User addresses
    user_destination_address VARCHAR(255) NOT NULL,
    refund_address VARCHAR(255) NOT NULL,

    -- Core status using enum
    status swap_status NOT NULL DEFAULT 'waiting_user_deposit_initiated',

    -- Deposit tracking (JSONB for rich data)
    user_deposit_status JSONB,
    mm_deposit_status JSONB,

    -- Settlement tracking
    settlement_status JSONB,

    -- Refund tracking
    latest_refund JSONB,
    
    -- Failure tracking
    failure_reason TEXT,
    failure_at TIMESTAMPTZ,
    
    -- MM coordination
    mm_notified_at TIMESTAMPTZ,
    mm_private_key_sent_at TIMESTAMPTZ,
    
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL 
);

-- Create indexes for efficient queries
CREATE INDEX idx_quotes_market_maker ON quotes(market_maker_id);
CREATE INDEX idx_quotes_expires_at ON quotes(expires_at);

CREATE INDEX idx_swaps_quote_id ON swaps(quote_id);
CREATE INDEX idx_swaps_market_maker ON swaps(market_maker_id);
CREATE INDEX idx_swaps_status ON swaps(status);

-- Indexes for monitoring active swaps
CREATE INDEX idx_swaps_active ON swaps(status) 
WHERE status NOT IN ('settled', 'failed');

CREATE INDEX idx_swaps_failure ON swaps(failure_at)
WHERE failure_at IS NOT NULL;

-- Combined index for market maker queries
CREATE INDEX idx_swaps_market_maker_active ON swaps(market_maker_id, status)
WHERE status NOT IN ('settled', 'failed');

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_swaps_updated_at BEFORE UPDATE ON swaps
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

-- =============================================================================
-- Protocol fee debt + settlement tables (no legacy direct-pay mode)
-- =============================================================================

-- Append-only ledger of debt changes (accruals and payments)
CREATE TABLE mm_protocol_fee_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    market_maker_id UUID NOT NULL,
    delta_sats BIGINT NOT NULL,
    kind TEXT NOT NULL,
    ref_chain TEXT,
    ref_tx_hash TEXT,
    batch_nonce_digest BYTEA,
    settlement_digest BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (delta_sats <> 0)
);

-- Idempotency constraints
CREATE UNIQUE INDEX mm_protocol_fee_ledger_unique_accrual
    ON mm_protocol_fee_ledger (kind, market_maker_id, batch_nonce_digest)
    WHERE kind = 'batch_accrual';

CREATE UNIQUE INDEX mm_protocol_fee_ledger_unique_settlement
    ON mm_protocol_fee_ledger (kind, market_maker_id, ref_chain, ref_tx_hash)
    WHERE kind = 'settlement_payment';

CREATE INDEX mm_protocol_fee_ledger_mm_created_at
    ON mm_protocol_fee_ledger (market_maker_id, created_at DESC);

-- Fee settlements (one per on-chain settlement tx), storing referenced batches directly (Option 1)
CREATE TABLE mm_fee_settlements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    market_maker_id UUID NOT NULL,
    chain TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    settlement_digest BYTEA NOT NULL,
    amount_sats BIGINT NOT NULL,
    batch_nonce_digests BYTEA[] NOT NULL,
    referenced_fee_sats BIGINT NOT NULL,
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (amount_sats >= 0),
    CHECK (referenced_fee_sats >= 0)
);

CREATE UNIQUE INDEX mm_fee_settlements_unique_tx
    ON mm_fee_settlements (chain, tx_hash);

CREATE UNIQUE INDEX mm_fee_settlements_unique_digest
    ON mm_fee_settlements (market_maker_id, settlement_digest);

-- Optional: membership queries on batch_nonce_digests
CREATE INDEX mm_fee_settlements_batch_nonce_digests_gin
    ON mm_fee_settlements
    USING GIN (batch_nonce_digests);

-- Cached per-MM fee standing state for fast routing checks
-- `debt_sats` may go negative to represent pre-paid credit from over-settlements.
-- Good-standing checks treat `debt_sats <= threshold` as compliant.
CREATE TABLE mm_fee_state (
    market_maker_id UUID PRIMARY KEY,
    debt_sats BIGINT NOT NULL DEFAULT 0,
    over_threshold_since TIMESTAMPTZ,
    last_payment_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
