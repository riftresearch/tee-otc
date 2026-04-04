CREATE TABLE IF NOT EXISTS public.mm_rebalances (
    id UUID PRIMARY KEY,
    src_asset TEXT NOT NULL,
    dst_asset TEXT NOT NULL,
    amount_sats BIGINT NOT NULL,
    evm_chain TEXT NOT NULL,
    state TEXT NOT NULL,
    recipient_address TEXT NOT NULL,
    source_confirmations_required INTEGER NOT NULL,
    source_tx_hash TEXT,
    withdrawal_id TEXT,
    completion_tx_hash TEXT,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT mm_rebalances_src_asset_check
        CHECK (src_asset IN ('asset_a', 'asset_b')),
    CONSTRAINT mm_rebalances_dst_asset_check
        CHECK (dst_asset IN ('asset_a', 'asset_b')),
    CONSTRAINT mm_rebalances_direction_check
        CHECK (src_asset <> dst_asset),
    CONSTRAINT mm_rebalances_amount_check
        CHECK (amount_sats > 0),
    CONSTRAINT mm_rebalances_state_check
        CHECK (
            state IN (
                'pending_source_transfer',
                'waiting_source_confirmations',
                'pending_withdrawal_submission',
                'waiting_withdrawal_completion',
                'completed',
                'failed'
            )
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mm_rebalances_single_active
    ON public.mm_rebalances ((TRUE))
    WHERE state NOT IN ('completed', 'failed');

CREATE INDEX IF NOT EXISTS idx_mm_rebalances_state
    ON public.mm_rebalances (state, created_at);
