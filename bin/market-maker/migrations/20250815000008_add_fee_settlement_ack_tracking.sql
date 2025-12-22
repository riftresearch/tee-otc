-- Track otc-server acknowledgment state for fee settlements and metadata needed for replay.
ALTER TABLE public.mm_fee_settlements
    ADD COLUMN IF NOT EXISTS otc_ack_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (otc_ack_status IN ('pending', 'accepted', 'rejected')),
    ADD COLUMN IF NOT EXISTS otc_ack_reason TEXT,
    ADD COLUMN IF NOT EXISTS otc_acked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS otc_last_request_id UUID,
    ADD COLUMN IF NOT EXISTS otc_last_submitted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS otc_submit_attempts BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_mm_fee_settlements_otc_ack_status
    ON public.mm_fee_settlements (otc_ack_status, otc_last_submitted_at);


