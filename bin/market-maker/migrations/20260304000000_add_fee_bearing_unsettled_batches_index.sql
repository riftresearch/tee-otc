CREATE INDEX IF NOT EXISTS idx_mm_batches_confirmed_unsettled_fee_bearing
    ON public.mm_batches (created_at)
    WHERE status = 'confirmed'
      AND fee_settlement_txid IS NULL
      AND aggregated_fee_sats > 0;
