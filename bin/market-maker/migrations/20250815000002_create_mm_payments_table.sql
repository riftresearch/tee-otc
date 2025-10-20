-- Track payments executed by the market maker per swap
CREATE TABLE IF NOT EXISTS public.mm_payments (
    swap_id UUID PRIMARY KEY,
    txid TEXT NOT NULL
);
