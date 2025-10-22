-- Track raw broadcasted transactions for audit and recovery
CREATE TABLE IF NOT EXISTS public.mm_broadcasted_transactions (
    txid TEXT PRIMARY KEY,
    chain TEXT NOT NULL,
    txdata BYTEA NOT NULL,
    bitcoin_tx_foreign_utxos JSONB,
    absolute_fee BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mm_broadcasted_transactions_chain
    ON public.mm_broadcasted_transactions (chain);

