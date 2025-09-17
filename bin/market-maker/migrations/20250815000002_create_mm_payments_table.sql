-- Ensure the dedicated schema exists so objects land in payment_storage
CREATE SCHEMA IF NOT EXISTS payment_storage;

-- Track payments executed by the market maker per swap
CREATE TABLE IF NOT EXISTS mm_payments (
    swap_id UUID PRIMARY KEY,
    txid TEXT NOT NULL
);
