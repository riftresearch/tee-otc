CREATE INDEX IF NOT EXISTS idx_swaps_waiting_user_deposit_watch_set
ON swaps (updated_at ASC, id ASC)
INCLUDE (quote_id, deposit_vault_address, created_at)
WHERE status = 'waiting_user_deposit_initiated';
