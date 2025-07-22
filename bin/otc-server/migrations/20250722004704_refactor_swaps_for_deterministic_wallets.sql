-- Drop the swap_secrets table (no longer needed)
DROP TABLE IF EXISTS swap_secrets;

-- Add salt columns for deterministic wallet generation
-- NOTE: Since we can't use gen_random_bytes without pgcrypto, we'll add without defaults
-- The application will generate salts when creating swaps
ALTER TABLE swaps
    ADD COLUMN user_deposit_salt BYTEA NOT NULL,
    ADD COLUMN mm_deposit_salt BYTEA NOT NULL;

-- Remove wallet address columns (will be derived from salts)
ALTER TABLE swaps
    DROP COLUMN user_deposit_chain,
    DROP COLUMN user_deposit_address,
    DROP COLUMN mm_deposit_chain,
    DROP COLUMN mm_deposit_address;