-- Add market_maker_id to batches table for efficient querying
-- Backfill from existing swap data

BEGIN;

-- Step 1: Add column as nullable
ALTER TABLE batches ADD COLUMN IF NOT EXISTS market_maker_id UUID;

-- Step 2: Backfill from the first swap id in each batch
-- Only for rows with a non-empty swap_ids array and null market_maker_id
-- Use ->> to get text (not JSONB) for proper UUID casting
UPDATE batches b
SET market_maker_id = s.market_maker_id
FROM swaps s
WHERE b.market_maker_id IS NULL
  AND jsonb_typeof(b.swap_ids) = 'array'
  AND jsonb_array_length(b.swap_ids) > 0
  AND s.id = (b.swap_ids->>0)::uuid;

-- Step 3: Sanity check (optional - uncomment to verify before proceeding)
-- DO $$
-- DECLARE
--   null_count integer;
-- BEGIN
--   SELECT COUNT(*) INTO null_count FROM batches WHERE market_maker_id IS NULL;
--   IF null_count > 0 THEN
--     RAISE EXCEPTION 'Found % batches with NULL market_maker_id - fix data before proceeding', null_count;
--   END IF;
-- END $$;

-- Step 4: Make the column NOT NULL now that it's backfilled
-- Assuming invariant holds: all batches have at least one swap
ALTER TABLE batches ALTER COLUMN market_maker_id SET NOT NULL;

COMMIT;

-- Step 5: Add index for efficient queries by market maker
-- Note: Use CREATE INDEX CONCURRENTLY on production systems with large tables
-- (CONCURRENTLY cannot run inside a transaction block)
CREATE INDEX IF NOT EXISTS idx_batches_market_maker_created_at 
  ON batches (market_maker_id, created_at DESC);

