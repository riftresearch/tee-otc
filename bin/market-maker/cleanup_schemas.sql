-- Safe cleanup script to consolidate mm_deposits data and remove duplicate schemas
-- Run this manually: docker exec -it mm-postgres psql -U user -d mm_db -f /path/to/cleanup_schemas.sql

BEGIN;

-- First, ensure public.mm_deposits exists
CREATE TABLE IF NOT EXISTS public.mm_deposits (
    private_key  TEXT      NOT NULL PRIMARY KEY,
    chain        TEXT      NOT NULL,
    token        JSONB     NOT NULL,
    decimals     SMALLINT  NOT NULL,
    amount       NUMERIC   NOT NULL,
    status       TEXT      NOT NULL DEFAULT 'available',
    reserved_by  UUID,
    reserved_at  TIMESTAMPTZ,
    used_at      TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL,
    funding_tx_hash TEXT NOT NULL
);

-- Copy data from deposit_key_storage.mm_deposits if it exists and has data
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables 
               WHERE table_schema = 'deposit_key_storage' 
               AND table_name = 'mm_deposits') THEN
        
        -- Insert data, skipping duplicates based on primary key
        INSERT INTO public.mm_deposits 
        SELECT * FROM deposit_key_storage.mm_deposits
        ON CONFLICT (private_key) DO NOTHING;
        
        RAISE NOTICE 'Copied data from deposit_key_storage.mm_deposits';
    END IF;
END $$;

-- Copy data from payment_storage.mm_deposits if it exists and has data
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables 
               WHERE table_schema = 'payment_storage' 
               AND table_name = 'mm_deposits') THEN
        
        -- Insert data, skipping duplicates based on primary key
        INSERT INTO public.mm_deposits 
        SELECT * FROM payment_storage.mm_deposits
        ON CONFLICT (private_key) DO NOTHING;
        
        RAISE NOTICE 'Copied data from payment_storage.mm_deposits';
    END IF;
END $$;

-- Copy data from quote_storage.mm_deposits if it exists and has data
DO $$
BEGIN
    IF EXISTS (SELECT FROM information_schema.tables 
               WHERE table_schema = 'quote_storage' 
               AND table_name = 'mm_deposits') THEN
        
        -- Insert data, skipping duplicates based on primary key
        INSERT INTO public.mm_deposits 
        SELECT * FROM quote_storage.mm_deposits
        ON CONFLICT (private_key) DO NOTHING;
        
        RAISE NOTICE 'Copied data from quote_storage.mm_deposits';
    END IF;
END $$;

-- Show row count in public.mm_deposits before cleanup
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count FROM public.mm_deposits;
    RAISE NOTICE 'public.mm_deposits now has % rows', row_count;
END $$;

-- Now drop the duplicate schemas (this will remove all tables in them)
DROP SCHEMA IF EXISTS deposit_key_storage CASCADE;
DROP SCHEMA IF EXISTS payment_storage CASCADE;
DROP SCHEMA IF EXISTS quote_storage CASCADE;

-- Recreate the index
CREATE INDEX IF NOT EXISTS idx_mm_deposits_match
ON public.mm_deposits (status, chain, decimals, created_at, private_key);

-- Show final state
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count FROM public.mm_deposits;
    RAISE NOTICE 'Cleanup complete. public.mm_deposits has % rows', row_count;
END $$;

COMMIT;

-- Verify the result
\dt
SELECT COUNT(*) as mm_deposits_count FROM public.mm_deposits;

