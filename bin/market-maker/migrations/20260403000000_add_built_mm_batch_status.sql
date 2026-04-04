ALTER TABLE public.mm_batches
DROP CONSTRAINT IF EXISTS mm_batches_status_check;

ALTER TABLE public.mm_batches
ADD CONSTRAINT mm_batches_status_check
CHECK (status IN ('built', 'created', 'confirmed', 'cancelled'));
