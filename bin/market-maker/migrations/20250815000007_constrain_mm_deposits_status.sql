-- Add CHECK constraint to ensure status is one of ('available', 'reserved')
ALTER TABLE public.mm_deposits
ADD CONSTRAINT mm_deposits_status_check
CHECK (status IN ('available', 'reserved'));

