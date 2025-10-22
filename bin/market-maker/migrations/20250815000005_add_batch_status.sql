-- Add status column to mm_batches table
ALTER TABLE public.mm_batches
ADD COLUMN status TEXT NOT NULL DEFAULT 'created';

-- Add constraint to ensure valid status values
ALTER TABLE public.mm_batches
ADD CONSTRAINT mm_batches_status_check CHECK (status IN ('created', 'confirmed', 'cancelled'));

-- Add index on status for efficient querying
CREATE INDEX IF NOT EXISTS idx_mm_batches_status ON public.mm_batches (status);

