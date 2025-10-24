-- Add associated_swap_id column to mm_deposits table
-- This column can store a reference to the swap that used this deposit
ALTER TABLE public.mm_deposits
ADD COLUMN associated_swap_id UUID;

