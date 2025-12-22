-- Market Maker Quotes Table
-- This migration creates the quotes table for the market maker to store locally generated quotes

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create market maker quotes table in the public schema
CREATE TABLE IF NOT EXISTS public.mm_quotes (
    id UUID PRIMARY KEY,
    
    -- The market maker that created the quote
    market_maker_id UUID NOT NULL,
    
    -- From lot (what user sends: currency + exact quoted amount)
    from_chain VARCHAR(50) NOT NULL,
    from_token JSONB NOT NULL,
    from_decimals SMALLINT NOT NULL,
    from_amount TEXT NOT NULL,
    
    -- To lot (what user receives: currency + exact quoted amount)
    to_chain VARCHAR(50) NOT NULL,
    to_token JSONB NOT NULL,
    to_decimals SMALLINT NOT NULL,
    to_amount TEXT NOT NULL,

    -- Input bounds (U256 stored as string)
    -- User can deposit any amount within these bounds
    min_input TEXT NOT NULL,
    max_input TEXT NOT NULL,

    -- Optional affiliate identifier for custom protocol fee rates
    affiliate TEXT,

    -- Rate parameters (SwapRates as JSONB)
    -- Used to compute realized amounts if deposit differs from quoted amount
    rates JSONB NOT NULL,

    -- Fee breakdown for the exact quoted amount (as JSONB)
    fees JSONB NOT NULL,

    -- Timestamps
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    
    -- Metadata for tracking
    sent_to_rfq BOOLEAN NOT NULL DEFAULT FALSE,
    sent_to_otc BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_mm_quotes_market_maker ON public.mm_quotes(market_maker_id);
CREATE INDEX IF NOT EXISTS idx_mm_quotes_expires_at ON public.mm_quotes(expires_at);
CREATE INDEX IF NOT EXISTS idx_mm_quotes_created_at ON public.mm_quotes(created_at DESC);

-- Index for finding unsent quotes
CREATE INDEX IF NOT EXISTS idx_mm_quotes_unsent ON public.mm_quotes(sent_to_rfq, sent_to_otc)
WHERE sent_to_rfq = FALSE OR sent_to_otc = FALSE;
