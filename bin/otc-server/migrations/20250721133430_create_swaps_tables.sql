-- Create swaps table with deterministic wallet salts
CREATE TABLE swaps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quote_id UUID NOT NULL REFERENCES quotes(id),
    market_maker VARCHAR(255) NOT NULL,
    
    -- Salt columns for deterministic wallet generation
    user_deposit_salt BYTEA NOT NULL,
    mm_deposit_salt BYTEA NOT NULL,
    
    -- User addresses
    user_destination_address VARCHAR(255) NOT NULL,
    user_refund_address VARCHAR(255) NOT NULL,
    
    -- Status
    status VARCHAR(50) NOT NULL,
    
    -- Deposit tracking
    user_deposit_tx_hash VARCHAR(255),
    user_deposit_amount TEXT, -- U256 stored as string
    user_deposit_detected_at TIMESTAMPTZ,
    
    mm_deposit_tx_hash VARCHAR(255),
    mm_deposit_amount TEXT, -- U256 stored as string
    mm_deposit_detected_at TIMESTAMPTZ,
    
    -- Withdrawal tracking
    user_withdrawal_tx VARCHAR(255),
    
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);