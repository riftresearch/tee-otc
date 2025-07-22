-- Create quotes table
CREATE TABLE quotes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- From currency details
    from_chain VARCHAR(50) NOT NULL,
    from_token JSONB NOT NULL,
    from_amount TEXT NOT NULL, -- U256 stored as string
    from_decimals SMALLINT NOT NULL,
    
    -- To currency details
    to_chain VARCHAR(50) NOT NULL,
    to_token JSONB NOT NULL,
    to_amount TEXT NOT NULL, -- U256 stored as string
    to_decimals SMALLINT NOT NULL,
    
    market_maker_identifier VARCHAR(255) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);