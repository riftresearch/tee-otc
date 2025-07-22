-- Create indexes for better query performance
CREATE INDEX idx_quotes_market_maker ON quotes(market_maker_identifier);
CREATE INDEX idx_quotes_expires_at ON quotes(expires_at);
CREATE INDEX idx_swaps_status ON swaps(status);
CREATE INDEX idx_swaps_quote_id ON swaps(quote_id);
CREATE INDEX idx_swaps_market_maker ON swaps(market_maker);