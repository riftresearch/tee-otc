-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create swap status enum
CREATE TYPE swap_status AS ENUM (
    'quote_validation',
    'quote_rejected',
    'waiting_user_deposit',
    'waiting_mm_deposit',
    'waiting_confirmations',
    'settling',
    'completed',
    'refunding'
);