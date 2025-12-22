-- Add per-market settled volume and protocol fee tracking tables
-- with automatic maintenance via statement-level triggers

BEGIN;

-- === Per-market settled volume and protocol-fee totals ======================

CREATE TABLE IF NOT EXISTS settled_volume_totals_market (
  market text PRIMARY KEY,
  total bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS settled_protocol_fee_totals_market (
  market text PRIMARY KEY,
  total bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- === Trigger function to maintain aggregates ================================
-- Swaps are always inserted as 'waiting_user_deposit_initiated', never 'settled'.
-- Volume tracking only happens on UPDATE when status transitions to/from 'settled'.
-- We don't track DELETEs since swaps are never deleted in production.

CREATE OR REPLACE FUNCTION agg_settled_totals_by_market()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Volume is tracked from realized_swap.mm_output (amount sent to user)
  -- Protocol fee is tracked from realized_swap.protocol_fee
  WITH
  new_settled AS (
    SELECT
      (
        CASE WHEN (q.from_token->>'type') = 'Native'
             THEN q.from_chain||':Native'
             ELSE q.from_chain||':'||(q.from_token->>'data') END
      )
      || '-' ||
      (
        CASE WHEN (q.to_token->>'type') = 'Native'
             THEN q.to_chain||':Native'
             ELSE q.to_chain||':'||(q.to_token->>'data') END
      ) AS market,
      SUM((s.realized_swap->>'mm_output')::numeric::bigint) AS volume,
      SUM(COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0)) AS protocol_fee
    FROM new_swaps s
    JOIN quotes q ON q.id = s.quote_id
    WHERE s.status = 'settled' AND s.realized_swap IS NOT NULL
    GROUP BY 1
  ),
  old_settled AS (
    SELECT
      (
        CASE WHEN (q.from_token->>'type') = 'Native'
             THEN q.from_chain||':Native'
             ELSE q.from_chain||':'||(q.from_token->>'data') END
      )
      || '-' ||
      (
        CASE WHEN (q.to_token->>'type') = 'Native'
             THEN q.to_chain||':Native'
             ELSE q.to_chain||':'||(q.to_token->>'data') END
      ) AS market,
      SUM((s.realized_swap->>'mm_output')::numeric::bigint) AS volume,
      SUM(COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0)) AS protocol_fee
    FROM old_swaps s
    JOIN quotes q ON q.id = s.quote_id
    WHERE s.status = 'settled' AND s.realized_swap IS NOT NULL
    GROUP BY 1
  ),
  deltas AS (
    SELECT
      COALESCE(n.market, o.market) AS market,
      COALESCE(n.volume, 0) - COALESCE(o.volume, 0) AS d_volume,
      COALESCE(n.protocol_fee, 0) - COALESCE(o.protocol_fee, 0) AS d_fee
    FROM new_settled n
    FULL OUTER JOIN old_settled o ON o.market = n.market
  )
  INSERT INTO settled_volume_totals_market(market, total, updated_at)
  SELECT market, d_volume, now()
  FROM deltas
  WHERE d_volume <> 0
  ON CONFLICT (market) DO UPDATE
    SET total = settled_volume_totals_market.total + EXCLUDED.total,
        updated_at = EXCLUDED.updated_at;

  WITH
  new_settled AS (
    SELECT
      (
        CASE WHEN (q.from_token->>'type') = 'Native'
             THEN q.from_chain||':Native'
             ELSE q.from_chain||':'||(q.from_token->>'data') END
      )
      || '-' ||
      (
        CASE WHEN (q.to_token->>'type') = 'Native'
             THEN q.to_chain||':Native'
             ELSE q.to_chain||':'||(q.to_token->>'data') END
      ) AS market,
      SUM((s.realized_swap->>'mm_output')::numeric::bigint) AS volume,
      SUM(COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0)) AS protocol_fee
    FROM new_swaps s
    JOIN quotes q ON q.id = s.quote_id
    WHERE s.status = 'settled' AND s.realized_swap IS NOT NULL
    GROUP BY 1
  ),
  old_settled AS (
    SELECT
      (
        CASE WHEN (q.from_token->>'type') = 'Native'
             THEN q.from_chain||':Native'
             ELSE q.from_chain||':'||(q.from_token->>'data') END
      )
      || '-' ||
      (
        CASE WHEN (q.to_token->>'type') = 'Native'
             THEN q.to_chain||':Native'
             ELSE q.to_chain||':'||(q.to_token->>'data') END
      ) AS market,
      SUM((s.realized_swap->>'mm_output')::numeric::bigint) AS volume,
      SUM(COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0)) AS protocol_fee
    FROM old_swaps s
    JOIN quotes q ON q.id = s.quote_id
    WHERE s.status = 'settled' AND s.realized_swap IS NOT NULL
    GROUP BY 1
  ),
  deltas AS (
    SELECT
      COALESCE(n.market, o.market) AS market,
      COALESCE(n.volume, 0) - COALESCE(o.volume, 0) AS d_volume,
      COALESCE(n.protocol_fee, 0) - COALESCE(o.protocol_fee, 0) AS d_fee
    FROM new_settled n
    FULL OUTER JOIN old_settled o ON o.market = n.market
  )
  INSERT INTO settled_protocol_fee_totals_market(market, total, updated_at)
  SELECT market, d_fee, now()
  FROM deltas
  WHERE d_fee <> 0
  ON CONFLICT (market) DO UPDATE
    SET total = settled_protocol_fee_totals_market.total + EXCLUDED.total,
        updated_at = EXCLUDED.updated_at;

  RETURN NULL;
END;
$$;

-- === Attach trigger to swaps table ==========================================

DROP TRIGGER IF EXISTS swaps_settled_totals_by_market ON swaps;
CREATE TRIGGER swaps_settled_totals_by_market
AFTER UPDATE ON swaps
REFERENCING NEW TABLE AS new_swaps OLD TABLE AS old_swaps
FOR EACH STATEMENT
EXECUTE FUNCTION agg_settled_totals_by_market();

-- === Performance optimization: partial index for settled swaps ==============

CREATE INDEX IF NOT EXISTS idx_swaps_settled_quote_id 
ON swaps(quote_id) WHERE status = 'settled';

-- === Backfill current totals per market =====================================
-- Materialize once, reuse for both inserts (more efficient than duplicated CTEs)
-- Volume is from realized_swap.mm_output, protocol fee from realized_swap.protocol_fee

CREATE TEMP TABLE _current_market_totals ON COMMIT DROP AS
SELECT
  (
    CASE WHEN (q.from_token->>'type') = 'Native'
         THEN q.from_chain||':Native'
         ELSE q.from_chain||':'||(q.from_token->>'data') END
  )
  || '-' ||
  (
    CASE WHEN (q.to_token->>'type') = 'Native'
         THEN q.to_chain||':Native'
         ELSE q.to_chain||':'||(q.to_token->>'data') END
  ) AS market,
  SUM((s.realized_swap->>'mm_output')::numeric::bigint) AS volume,
  SUM(COALESCE((s.realized_swap->>'protocol_fee')::numeric::bigint, 0)) AS protocol_fee
FROM swaps s
JOIN quotes q ON q.id = s.quote_id
WHERE s.status = 'settled' AND s.realized_swap IS NOT NULL
GROUP BY 1;

INSERT INTO settled_volume_totals_market(market, total, updated_at)
SELECT market, volume, now() FROM _current_market_totals
ON CONFLICT (market) DO UPDATE
SET total = EXCLUDED.total, updated_at = EXCLUDED.updated_at;

INSERT INTO settled_protocol_fee_totals_market(market, total, updated_at)
SELECT market, protocol_fee, now() FROM _current_market_totals
ON CONFLICT (market) DO UPDATE
SET total = EXCLUDED.total, updated_at = EXCLUDED.updated_at;

COMMIT;

-- === Query examples for monitoring ===========================================
-- SELECT market, total FROM settled_volume_totals_market ORDER BY total DESC;
-- SELECT market, total FROM settled_protocol_fee_totals_market ORDER BY total DESC;

