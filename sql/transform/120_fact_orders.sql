-- Builds `fact_orders` from stg_orders with deterministic upsert semantics.


WITH current_run AS (
  SELECT mode
  FROM run_ledger
  WHERE run_id = %(run_id)s
),
staged_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_ts::date AS date,
    o.order_ts,
    o.country,
    o.status,
    o.total_usd,
    o.run_id AS source_run_id
  FROM stg_orders o
  WHERE o.run_id = %(run_id)s
)
DELETE FROM fact_orders fo
WHERE
  (SELECT mode FROM current_run) IN ('snapshot', 'live')
  AND NOT EXISTS (
    SELECT 1
    FROM staged_orders so
    WHERE so.order_id = fo.order_id
  );

WITH staged_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_ts::date AS date,
    o.order_ts,
    o.country,
    o.status,
    o.total_usd,
    o.run_id AS source_run_id
  FROM stg_orders o
  WHERE o.run_id = %(run_id)s
)
INSERT INTO fact_orders (
  order_id, customer_id, date, order_ts, country, status, total_usd, source_run_id
)
SELECT
  order_id, customer_id, date, order_ts, country, status, total_usd, source_run_id
FROM staged_orders
ON CONFLICT (order_id) DO UPDATE
SET
  customer_id = EXCLUDED.customer_id,
  date = EXCLUDED.date,
  order_ts = EXCLUDED.order_ts,
  country = EXCLUDED.country,
  status = EXCLUDED.status,
  total_usd = EXCLUDED.total_usd,
  source_run_id = EXCLUDED.source_run_id,
  built_at = now();
