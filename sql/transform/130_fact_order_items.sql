-- Build `fact_order_items` from stg_order_items  with deterministic upsert semantics.
-- joining orders to attach customer/date (nullable if orphan items exist).

WITH current_run AS (
  SELECT mode
  FROM run_ledger
  WHERE run_id = %(run_id)s
),
staged_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_ts::date AS date
  FROM stg_orders o
  WHERE o.run_id = %(run_id)s
),
staged_items AS (
  SELECT
    i.order_id,
    i.line_id,
    so.customer_id,
    so.date,
    i.product_id,
    i.sku,
    i.qty,
    i.unit_price_usd,
    i.gross_usd,
    i.net_usd,
    i.run_id AS source_run_id
  FROM stg_order_items i
  LEFT JOIN staged_orders so
    ON so.order_id = i.order_id
  WHERE i.run_id = %(run_id)s
),
touched_orders AS (
  SELECT DISTINCT order_id
  FROM staged_items
)
DELETE FROM fact_order_items foi
WHERE
  (
    (SELECT mode FROM current_run) IN ('snapshot', 'live')
    AND NOT EXISTS (
      SELECT 1
      FROM staged_items si
      WHERE si.order_id = foi.order_id
        AND si.line_id = foi.line_id
    )
  )
  OR
  (
    (SELECT mode FROM current_run) = 'incremental'
    AND EXISTS (
      SELECT 1
      FROM touched_orders t
      WHERE t.order_id = foi.order_id
    )
    AND NOT EXISTS (
      SELECT 1
      FROM staged_items si
      WHERE si.order_id = foi.order_id
        AND si.line_id = foi.line_id
    )
  );

WITH staged_orders AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.order_ts::date AS date
  FROM stg_orders o
  WHERE o.run_id = %(run_id)s
),
staged_items AS (
  SELECT
    i.order_id,
    i.line_id,
    so.customer_id,
    so.date,
    i.product_id,
    i.sku,
    i.qty,
    i.unit_price_usd,
    i.gross_usd,
    i.net_usd,
    i.run_id AS source_run_id
  FROM stg_order_items i
  LEFT JOIN staged_orders so
    ON so.order_id = i.order_id
  WHERE i.run_id = %(run_id)s
)
INSERT INTO fact_order_items (
  order_id, line_id, customer_id, date, product_id, sku,
  qty, unit_price_usd, gross_usd, net_usd, source_run_id
)
SELECT
  order_id, line_id, customer_id, date, product_id, sku,
  qty, unit_price_usd, gross_usd, net_usd, source_run_id
FROM staged_items
ON CONFLICT (order_id, line_id) DO UPDATE
SET
  customer_id = EXCLUDED.customer_id,
  date = EXCLUDED.date,
  product_id = EXCLUDED.product_id,
  sku = EXCLUDED.sku,
  qty = EXCLUDED.qty,
  unit_price_usd = EXCLUDED.unit_price_usd,
  gross_usd = EXCLUDED.gross_usd,
  net_usd = EXCLUDED.net_usd,
  source_run_id = EXCLUDED.source_run_id,
  built_at = now();
