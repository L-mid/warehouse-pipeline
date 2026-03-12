-- Build `fact_order_items` from stg_order_items (from the latest succeeded run)
-- joining orders to attach customer/date (nullable if orphan items exist).


TRUNCATE TABLE fact_order_items;


WITH o AS (      -- orders
  SELECT *
  FROM stg_orders
  WHERE run_id = %(run_id)s  -- for the provided run_id
),

i AS (      -- items
  SELECT *
  FROM stg_order_items
  WHERE run_id = %(run_id)s
)
INSERT INTO fact_order_items (
  order_id, line_id, customer_id, date, product_id, sku,
  qty, unit_price_usd, gross_usd, net_usd, source_run_id
)
SELECT
  i.order_id,
  i.line_id,
  o.customer_id,
  o.order_ts::date AS date,
  i.product_id,
  i.sku,
  i.qty,
  i.unit_price_usd,
  i.gross_usd,
  i.net_usd,
  i.run_id AS source_run_id
FROM i
LEFT JOIN o   -- leave `order_id` null if orphaned
  ON o.order_id = i.order_id;
