TRUNCATE fact_order_items;

INSERT INTO fact_order_items (
  order_id, line_id, customer_id, date,
  sku, qty, unit_price_usd, discount_usd,
  gross_usd, net_usd, source_run_id
)
SELECT
  oi.order_id,
  oi.line_id,
  fo.customer_id,          -- is NULL if there's an orphan `order_items` row (no because matching order)
  fo.date,                 -- NULL if orphan
  oi.sku,
  oi.qty,
  oi.unit_price_usd,
  oi.discount_usd,
  (oi.qty * oi.unit_price_usd)::numeric(12,2) AS gross_usd,
  (oi.qty * oi.unit_price_usd - oi.discount_usd)::numeric(12,2) AS net_usd,
  %(order_items_run_id)s::uuid AS source_run_id
FROM stg_order_items oi
LEFT JOIN fact_orders fo
  ON fo.order_id = oi.order_id
WHERE oi.run_id = %(order_items_run_id)s::uuid;