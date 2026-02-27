TRUNCATE fact_orders;

INSERT INTO fact_orders (
  order_id, customer_id, date, order_ts, country, status, total_usd, source_run_id
)
SELECT
  order_id,
  customer_id,
  order_ts::date AS date,
  order_ts,
  country,
  status,
  total_usd,
  %(orders_run_id)s::uuid AS source_run_id
FROM stg_orders
WHERE run_id = %(orders_run_id)s::uuid;