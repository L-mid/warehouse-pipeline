-- Builds `fact_orders` from stg_orders (from the latest succeeded run)


TRUNCATE TABLE fact_orders;

WITH latest_run AS (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1
)
INSERT INTO fact_orders (
  order_id, customer_id, date, order_ts, country, status, total_usd, source_run_id
)
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
WHERE o.run_id = %(orders_run_id)s;   -- for the provided run_id