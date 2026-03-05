-- Builds fact_orders from stg_orders (from the latest succeeded run)
CREATE TABLE IF NOT EXISTS fact_orders (
  order_id      bigint PRIMARY KEY,           -- fact_orders grain is 1 row per order
  customer_id   bigint NOT NULL,
  date          date,
  order_ts      timestamptz,
  country       text,
  status        text,
  total_usd     numeric(12,2),
  source_run_id uuid NOT NULL,
  built_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_orders_customer_idx
  ON fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS fact_orders_date_idx
  ON fact_orders (date);


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
WHERE o.run_id = (SELECT run_id FROM latest_run);