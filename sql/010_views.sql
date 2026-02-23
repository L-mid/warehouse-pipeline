-- Business outputs as SQL views. 
-- each view reports the latest succeeded run for its sourced staging table.
-- grain is by day

-- revenue per day
CREATE OR REPLACE VIEW daily_revenue AS
WITH latest_run AS (
  SELECT run_id
  FROM ingest_runs
  WHERE table_name = 'stg_retail_transactions'
    AND status = 'succeeded'
  ORDER BY started_at DESC
  LIMIT 1
),
paid_rows AS (
  -- only counts rows that actually contributed sales value.
  SELECT t.*
  FROM stg_retail_transactions t
  JOIN latest_run r ON r.run_id = t.run_id
  WHERE t.units_sold > 0
    AND t.revenue > 0
)
SELECT
  date AS day,
  SUM(revenue)    AS paid_revenue_usd,
  SUM(units_sold) AS paid_units_sold,
  COUNT(*)        AS paid_rows
FROM paid_rows
GROUP BY 1
ORDER BY 1;

-- new customers by day
CREATE OR REPLACE VIEW new_customers_by_day AS
WITH latest_run AS (
  SELECT run_id
  FROM ingest_runs
  WHERE table_name = 'stg_customers'
    AND status = 'succeeded'
  ORDER BY started_at DESC
  LIMIT 1
)
SELECT
  c.subscription_date AS day,
  COUNT(*)            AS new_customers
FROM stg_customers c
JOIN latest_run r ON r.run_id = c.run_id
GROUP BY 1
ORDER BY 1;


