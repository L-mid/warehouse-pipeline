
-- dim_date
-- Builds a calendar table spanning the order dates. append only.

WITH bounds AS (
  SELECT
    min(order_ts)::date AS min_d,
    max(order_ts)::date AS max_d
  FROM stg_orders
  WHERE run_id = %(run_id)s  -- for the provided run_id
),
days AS (
  SELECT generate_series((SELECT min_d FROM bounds),    -- generated
                         (SELECT max_d FROM bounds),
                         interval '1 day')::date AS d
  WHERE (SELECT min_d FROM bounds) IS NOT NULL
)
INSERT INTO dim_date (
  date, year, month, day, iso_week, iso_dow, quarter, week_start, month_start
)
SELECT
  d AS date,
  EXTRACT(isoyear FROM d)::int AS year,
  EXTRACT(month FROM d)::int AS month,
  EXTRACT(day FROM d)::int AS day,
  EXTRACT(week FROM d)::int AS iso_week,
  EXTRACT(isodow FROM d)::int AS iso_dow,
  EXTRACT(quarter FROM d)::int AS quarter,
  DATE_TRUNC('week', d)::date AS week_start,
  DATE_TRUNC('month', d)::date AS month_start
FROM days
ON CONFLICT (date) DO NOTHING;
