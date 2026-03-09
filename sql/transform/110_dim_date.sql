
-- dim_date
-- Builds a calendar table spanning the order dates for the latest run.

TRUNCATE TABLE dim_date;


WITH bounds AS (
  SELECT
    COALESCE(min(order_ts)::date, current_date) AS min_d,
    COALESCE(max(order_ts)::date, current_date) AS max_d
  FROM stg_orders
  WHERE run_id = %(run_id)s  -- for the provided run_id
),
days AS (
  SELECT generate_series((SELECT min_d FROM bounds),    -- generated
                         (SELECT max_d FROM bounds),
                         interval '1 day')::date AS d
)
INSERT INTO dim_date (
  date, year, month, day, iso_week, iso_dow, quarter, week_start, month_start
)
SELECT
  d AS date,
  EXTRACT(isoyear FROM d)::int AS iso_year,
  EXTRACT(month FROM d)::int AS month,
  EXTRACT(day FROM d)::int AS day,
  EXTRACT(week FROM d)::int AS iso_week,
  EXTRACT(isodow FROM d)::int AS iso_dow,
  EXTRACT(quarter FROM d)::int AS quarter,
  DATE_TRUNC('week', d)::date AS week_start,
  DATE_TRUNC('month', d)::date AS month_start
FROM days; 


