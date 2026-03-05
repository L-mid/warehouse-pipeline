
-- dim_date
-- Builds a calendar table spanning the order dates for the latest run.

CREATE TABLE IF NOT EXISTS dim_date (
  date        date PRIMARY KEY,
  year        int NOT NULL,
  month       int NOT NULL,
  day         int NOT NULL,
  iso_week    int NOT NULL,
  iso_dow     int NOT NULL,   -- 1=Mon..7=Sun
  quarter     int NOT NULL,
  week_start  date NOT NULL,
  month_start date NOT NULL,
  built_at    timestamptz NOT NULL DEFAULT now()
);


TRUNCATE TABLE dim_date;


WITH latest_run AS (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1
),
bounds AS (
  SELECT
    COALESCE(min(order_ts)::date, current_date) AS min_d,
    COALESCE(max(order_ts)::date, current_date) AS max_d
  FROM stg_orders
  WHERE run_id = (SELECT run_id FROM latest_run)
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
  extract(year from d)::int AS year,
  extract(month from d)::int AS month,
  extract(day from d)::int AS day,
  extract(isoweek from d)::int AS iso_week,
  extract(isodow from d)::int AS iso_dow,
  extract(quarter from d)::int AS quarter,
  date_trunc('week', d)::date AS week_start,
  date_trunc('month', d)::date AS month_start
FROM days; 


