TRUNCATE dim_date;

INSERT INTO dim_date (date, year, month, day, iso_week, iso_dow, quarter)
SELECT
  d::date AS date,
  EXTRACT(YEAR FROM d)::int,
  EXTRACT(MONTH FROM d)::int,
  EXTRACT(DAY FROM d)::int,
  EXTRACT(WEEK FROM d)::int,
  EXTRACT(ISODOW FROM d)::int,
  EXTRACT(QUARTER FROM d)::int
FROM (
  SELECT DISTINCT (order_ts::date) AS d
  FROM stg_orders
  WHERE run_id = %(orders_run_id)s::uuid
) x
ORDER BY date;