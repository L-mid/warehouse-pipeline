-- paid vs refunded counts (conditional aggregation is at order grain)
-- grain is by (country)

SELECT
  upper(trim(country)) AS country,
  count(*) FILTER (WHERE lower(trim(status)) = 'paid')     AS paid_orders,
  count(*) FILTER (WHERE lower(trim(status)) = 'refunded') AS refunded_orders,
  count(*)                                                   AS total_orders
FROM fact_orders
GROUP BY 1
ORDER BY 1 ASC;