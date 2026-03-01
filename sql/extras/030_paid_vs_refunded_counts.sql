-- paid vs refunded counts (conditional aggregation is at order grain)
-- grain is by (country)

SELECT
  country,
  COUNT(*) FILTER (WHERE status = 'paid')     AS paid_orders,
  COUNT(*) FILTER (WHERE status = 'refunded') AS refunded_orders,
  COUNT(*)                                    AS total_orders
FROM fact_orders
GROUP BY 1
ORDER BY 1 ASC;