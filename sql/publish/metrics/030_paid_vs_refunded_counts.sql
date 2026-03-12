-- paid vs refunded counts (conditional aggregation is at order grain)
-- grain is by (country)

SELECT
  fo.country,
  COUNT(*) FILTER (WHERE fo.status = 'paid')     AS paid_orders,
  COUNT(*) FILTER (WHERE fo.status = 'refunded') AS refunded_orders
FROM v_fact_orders_latest fo
GROUP BY 1
ORDER BY paid_orders DESC, fo.country ASC;
