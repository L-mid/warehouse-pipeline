-- distinct customers with purchases (correct distinct)
-- grain is 1 row
-- paid rule is status = 'paid'

-- Correct because `v_fact_orders_latest` is already at order grain (1 row per order_id), so
-- COUNT(DISTINCT customer_id) does not get inflated by joins.

SELECT
  fo.country,
  COUNT(DISTINCT fo.customer_id) FILTER (WHERE LOWER(TRIM(fo.status)) = 'paid') AS distinct_paid_customers
FROM v_fact_orders_latest fo
GROUP BY 1
ORDER BY distinct_paid_customers DESC, fo.country ASC;

