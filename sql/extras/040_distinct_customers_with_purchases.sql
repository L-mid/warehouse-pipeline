-- distinct customers with purchases (correct distinct)
-- grain is 1 row
-- paid rule is status = 'paid'

-- Correct because `fact_orders` is already at order grain (1 row per order_id), so
-- COUNT(DISTINCT customer_id) does not get inflated by joins.

SELECT
  COUNT(DISTINCT fo.customer_id) AS paid_customers
FROM fact_orders fo
WHERE fo.status = 'paid';