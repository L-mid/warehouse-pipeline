-- top products per week (from `fact_order_items + fact_orders)
-- grain is (week_start, sku)
-- paid rule is (status) = 'paid'
-- a note: excludes orphan items by joining to `fact_orders`

WITH product_week AS (
  SELECT
    date_trunc('week', fo.date)::date AS week_start,
    foi.sku,
    SUM(foi.qty)     AS units,
    SUM(foi.net_usd) AS revenue_usd
  FROM v_fact_order_items_latest foi
  JOIN v_fact_orders_latest fo
    ON fo.order_id = foi.order_id
  WHERE LOWER(TRIM(fo.status)) = 'paid'
  GROUP BY 1, 2
)
SELECT *
FROM product_week
ORDER BY week_start ASC, revenue_usd DESC, sku ASC; 