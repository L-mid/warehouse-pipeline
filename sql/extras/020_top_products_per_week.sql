-- top products per week (from `fact_order_items + fact_orders)
-- grain is (week_start, sku)
-- paid rule is (status) = 'paid'
-- a note: excludes orphan items by joining to `fact_orders`

WITH product_week AS (
  SELECT
    date_trunc('week', fo.date)::date AS week_start,
    foi.sku,
    SUM(foi.qty) AS units,
    SUM(foi.net_usd) AS revenue_usd
  FROM fact_order_items foi
  JOIN fact_orders fo
    ON fo.order_id = foi.order_id
  WHERE fo.status = 'paid'
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY week_start
      ORDER BY revenue_usd DESC, sku ASC
    ) AS rn
  FROM product_week
)
SELECT
  week_start,
  sku,
  units,
  revenue_usd
FROM ranked
WHERE rn <= 3
ORDER BY week_start ASC, rn ASC;