--  FANOUT TRAP RIGHT WAY

-- To touch item-grain data, pre-aggregate it to the order grain FIRST,
-- then join it. Guarantees at most 1 row per `order_id` on the join.

-- (Other fixes also exist: summing item-level measures directly from
-- `fact_order_items`, or not joining at all and using the `fact_orders` table.)

WITH items_at_order_grain AS (
  SELECT
    order_id
  FROM fact_order_items
  GROUP BY 1
),
paid_orders AS (
  SELECT
    fo.order_id,
    fo.total_usd
  FROM fact_orders fo
  JOIN items_at_order_grain i
    ON i.order_id = fo.order_id
  WHERE fo.status = 'paid'
)
SELECT
  SUM(total_usd) AS paid_revenue_usd
FROM paid_orders;


