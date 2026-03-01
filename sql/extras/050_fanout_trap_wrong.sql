-- THIS FANOUT TRAP IS WRONG

-- `fact_orders` is on the order grain (1 row per order_id).
-- `fact_order_items` is item grain (many rows per order_id).
-- AND this query JOINs order to items and then SUMs an order-level measure.
-- each order's `total_usd` repeats once per item row.
-- So revenue inflates (this is the 'fanout trap').

SELECT
  SUM(fo.total_usd) AS paid_revenue_usd_wrong
FROM fact_orders fo
JOIN fact_order_items foi
  ON foi.order_id = fo.order_id
WHERE fo.status = 'paid';