-- THIS FANOUT TRAP IS WRONG

-- `fact_orders` is on the order grain (1 row per order_id).
-- `fact_order_items` is item grain (many rows per order_id).
-- AND this query JOINs order to items and then SUMs an order-level measure.
-- each order's `total_usd` repeats once per item row.
-- So revenue inflates (this is the 'fanout trap').

SELECT
  fo.date AS day,
  SUM(fo.total_usd) AS paid_revenue_usd_wrong
FROM v_fact_orders_latest fo
JOIN v_fact_order_items_latest foi
  ON foi.order_id = fo.order_id
WHERE fo.status = 'paid'
GROUP BY 1
ORDER BY 1;


