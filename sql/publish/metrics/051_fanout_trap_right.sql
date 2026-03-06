--  FANOUT TRAP RIGHT WAY

-- To touch item-grain data, pre-aggregate it to the order grain FIRST,
-- then join it. Guarantees at most 1 row per `order_id` on the join.


WITH items_by_order AS (
  SELECT 
    order_id, 
    SUM(net_usd) AS net_usd
  FROM v_fact_order_items_latest
  GROUP BY 1
)
SELECT 
  fo.date AS day, 
  SUM(ibo.net_usd) AS paid_revenue_usd_right
FROM v_fact_orders_latest fo
JOIN items_by_order ibo 
  ON ibo.order_id = fo.order_id
WHERE fo.status = 'paid'
GROUP BY 1
ORDER BY 1;



