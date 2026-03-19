SELECT
    business_date,
    COALESCE(order_state, 'UNKNOWN') AS order_state,
    COUNT(*) AS order_count,
    COALESCE(SUM(total_money), 0)::numeric(12,2) AS total_money,
    COALESCE(SUM(net_total_money), 0)::numeric(12,2) AS net_total_money
FROM v_fact_orders_current
WHERE business_date IS NOT NULL
GROUP BY business_date, COALESCE(order_state, 'UNKNOWN')
ORDER BY business_date, order_state;
