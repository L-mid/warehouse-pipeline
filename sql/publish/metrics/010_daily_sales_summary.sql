SELECT
    business_date,
    COUNT(*) FILTER (WHERE order_state = 'COMPLETED') AS completed_order_count,
    COALESCE(SUM(total_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_money,
    COALESCE(SUM(net_total_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS net_total_money,
    COALESCE(SUM(total_discount_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_discount_money,
    COALESCE(SUM(total_tax_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_tax_money,
    COALESCE(SUM(total_tip_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_tip_money
FROM v_fact_orders_current
WHERE business_date IS NOT NULL
GROUP BY business_date
ORDER BY business_date;
