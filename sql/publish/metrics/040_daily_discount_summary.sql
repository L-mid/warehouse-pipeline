SELECT
    business_date,
    COUNT(*) FILTER (WHERE order_state = 'COMPLETED') AS completed_order_count,
    COALESCE(SUM(total_discount_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_discount_money,
    COALESCE(SUM(total_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS total_money,
    CASE
        WHEN COALESCE(SUM(total_money) FILTER (WHERE order_state = 'COMPLETED'), 0) = 0 THEN 0::numeric(12,4)
        ELSE (
            COALESCE(SUM(total_discount_money) FILTER (WHERE order_state = 'COMPLETED'), 0)
            / NULLIF(SUM(total_money) FILTER (WHERE order_state = 'COMPLETED'), 0)
        )::numeric(12,4)
    END AS discount_rate
FROM v_fact_orders_current
WHERE business_date IS NOT NULL
GROUP BY business_date
ORDER BY business_date;
