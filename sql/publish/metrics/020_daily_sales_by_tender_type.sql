SELECT
    business_date,
    COALESCE(tender_type, 'UNKNOWN') AS tender_type,
    COUNT(*) FILTER (WHERE order_state = 'COMPLETED') AS tender_count,
    COALESCE(SUM(amount_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS tender_amount_money,
    COALESCE(SUM(tip_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS tender_tip_money
FROM v_fact_order_tenders_current
WHERE business_date IS NOT NULL
GROUP BY business_date, COALESCE(tender_type, 'UNKNOWN')
ORDER BY business_date, tender_type;
