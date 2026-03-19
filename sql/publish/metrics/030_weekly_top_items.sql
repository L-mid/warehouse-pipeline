WITH rolled AS (
    SELECT
        date_trunc('week', business_date::timestamp)::date AS week_start,
        COALESCE(
            catalog_object_id,
            NULLIF(TRIM(CONCAT_WS(' / ', item_name, variation_name)), ''),
            'UNKNOWN_ITEM'
        ) AS item_key,
        MAX(item_name) AS item_name,
        MAX(variation_name) AS variation_name,
        COALESCE(SUM(quantity) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(14,3) AS quantity_sold,
        COALESCE(SUM(net_sales_money) FILTER (WHERE order_state = 'COMPLETED'), 0)::numeric(12,2) AS net_sales_money
    FROM v_fact_order_lines_current
    WHERE business_date IS NOT NULL
    GROUP BY
        date_trunc('week', business_date::timestamp)::date,
        COALESCE(
            catalog_object_id,
            NULLIF(TRIM(CONCAT_WS(' / ', item_name, variation_name)), ''),
            'UNKNOWN_ITEM'
        )
),
ranked AS (
    SELECT
        week_start,
        item_key,
        item_name,
        variation_name,
        quantity_sold,
        net_sales_money,
        ROW_NUMBER() OVER (
            PARTITION BY week_start
            ORDER BY net_sales_money DESC, quantity_sold DESC, item_key
        ) AS item_rank
    FROM rolled
)
SELECT
    week_start,
    item_rank,
    item_key,
    item_name,
    variation_name,
    quantity_sold,
    net_sales_money
FROM ranked
WHERE item_rank <= 10
ORDER BY week_start, item_rank;
