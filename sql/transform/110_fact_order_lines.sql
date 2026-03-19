-- Grain is exactly one row per (order_id, line_uid).



WITH current_run AS (
    SELECT mode
    FROM run_ledger
    WHERE run_id = %(run_id)s
),
touched_orders AS (
    SELECT DISTINCT order_id
    FROM stg_square_orders
    WHERE run_id = %(run_id)s
)
DELETE FROM fact_order_lines fol
WHERE
    (
        (SELECT mode FROM current_run) IN ('snapshot', 'live')
    )
    OR
    (
        (SELECT mode FROM current_run) = 'incremental'
        AND EXISTS (
            SELECT 1
            FROM touched_orders t
            WHERE t.order_id = fol.order_id
        )
    );


INSERT INTO fact_order_lines (
    order_id,
    line_uid,
    business_date,
    location_id,
    order_state,
    catalog_object_id,
    item_name,
    variation_name,
    quantity,
    base_price_money,
    gross_sales_money,
    total_discount_money,
    total_tax_money,
    net_sales_money,
    currency_code,
    source_run_id
)
SELECT
    l.order_id,
    l.line_uid,
    COALESCE(
        o.closed_at_source::date,
        o.created_at_source::date,
        o.updated_at_source::date
    ) AS business_date,
    o.location_id,
    o.state AS order_state,
    l.catalog_object_id,
    l.name AS item_name,
    l.variation_name,
    l.quantity,
    l.base_price_money,
    l.gross_sales_money,
    l.total_discount_money,
    l.total_tax_money,
    l.net_sales_money,
    COALESCE(l.currency_code, o.currency_code) AS currency_code,
    l.run_id AS source_run_id
FROM stg_square_order_lines l
JOIN stg_square_orders o
    ON o.run_id = l.run_id
    AND o.order_id = l.order_id
WHERE l.run_id = %(run_id)s;
