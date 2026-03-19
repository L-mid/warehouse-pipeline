
-- Grain is one row per order_id.
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
DELETE FROM fact_orders fo
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
            WHERE t.order_id = fo.order_id
        )
    );



INSERT INTO fact_orders (
    order_id,
    business_date,
    location_id,
    customer_id,
    order_state,
    created_at_source,
    updated_at_source,
    closed_at_source,
    currency_code,
    total_money,
    net_total_money,
    total_discount_money,
    total_tax_money,
    total_tip_money,
    source_run_id
)
SELECT
    o.order_id,
    COALESCE(
        o.closed_at_source::date,
        o.created_at_source::date,
        o.updated_at_source::date
    ) AS business_date,
    o.location_id,
    o.customer_id,
    o.state AS order_state,
    o.created_at_source,
    o.updated_at_source,
    o.closed_at_source,
    o.currency_code,
    o.total_money,
    o.net_total_money,
    o.total_discount_money,
    o.total_tax_money,
    o.total_tip_money,
    o.run_id AS source_run_id
FROM stg_square_orders o
WHERE o.run_id = %(run_id)s;
