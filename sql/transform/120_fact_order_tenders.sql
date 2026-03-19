-- Grain is one row per (order_id, tender_id).
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
DELETE FROM fact_order_tenders fot
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
            WHERE t.order_id = fot.order_id
        )
    );

INSERT INTO fact_order_tenders (
    order_id,
    tender_id,
    business_date,
    location_id,
    order_state,
    tender_type,
    card_brand,
    amount_money,
    tip_money,
    currency_code,
    source_run_id
)
SELECT
    t.order_id,
    t.tender_id,
    COALESCE(
        o.closed_at_source::date,
        o.created_at_source::date,
        o.updated_at_source::date
    ) AS business_date,
    o.location_id,
    o.state AS order_state,
    t.tender_type,
    t.card_brand,
    t.amount_money,
    t.tip_money,
    COALESCE(t.currency_code, o.currency_code) AS currency_code,
    t.run_id AS source_run_id
FROM stg_square_tenders t
JOIN stg_square_orders o
    ON o.run_id = t.run_id
    AND o.order_id = t.order_id
WHERE t.run_id = %(run_id)s;
