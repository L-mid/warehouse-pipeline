-- Business outputs as SQL views.
-- each view reports its sourced staging table.


CREATE OR REPLACE VIEW v_fact_orders_current AS
SELECT *
FROM fact_orders;

CREATE OR REPLACE VIEW v_fact_order_lines_current AS
SELECT *
FROM fact_order_lines;

CREATE OR REPLACE VIEW v_fact_order_tenders_current AS
SELECT *
FROM fact_order_tenders;


CREATE OR REPLACE VIEW v_dq_results_latest AS
SELECT *
FROM dq_results
WHERE run_id = (
    SELECT run_id
    FROM run_ledger
    WHERE status = 'succeeded'
    ORDER BY finished_at DESC NULLS LAST, started_at DESC
    LIMIT 1
);
