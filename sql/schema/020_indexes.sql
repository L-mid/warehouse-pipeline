-- Common Indexes for speed


CREATE INDEX IF NOT EXISTS run_ledger_status_finished_idx
    ON run_ledger (status, finished_at DESC);

CREATE INDEX IF NOT EXISTS stg_square_orders_run_idx
    ON stg_square_orders (run_id);

CREATE INDEX IF NOT EXISTS stg_square_orders_updated_at_idx
    ON stg_square_orders (updated_at_source);

CREATE INDEX IF NOT EXISTS stg_square_orders_closed_at_idx
    ON stg_square_orders (closed_at_source);

CREATE INDEX IF NOT EXISTS stg_square_order_lines_run_idx
    ON stg_square_order_lines (run_id);

CREATE INDEX IF NOT EXISTS stg_square_order_lines_order_idx
    ON stg_square_order_lines (order_id);

CREATE INDEX IF NOT EXISTS stg_square_order_lines_catalog_idx
    ON stg_square_order_lines (catalog_object_id);

CREATE INDEX IF NOT EXISTS stg_square_tenders_run_idx
    ON stg_square_tenders (run_id);

CREATE INDEX IF NOT EXISTS stg_square_tenders_order_idx
    ON stg_square_tenders (order_id);

CREATE INDEX IF NOT EXISTS reject_rows_run_table_idx
    ON reject_rows (run_id, table_name);

CREATE INDEX IF NOT EXISTS reject_rows_reason_idx
    ON reject_rows (table_name, reason_code);

CREATE INDEX IF NOT EXISTS dq_results_run_table_idx
    ON dq_results (run_id, table_name);

CREATE INDEX IF NOT EXISTS dq_results_check_idx
    ON dq_results (run_id, table_name, check_name);


-- Warehouse inner indexes

CREATE INDEX IF NOT EXISTS fact_orders_business_date_idx
    ON fact_orders (business_date);

CREATE INDEX IF NOT EXISTS fact_orders_state_idx
    ON fact_orders (order_state);

CREATE INDEX IF NOT EXISTS fact_orders_location_idx
    ON fact_orders (location_id);

CREATE INDEX IF NOT EXISTS fact_order_lines_business_date_idx
    ON fact_order_lines (business_date);

CREATE INDEX IF NOT EXISTS fact_order_lines_order_state_idx
    ON fact_order_lines (order_state);

CREATE INDEX IF NOT EXISTS fact_order_lines_catalog_idx
    ON fact_order_lines (catalog_object_id);

CREATE INDEX IF NOT EXISTS fact_order_tenders_business_date_idx
    ON fact_order_tenders (business_date);

CREATE INDEX IF NOT EXISTS fact_order_tenders_type_idx
    ON fact_order_tenders (tender_type);

CREATE INDEX IF NOT EXISTS fact_order_tenders_order_state_idx
    ON fact_order_tenders (order_state);
