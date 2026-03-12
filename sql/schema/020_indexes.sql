-- Common Indexes for speed


-- run_ledger
CREATE INDEX IF NOT EXISTS run_ledger_status_finished_idx
    ON run_ledger (status, finished_at DESC);



-- stg_customers
CREATE INDEX IF NOT EXISTS stg_customers_run_idx
    ON stg_customers (run_id);

CREATE INDEX IF NOT EXISTS stg_customers_country_idx
    ON stg_customers (country);



-- stg_products
CREATE INDEX IF NOT EXISTS stg_products_run_idx
    ON stg_products (run_id);

CREATE INDEX IF NOT EXISTS stg_products_sku_idx
    ON stg_products (sku);



-- stg_orders
CREATE INDEX IF NOT EXISTS stg_orders_run_idx
    ON stg_orders (run_id);

CREATE INDEX IF NOT EXISTS stg_orders_customer_idx
    ON stg_orders (customer_id);

CREATE INDEX IF NOT EXISTS stg_orders_order_ts_idx
    ON stg_orders (order_ts);



-- stg_order_items
CREATE INDEX IF NOT EXISTS stg_order_items_run_idx
    ON stg_order_items (run_id);

CREATE INDEX IF NOT EXISTS stg_order_items_order_idx
    ON stg_order_items (order_id);

CREATE INDEX IF NOT EXISTS stg_order_items_sku_idx
    ON stg_order_items (sku);




-- reject_rows
CREATE INDEX IF NOT EXISTS reject_rows_run_table_idx
    ON reject_rows (run_id, table_name);

CREATE INDEX IF NOT EXISTS reject_rows_reason_idx
    ON reject_rows (table_name, reason_code);



-- dq_results
CREATE INDEX IF NOT EXISTS dq_results_run_table_idx
    ON dq_results (run_id, table_name);

CREATE INDEX IF NOT EXISTS dq_results_check_idx
    ON dq_results (run_id, table_name, check_name);
