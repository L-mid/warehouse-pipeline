-- 010_tables.sql
-- ## run_ledger:   All runs, one row per pipeline run.
-- ## stg_*:        the typed staging tables (all the rows tagged with `run_id`).
-- ## reject_rows:  collected bad rows over this run.
-- ## dq_results:   stored data quality checks per run


-- Run_ledger (Has 1 row per run)
CREATE TABLE IF NOT EXISTS run_ledger (
    run_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system   text NOT NULL DEFAULT 'square_orders',
    mode            text NOT NULL CHECK (mode IN ('snapshot', 'live')),
    snapshot_key    text,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error_message   text,
    git_sha         text,
    args_json       jsonb NOT NULL DEFAULT '{}'::jsonb
);


-- stg_square_orders
-- grain is one row per (run_id, order_id).
CREATE TABLE IF NOT EXISTS stg_square_orders (
    run_id                  uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    order_id                text NOT NULL,
    location_id             text,
    customer_id             text,
    state                   text,
    created_at_source       timestamptz,
    updated_at_source       timestamptz,
    closed_at_source        timestamptz,
    currency_code           text,
    total_money             numeric(12,2),
    net_total_money         numeric(12,2),
    total_discount_money    numeric(12,2),
    total_tax_money         numeric(12,2),
    total_tip_money         numeric(12,2),
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, order_id)
);




-- stg_square_order_lines
-- grain is one row per (run_id, order_id, line_uid).
CREATE TABLE IF NOT EXISTS stg_square_order_lines (
    run_id                  uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    order_id                text NOT NULL,
    line_uid                text NOT NULL,
    catalog_object_id       text,
    name                    text,
    variation_name          text,
    quantity                numeric(14,3),
    base_price_money        numeric(12,2),
    gross_sales_money       numeric(12,2),
    total_discount_money    numeric(12,2),
    total_tax_money         numeric(12,2),
    net_sales_money         numeric(12,2),
    currency_code           text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, order_id, line_uid)
);



-- stg_orders
-- grain is one row per (run_id, tender_id)
CREATE TABLE IF NOT EXISTS stg_square_tenders (
    run_id              uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    order_id            text NOT NULL,
    tender_id           text NOT NULL,
    tender_type         text,
    card_brand          text,
    amount_money        numeric(12,2),
    tip_money           numeric(12,2),
    currency_code       text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, order_id, tender_id)
);





-- reject_rows
-- Grain is one row per rejected row (all table fields non-null)
CREATE TABLE IF NOT EXISTS reject_rows (
    reject_id       bigserial PRIMARY KEY,
    run_id          uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    table_name      text NOT NULL,
    source_ref      integer NOT NULL,
    raw_payload     jsonb NOT NULL,
    reason_code     text NOT NULL,
    reason_detail   text NOT NULL,
    rejected_at     timestamptz NOT NULL DEFAULT now()
);



-- dq_results           (data quality metric rows stored as rows (per run))
-- grain is one row per (run_id, table_name, check_name, metric_name).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id          uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    table_name      text NOT NULL,
    check_name      text NOT NULL,
    metric_name     text NOT NULL,
    metric_value    numeric(18,6) NOT NULL,
    passed          boolean NOT NULL,
    details_json    jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, table_name, check_name, metric_name)
);
