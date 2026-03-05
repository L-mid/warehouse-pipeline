-- 010_tables.sql
-- ## run_ledger:   All runs, one row per pipeline run.
-- ## stg_*:        the typed staging tables (all the rows tagged with `run_id`).
-- ## reject_rows:  collected bad rows over this run.
-- ## dq_results:   stored data quality checks per run


-- Run_ledger (Has 1 row per run)
CREATE TABLE IF NOT EXISTS run_ledger (
    run_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system   text NOT NULL DEFAULT 'dummyjson',
    mode            text NOT NULL CHECK (mode IN ('snapshot', 'live')),
    snapshot_key    text,                           -- `dummyjson/<v>` or file path, optional
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    error_message   text,
    git_sha         text,
    args_json       jsonb NOT NULL DEFAULT '{}'::jsonb
);


-- stg_customers        (from users.json)
-- grain is one row per (run_id, customer_id).
CREATE TABLE IF NOT EXISTS stg_customers (
    run_id              uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    customer_id         bigint NOT NULL,
    first_name          text,
    last_name           text,
    full_name           text,           -- derived in staging. first_name || ' ' || last_name.
    email               text,
    phone               text, 
    city                text,
    country             text,
    company             text,             
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, customer_id)
);



-- stg_products         (from products.json).
-- grain is one row per (run_id, product_id).
CREATE TABLE IF NOT EXISTS stg_products (
    run_id          uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    product_id      bigint NOT NULL,
    sku             text,           -- an optional derived stable sku for later metrics
    title           text,
    brand           text,
    category        text,
    price_usd       numeric(12,2),
    discount_pct    numeric(8,4),
    rating          numeric(6,3),
    stock           integer,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, product_id)
);



-- stg_orders           (from carts.json)
-- grain is one row per (run_id, order_id)  
-- and [order_id == cart id]
CREATE TABLE IF NOT EXISTS stg_orders (
    run_id          uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    order_id        bigint NOT NULL,
    customer_id     bigint NOT NULL,
    order_ts        timestamptz,        -- derived (deterministic) or nullable for this spec
    country         text,
    status          text,               -- derived from pipeline, in (paid/refunded/pending/canceled)
    total_usd       numeric(12,2),
    total_products  integer,
    total_quantity  integer,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, order_id)
);



-- stg_order_items      (from carts.json products[])
-- grain is one row per (run_id, order_id, line_id)
CREATE TABLE IF NOT EXISTS stg_order_items (
    run_id          uuid NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    order_id        bigint NOT NULL,
    line_id         integer NOT NULL,                   -- 1..N within the order
    product_id      bigint,
    sku             text,
    qty             integer,
    unit_price_usd  numeric(12,2),
    discount_pct    numeric(8,4), DEFAULT 0,            -- Default is 0 (not null).
    gross_usd       numeric(12,2),                      -- `qty` * `unit_price`
    created_at      timestamptz NOT NULL DEFAULT now(),
    net_usd         numeric(12,2),                      -- after discounted
    PRIMARY KEY (run_id, order_id, line_id)
);



-- reject_rows
-- Grain is one row per rejected row (all table fields non-null)
CREATE TABLE IF NOT EXISTS reject_rows (
    reject_id       bigserial PRIMARY KEY,
    run_id          uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
    table_name      text NOT NULL,              -- which stg_* this reject was collected from
    source_ref      integer NOT NULL,           -- where it came from, -- e.g. `'users[12]'`, `'carts[4].products[2]'`, .
    raw_payload     jsonb NOT NULL,             -- storing `{"raw": {...}, "canonical": {...}}` or `{"raw": {...}}` from ingestion,
    reason_code     text NOT NULL,              -- or the JSON object for JSONL
    reason_detail   text NOT NULL,
    rejected_at     timestamptz NOT NULL DEFAULT now()
);



-- dq_results           (data quality metric rows stored as rows (per run))
-- grain is one row per (run_id, table_name, check_name, metric_name).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id          uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
    table_name      text NOT NULL,
    check_name      text NOT NULL,
    metric_name     text NOT NULL,
    metric_value    numeric(18,6) NOT NULL,
    passed          boolean NOT NULL,
    details_json    jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, table_name, check_name, metric_name)
);




