-- Extension (for unique run_id assignment)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ingest_runs (run ledger)
CREATE TABLE IF NOT EXISTS ingest_runs (
  run_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at  timestamptz NOT NULL DEFAULT now(),
  input_path  text NOT NULL,
  table_name  text NOT NULL,
  status      text NOT NULL CHECK (status IN ('running','succeeded','failed'))
);

CREATE INDEX IF NOT EXISTS ingest_runs_table_started_idx
  ON ingest_runs (table_name, started_at DESC);


-- stg_customers (typed columns, staging and run lineage)
-- grain is one row per customer (per run).
CREATE TABLE IF NOT EXISTS stg_customers (
  run_id             uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  customer_id        text NOT NULL,               -- text, e.g. "dE014d010c7ab0c" (this is not numeric)
  first_name         text NOT NULL,
  last_name          text NOT NULL,
  full_name          text NOT NULL,               -- derived. first_name || ' ' || last_name.
  company            text,
  city               text,
  country            text,
  phone_1            text,
  phone_2            text,
  email              text,
  subscription_date  date NOT NULL,               
  website            text,
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, customer_id)
);

CREATE INDEX IF NOT EXISTS stg_customers_country_idx
  ON stg_customers (country);

CREATE INDEX IF NOT EXISTS stg_customers_run_idx 
  ON stg_customers (run_id);
  

-- stg_retail_transactions (typed columns, staging + run_id)
-- grain is one row per input row (per run), keyed by (run_id, source_row).
CREATE TABLE IF NOT EXISTS stg_retail_transactions (
  run_id        uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  source_row    integer NOT NULL, -- 1-based data-row index in the source file (header excluded).
  date          date NOT NULL, 
  week          integer NOT NULL,
  sku           text NOT NULL,
  product_category text NOT NULL,
  gender        text NOT NULL,
  marketplace   text NOT NULL,
  fulfillment   text NOT NULL,
  color         text,
  size          text,
  list_price    numeric(12,2) NOT NULL,
  discount_pct  numeric(6,4) NOT NULL,
  promo_type    text,              
  ad_spend      numeric(12,2) NOT NULL,
  impressions   integer NOT NULL,
  clicks        integer NOT NULL,
  cvr           numeric(10,6) NOT NULL,
  units_sold    integer NOT NULL,
  revenue       numeric(12,2) NOT NULL,
  rating        numeric(4,2) NOT NULL,
  reviews       integer NOT NULL,
  competitor_price_index numeric(10,6) NOT NULL,
  stock_on_hand integer NOT NULL,
  stockout_flag boolean NOT NULL,
  holiday_flag  boolean NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, source_row)
);

CREATE INDEX IF NOT EXISTS stg_retail_transactions_date_idx
  ON stg_retail_transactions (date);

CREATE INDEX IF NOT EXISTS stg_retail_transactions_sku_idx
  ON stg_retail_transactions (sku);

CREATE INDEX IF NOT EXISTS stg_retail_transactions_run_idx 
  ON stg_retail_transactions (run_id);

 
-- reject_rows
CREATE TABLE IF NOT EXISTS reject_rows (
  reject_id      bigserial PRIMARY KEY,
  run_id         uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  table_name     text NOT NULL,
  source_row     integer NOT NULL,        -- row/line number in the source file (1-based is expected)
  raw_payload    jsonb NOT NULL,          -- storing `{"raw": {...}, "canonical": {...}}` or `{"raw": {...}}` for CSV,
  reason_code    text NOT NULL,           -- or the JSON object for JSONL
  reason_detail  text NOT NULL,
  rejected_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reject_rows_run_table_idx
  ON reject_rows (run_id, table_name);

CREATE INDEX IF NOT EXISTS reject_rows_reason_idx
  ON reject_rows (table_name, reason_code);
