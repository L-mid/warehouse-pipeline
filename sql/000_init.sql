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

-- stg_customers (typed columns, staging + run lineage)
CREATE TABLE IF NOT EXISTS stg_customers (
  run_id        uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  customer_id   bigint NOT NULL,
  full_name     text NOT NULL,
  email         text,
  signup_date   date NOT NULL,
  country       text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, customer_id)
);

-- stg_orders (typed columns, staging + run_id)
CREATE TABLE IF NOT EXISTS stg_orders (
  run_id        uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  order_id      bigint NOT NULL,
  customer_id   bigint NOT NULL,
  order_ts      timestamptz NOT NULL,
  status        text NOT NULL,
  total_usd     numeric(12,2) NOT NULL,
  items         jsonb, -- optional (handy for JSONL that contains nested items)
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, order_id)
);

CREATE INDEX IF NOT EXISTS stg_orders_customer_idx
  ON stg_orders (customer_id);

-- reject_rows
CREATE TABLE IF NOT EXISTS reject_rows (
  reject_id      bigserial PRIMARY KEY,
  run_id         uuid NOT NULL REFERENCES ingest_runs(run_id) ON DELETE CASCADE,
  table_name     text NOT NULL,
  source_row     integer NOT NULL,        -- row/line number in the source file (1-based is fine)
  raw_payload    jsonb NOT NULL,          -- store {"raw_line": "..."} for CSV, or the JSON object for JSONL
  reason_code    text NOT NULL,
  reason_detail  text NOT NULL,
  rejected_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS reject_rows_run_table_idx
  ON reject_rows (run_id, table_name);

CREATE INDEX IF NOT EXISTS reject_rows_reason_idx
  ON reject_rows (table_name, reason_code);
