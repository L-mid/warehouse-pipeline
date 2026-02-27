-- fact_orders
CREATE TABLE IF NOT EXISTS fact_orders (
  order_id      text PRIMARY KEY,          -- fact_orders grain is 1 row per order
  customer_id   text NOT NULL,
  date          date NOT NULL,
  order_ts      timestamptz NOT NULL,
  country       text NOT NULL,
  status        text NOT NULL,
  total_usd     numeric(12,2) NOT NULL,
  source_run_id uuid NOT NULL,
  built_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_orders_customer_idx
  ON fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS fact_orders_date_idx
  ON fact_orders (date);