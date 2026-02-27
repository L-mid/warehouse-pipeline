CREATE TABLE IF NOT EXISTS fact_order_items (
  order_id        text NOT NULL,
  line_id         int  NOT NULL,            -- grain is 1 row per (order_id, line_id)
  customer_id     text,                     -- this is nullable if an orphan item exists
  date            date,
  sku             text NOT NULL,
  qty             int  NOT NULL,
  unit_price_usd  numeric(12,2) NOT NULL,
  discount_usd    numeric(12,2) NOT NULL,
  gross_usd       numeric(12,2) NOT NULL,
  net_usd         numeric(12,2) NOT NULL,
  source_run_id   uuid NOT NULL,
  built_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (order_id, line_id)
);

CREATE INDEX IF NOT EXISTS fact_order_items_order_idx
  ON fact_order_items (order_id);