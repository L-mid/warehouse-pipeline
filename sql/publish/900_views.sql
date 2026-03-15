-- Business outputs as SQL views.
-- each view reports its sourced staging table.



-- row per user
CREATE OR REPLACE VIEW v_dim_customer_latest AS
SELECT *
FROM dim_customer
WHERE source_run_id = (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1
);

-- row per order
CREATE OR REPLACE VIEW v_fact_orders_current AS
SELECT *
FROM fact_orders;

-- row per order item
CREATE OR REPLACE VIEW v_fact_order_items_current AS
SELECT *
FROM fact_order_items;


-- Compatibility aliases for the existing metrics/tests.(REMOVE THESE LATER)
CREATE OR REPLACE VIEW v_fact_orders_latest AS
SELECT *
FROM v_fact_orders_current;

CREATE OR REPLACE VIEW v_fact_order_items_latest AS
SELECT *
FROM v_fact_order_items_current;



-- row per dq metric
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
