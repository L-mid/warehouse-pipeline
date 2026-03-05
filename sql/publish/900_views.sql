-- Business outputs as SQL views. 
-- each view reports the latest succeeded run for its sourced staging table.



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
CREATE OR REPLACE VIEW v_fact_orders_latest AS
SELECT *
FROM fact_orders
WHERE source_run_id = (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1
);


-- row per item(s)
CREATE OR REPLACE VIEW v_fact_order_items_latest AS
SELECT *
FROM fact_order_items
WHERE source_run_id = (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1
);



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