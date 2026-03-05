-- dim_customer
-- Builds dim_customer from stg_customers (from the latest succeeded run only)

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id         bigint PRIMARY KEY,
  first_name          text,
  last_name           text,
  full_name           text,
  email               text,
  phone               text,
  city                text,
  country             text,
  company             text,
  source_run_id       uuid NOT NULL,
  built_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dim_customer_country_idx
  ON dim_customer (country);



TRUNCATE TABLE dim_customer;


WITH latest_run AS (
  SELECT run_id
  FROM run_ledger
  WHERE status = 'succeeded'
  ORDER BY finished_at DESC NULLS LAST, started_at DESC
  LIMIT 1   
)
INSERT INTO dim_customer (
  customer_id, first_name, last_name, full_name,
  email, phone, city, country, company,
  source_run_id
)
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  COALESCE(c.full_name, trim(COALESCE(c.first_name,'') || ' ' || COALESCE(c.last_name,''))) AS full_name,   -- safety
  c.email,
  c.phone,
  c.city,
  c.country,
  c.company,
  c.run_id AS source_run_id
FROM stg_customers c
WHERE c.run_id = (SELECT run_id FROM latest_run); -- just the latest