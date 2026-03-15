-- `dim_customer`
-- Builds dim_customer from stg_customers. last write wins per customer.


TRUNCATE TABLE dim_customer;


WITH staged_customers AS (
  SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COALESCE(
      c.full_name,
      TRIM(COALESCE(c.first_name, '') || ' ' || COALESCE(c.last_name, ''))
    ) AS full_name,   -- safety
    c.email,
    c.phone,
    c.city,
    c.country,
    c.company,
    c.run_id AS source_run_id
  FROM stg_customers c
  WHERE c.run_id = %(run_id)s -- for the provided run_id
)
INSERT INTO dim_customer (
  customer_id, first_name, last_name, full_name,
  email, phone, city, country, company,
  source_run_id
)
SELECT
  customer_id, first_name, last_name, full_name,
  email, phone, city, country, company,
  source_run_id
FROM staged_customers
ON CONFLICT (customer_id) DO UPDATE
SET
  first_name = EXCLUDED.first_name,
  last_name = EXCLUDED.last_name,
  full_name = EXCLUDED.full_name,
  email = EXCLUDED.email,
  phone = EXCLUDED.phone,
  city = EXCLUDED.city,
  country = EXCLUDED.country,
  company = EXCLUDED.company,
  source_run_id = EXCLUDED.source_run_id,
  built_at = now();
