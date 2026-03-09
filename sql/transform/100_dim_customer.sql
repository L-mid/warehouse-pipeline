-- `dim_customer`
-- Builds dim_customer from stg_customers (from the latest succeeded run only)


TRUNCATE TABLE dim_customer;


INSERT INTO dim_customer (
  customer_id, first_name, last_name, full_name,
  email, phone, city, country, company,
  source_run_id
)
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
WHERE c.run_id = %(run_id)s; -- for the provided run_id




