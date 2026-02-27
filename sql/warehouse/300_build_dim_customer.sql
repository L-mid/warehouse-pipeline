TRUNCATE dim_customer;

INSERT INTO dim_customer (
  customer_id, first_name, last_name, full_name,
  company, city, country, phone_1, phone_2, email,
  subscription_date, website, source_run_id
)
SELECT
  customer_id, first_name, last_name, full_name,
  company, city, country, phone_1, phone_2, email,
  subscription_date, website,
  %(customers_run_id)s::uuid AS source_run_id
FROM stg_customers
WHERE run_id = %(customers_run_id)s::uuid;