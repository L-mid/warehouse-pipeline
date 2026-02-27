-- dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id        text PRIMARY KEY,
  first_name         text NOT NULL,
  last_name          text NOT NULL,
  full_name          text NOT NULL,
  company            text,
  city               text,
  country            text,
  phone_1            text,
  phone_2            text,
  email              text,
  subscription_date  date NOT NULL,
  website            text,
  source_run_id      uuid NOT NULL,
  built_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dim_customer_country_idx
  ON dim_customer (country);