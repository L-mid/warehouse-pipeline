-- Create warehouse tables schema, (aka dim/fact).


-- ## `dim_customer`
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


-- ## `dim_date`
CREATE TABLE IF NOT EXISTS dim_date (
    date            date PRIMARY KEY,
    year            int NOT NULL,
    month           int NOT NULL,
    day             int NOT NULL,
    iso_week        int NOT NULL,
    iso_dow         int NOT NULL,   -- 1=Mon..7=Sun
    quarter         int NOT NULL,
    week_start      date NOT NULL,
    month_start     date NOT NULL,
    built_at        timestamptz NOT NULL DEFAULT now()
);

-- no index



-- ## `fact_orders`
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id        bigint PRIMARY KEY,           -- fact_orders grain is one row per order
    customer_id     bigint NOT NULL,
    date            date,
    order_ts        timestamptz,
    country         text,
    status          text,
    total_usd       numeric(12,2),
    source_run_id   uuid NOT NULL,
    built_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fact_orders_customer_idx
    ON fact_orders (customer_id);

CREATE INDEX IF NOT EXISTS fact_orders_date_idx
    ON fact_orders (date);



-- ## `fact_order_items`
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id            bigint NOT NULL,
    line_id             int  NOT NULL,            -- grain is one row per (order_id, line_id)
    customer_id         bigint,                   -- this is nullable if an orphan item exists
    date                date,
    product_id          bigint,
    sku                 text,
    qty                 int,
    unit_price_usd      numeric(12,2),
    gross_usd           numeric(12,2),
    net_usd             numeric(12,2),
    source_run_id       uuid NOT NULL,
    built_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_id)
);

CREATE INDEX IF NOT EXISTS fact_order_items_order_idx
    ON fact_order_items (order_id);

CREATE INDEX IF NOT EXISTS fact_order_items_sku_idx
    ON fact_order_items (sku);