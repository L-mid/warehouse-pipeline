-- Create warehouse tables schema, (aka dim/fact).


-- fact_orders
-- grain is one row per order_id
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                text PRIMARY KEY,
    business_date           date,
    location_id             text,
    customer_id             text,
    order_state             text,
    created_at_source       timestamptz,
    updated_at_source       timestamptz,
    closed_at_source        timestamptz,
    currency_code           text,
    total_money             numeric(12,2),
    net_total_money         numeric(12,2),
    total_discount_money    numeric(12,2),
    total_tax_money         numeric(12,2),
    total_tip_money         numeric(12,2),
    source_run_id           uuid NOT NULL,
    built_at                timestamptz NOT NULL DEFAULT now()
);


-- fact_order_lines
-- grain is one row per (order_id, line_uid)
CREATE TABLE IF NOT EXISTS fact_order_lines (
    order_id                text NOT NULL,
    line_uid                text NOT NULL,
    business_date           date,
    location_id             text,
    order_state             text,
    catalog_object_id       text,
    item_name               text,
    variation_name          text,
    quantity                numeric(14,3),
    base_price_money        numeric(12,2),
    gross_sales_money       numeric(12,2),
    total_discount_money    numeric(12,2),
    total_tax_money         numeric(12,2),
    net_sales_money         numeric(12,2),
    currency_code           text,
    source_run_id           uuid NOT NULL,
    built_at                timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, line_uid)
);



-- fact_order_tenders
-- grain is one row per (order_id, tender_id)
CREATE TABLE IF NOT EXISTS fact_order_tenders (
    order_id                text NOT NULL,
    tender_id               text NOT NULL,
    business_date           date,
    location_id             text,
    order_state             text,
    tender_type             text,
    card_brand              text,
    amount_money            numeric(12,2),
    tip_money               numeric(12,2),
    currency_code           text,
    source_run_id           uuid NOT NULL,
    built_at                timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, tender_id)
);
