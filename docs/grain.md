Star schema:

`fact_orders` grain = one row per order.

`fact_order_items` grain = one row per (order_id, line_id)

`dim_customer` grain = one row per customer

`dim_date` grain = one row per date.


SQL DDL files for the warehouse tables:

`dim_customer` 

`dim_date` 

`fact_orders`

`fact_order_items`


Decided keys:

`dim_customer.customer_id`

`dim_date.date`

facts carry `customer_id` + `date` so joins are obvious.

