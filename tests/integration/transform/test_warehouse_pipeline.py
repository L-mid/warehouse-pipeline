from __future__ import annotations

from uuid import uuid4

from warehouse_pipeline.transform.warehouse_build import build_warehouse


def test_build_warehouse_happy_path(conn) -> None:
    
    # 'run_id' provided
    run_id = uuid4()


    # init all db tables with stuff to allow building

    ## -- `run_ledger`
    conn.execute(
        """
        INSERT INTO run_ledger (run_id, mode, status, finished_at)
        VALUES (%s, 'snapshot', 'succeeded', now())
        """,
        (run_id,),
    )

    ## -- `stg_customers`
    conn.execute(
        """
        INSERT INTO stg_customers (
            run_id, customer_id, first_name, last_name, full_name,
            email, phone, city, country, company
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            1,
            "Ada",
            "Lovelace",
            "Ada Lovelace",
            "ada@example.com",
            "123",
            "London",
            "UK",
            "Analytical Engines Ltd",
        ),
    )

    ## -- `stg_orders`
    conn.execute(
        """
        INSERT INTO stg_orders (
            run_id, order_id, customer_id, order_ts, country, status,
            total_usd, total_products, total_quantity
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            10,
            1,
            "2026-03-07T10:00:00+00:00",
            "UK",
            "paid",
            25.00,
            1,
            2,
        ),
    )

    ## -- `stg_order_items`
    conn.execute(
        """
        INSERT INTO stg_order_items (
            run_id, order_id, line_id, product_id, sku, qty,
            unit_price_usd, discount_pct, gross_usd, net_usd
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            10,
            1,
            100,
            "SKU-100",
            2,
            12.50,
            0.0,
            25.00,
            25.00,
        ),
    )

    # build
    result = build_warehouse(
        conn,
        run_id=run_id,
    )

    assert result.run_id == run_id
    assert result.files_ran == (
        "100_dim_customer.sql",
        "110_dim_date.sql",
        "120_fact_orders.sql",
        "130_fact_order_items.sql",
    )

    # get rows, test sql works
    dim_customer_count = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    fact_orders_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    fact_order_items_count = conn.execute("SELECT COUNT(*) FROM fact_order_items").fetchone()[0]


    assert dim_customer_count == 1
    assert fact_orders_count == 1
    assert fact_order_items_count == 1


    # precise sql test. example is `fact_orders`
    row = conn.execute(
        """
        SELECT order_id, customer_id, country, total_usd
        FROM fact_orders
        WHERE order_id = 10
        """
    ).fetchone()

    assert row == (10, 1, "UK", 25.00)    