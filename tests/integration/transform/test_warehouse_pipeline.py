from __future__ import annotations

from uuid import uuid4

import pytest

from warehouse_pipeline.transform.warehouse_build import build_warehouse


def _insert_run(conn, *, run_id, mode: str = "snapshot") -> None:
    conn.execute(
        """
        INSERT INTO run_ledger (run_id, mode, status, finished_at)
        VALUES (%s, %s, 'succeeded', now())
        """,
        (run_id, mode),
    )


def _insert_order(
    conn, *, run_id, order_id: int, customer_id: int, total_usd: float, status: str
) -> None:
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
            order_id,
            customer_id,
            "2026-03-07T10:00:00+00:00",
            "UK",
            status,
            total_usd,
            1,
            1,
        ),
    )


def _insert_item(
    conn,
    *,
    run_id,
    order_id: int,
    line_id: int,
    product_id: int,
    qty: int,
    unit_price_usd: float,
    gross_usd: float,
    net_usd: float,
) -> None:
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
            order_id,
            line_id,
            product_id,
            f"SKU-{product_id}",
            qty,
            unit_price_usd,
            0.0,
            gross_usd,
            net_usd,
        ),
    )


@pytest.mark.docker_required
def test_build_warehouse_happy_path(conn) -> None:
    """Tests transforms from staged tables work and looks like expected."""

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


@pytest.mark.docker_required
def test_build_warehouse_same_input_twice_is_idempotent(conn) -> None:
    """Runs don't delete or append themselves on the same input."""
    run_1 = uuid4()
    run_2 = uuid4()

    _insert_run(conn, run_id=run_1)
    _insert_order(conn, run_id=run_1, order_id=10, customer_id=1, total_usd=25.00, status="paid")
    _insert_item(
        conn,
        run_id=run_1,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=2,
        unit_price_usd=12.50,
        gross_usd=25.00,
        net_usd=25.00,
    )
    build_warehouse(conn, run_id=run_1, step_name="build_facts")

    _insert_run(conn, run_id=run_2)
    _insert_order(conn, run_id=run_2, order_id=10, customer_id=1, total_usd=25.00, status="paid")
    _insert_item(
        conn,
        run_id=run_2,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=2,
        unit_price_usd=12.50,
        gross_usd=25.00,
        net_usd=25.00,
    )
    build_warehouse(conn, run_id=run_2, step_name="build_facts")

    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM fact_order_items").fetchone()[0] == 1


@pytest.mark.docker_required
def test_build_warehouse_changed_order_updates_not_duplicates(conn) -> None:
    """Updates do not duplicate a line in `fact_order_items`."""
    run_1 = uuid4()
    run_2 = uuid4()

    _insert_run(conn, run_id=run_1, mode="incremental")
    _insert_order(conn, run_id=run_1, order_id=10, customer_id=1, total_usd=25.00, status="paid")
    _insert_item(
        conn,
        run_id=run_1,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=2,
        unit_price_usd=12.50,
        gross_usd=25.00,
        net_usd=25.00,
    )
    build_warehouse(conn, run_id=run_1, step_name="build_facts")

    _insert_run(conn, run_id=run_2, mode="incremental")
    _insert_order(
        conn, run_id=run_2, order_id=10, customer_id=1, total_usd=30.00, status="refunded"
    )
    _insert_item(
        conn,
        run_id=run_2,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=2,
        unit_price_usd=15.00,
        gross_usd=30.00,
        net_usd=30.00,
    )
    build_warehouse(conn, run_id=run_2, step_name="build_facts")

    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM fact_order_items").fetchone()[0] == 1

    order_row = conn.execute(
        "SELECT status, total_usd, source_run_id FROM fact_orders WHERE order_id = 10"
    ).fetchone()
    item_row = conn.execute(
        "SELECT unit_price_usd, gross_usd, net_usd, source_run_id "
        "FROM fact_order_items WHERE order_id = 10 AND line_id = 1"
    ).fetchone()

    assert order_row == ("refunded", 30.00, run_2)
    assert item_row == (15.00, 30.00, 30.00, run_2)


@pytest.mark.docker_required
def test_build_warehouse_overlapping_incremental_rerun(conn) -> None:
    """Incremental mode keeps untouched orders stable."""
    run_1 = uuid4()
    run_2 = uuid4()

    _insert_run(conn, run_id=run_1, mode="incremental")
    _insert_order(conn, run_id=run_1, order_id=10, customer_id=1, total_usd=25.00, status="paid")
    _insert_order(conn, run_id=run_1, order_id=20, customer_id=2, total_usd=40.00, status="paid")
    _insert_item(
        conn,
        run_id=run_1,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=1,
        unit_price_usd=10.00,
        gross_usd=10.00,
        net_usd=10.00,
    )
    _insert_item(
        conn,
        run_id=run_1,
        order_id=10,
        line_id=2,
        product_id=101,
        qty=1,
        unit_price_usd=15.00,
        gross_usd=15.00,
        net_usd=15.00,
    )
    _insert_item(
        conn,
        run_id=run_1,
        order_id=20,
        line_id=1,
        product_id=200,
        qty=1,
        unit_price_usd=40.00,
        gross_usd=40.00,
        net_usd=40.00,
    )
    build_warehouse(conn, run_id=run_1, step_name="build_facts")

    _insert_run(conn, run_id=run_2, mode="incremental")
    _insert_order(conn, run_id=run_2, order_id=10, customer_id=1, total_usd=12.00, status="paid")
    _insert_item(
        conn,
        run_id=run_2,
        order_id=10,
        line_id=1,
        product_id=100,
        qty=1,
        unit_price_usd=12.00,
        gross_usd=12.00,
        net_usd=12.00,
    )
    build_warehouse(conn, run_id=run_2, step_name="build_facts")

    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM fact_order_items").fetchone()[0] == 2

    order_10_items = conn.execute(
        "SELECT line_id, net_usd FROM fact_order_items WHERE order_id = 10 ORDER BY line_id"
    ).fetchall()
    order_20_items = conn.execute(
        "SELECT line_id, net_usd FROM fact_order_items WHERE order_id = 20 ORDER BY line_id"
    ).fetchall()

    assert order_10_items == [(1, 12.00)]
    assert order_20_items == [(1, 40.00)]
