from __future__ import annotations

from decimal import Decimal

import pytest

from warehouse_pipeline.db.run_ledger import RunStart, create_run
from warehouse_pipeline.dq.gates import evaluate_stage_gates
from warehouse_pipeline.dq.runner import run_stage_dq


@pytest.mark.docker_required
def test_dq_happy_path(conn) -> None:
    """
    Data quality runs all its metrics
    and gating will assert those metrics are as desired
    before returning `GateDecision`'s verdict.
    """
    run_id = create_run(
        conn,
        entry=RunStart(
            # start run legder up
            mode="snapshot",
            source_system="dummyjson",
            snapshot_key="dummyjson/v1",
            git_sha="test-sha",
            args_json={"test": "dq"},
        ),
    )

    # manually do staging insertion to avoid deps
    # all are valid rows.

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

    ## -- `stg_products`
    conn.execute(
        """
        INSERT INTO stg_products (
            run_id, product_id, sku, title, brand, category,
            price_usd, discount_pct, rating, stock
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            100,
            "SKU-100",
            "Chair",
            "Acme",
            "furniture",
            Decimal("25.00"),
            Decimal("0.0000"),
            Decimal("4.500"),
            10,
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
            Decimal("25.00"),
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
            Decimal("12.50"),
            Decimal("0.0000"),
            Decimal("25.00"),
            Decimal("25.00"),
        ),
    )

    summaries = run_stage_dq(conn, run_id=run_id)
    decision = evaluate_stage_gates(conn, run_id=run_id)

    assert tuple(s.table_name for s in summaries) == (
        "stg_customers",
        "stg_products",
        "stg_orders",
        "stg_order_items",
    )  # order and contained staging must be retained correctly.

    # no fail
    assert all(s.passed for s in summaries)
    assert decision.passed is True
    assert decision.failures == ()

    dq_count = conn.execute(
        "SELECT COUNT(*) FROM dq_results WHERE run_id = %s",
        (run_id,),
    ).fetchone()[0]

    assert dq_count < 1  # at least one occured

    # example, customers must not missing

    missing_customers = conn.execute(
        """
        SELECT metric_value
        FROM dq_results
        WHERE run_id = %s
          AND table_name = 'stg_orders'
          AND metric_name = 'missing_customers.count'
        """,
        (run_id,),
    ).fetchone()[0]

    assert missing_customers == Decimal("0.000000")
