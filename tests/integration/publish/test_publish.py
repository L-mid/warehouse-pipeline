from __future__ import annotations

from decimal import Decimal

import pytest

from warehouse_pipeline.db.run_ledger import RunStart, create_run, mark_run_succeeded
from warehouse_pipeline.publish.views import apply_views, run_metric_query


@pytest.mark.docker_required
def test_publish_views_happy_path(conn) -> None:
    """Tests views appear to publish with an expected row."""
    run_id = create_run(
        conn,
        entry=RunStart(
            # start run legder.
            mode="snapshot",
            source_system="dummyjson",
            snapshot_key="dummyjson/v1",
            git_sha="test-sha",
            args_json={"test": "publish"},
        ),
    )
    mark_run_succeeded(conn, run_id=run_id)

    ## -- two rows
    conn.execute(
        """
        INSERT INTO fact_orders (
            order_id, customer_id, date, order_ts, country, status,
            total_usd, source_run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            10,
            1,
            "2026-03-07",
            "2026-03-07T10:00:00+00:00",
            "UK",
            "paid",
            Decimal("25.00"),
            run_id,
        ),
    )

    conn.execute(
        """
        INSERT INTO fact_orders (
            order_id, customer_id, date, order_ts, country, status,
            total_usd, source_run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            11,
            2,
            "2026-03-07",
            "2026-03-07T11:00:00+00:00",
            "UK",
            "refunded",
            Decimal("10.00"),
            run_id,
        ),
    )

    publish_result = apply_views(conn)

    assert publish_result.files_ran == ("900_views.sql",)  # only the main views sql ran
    assert "030_paid_vs_refunded_counts" in publish_result.metrics_available

    latest_count = conn.execute("SELECT COUNT(*) FROM v_fact_orders_latest").fetchone()[0]
    assert latest_count == 2

    # example, `030_paid_vs_refunded_counts`
    metric_result = run_metric_query(conn, name="030_paid_vs_refunded_counts")

    assert metric_result.name == "030_paid_vs_refunded_counts"
    assert metric_result.columns == ("country", "paid_orders", "refunded_orders")
    assert metric_result.rows == (
        {
            "country": "UK",
            "paid_orders": 1,
            "refunded_orders": 1,
        },
    )
