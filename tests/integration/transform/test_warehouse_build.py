from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from tests.integration.helpers.square import (
    create_stage_run,
    square_line,
    square_order,
    square_tender,
)
from warehouse_pipeline.transform.warehouse_build import build_warehouse


@pytest.mark.docker_required
def test_build_warehouse_snapshot_then_incremental_replaces_only_touched_orders(conn) -> None:
    run1_orders = [
        square_order(
            order_id="ord-100",
            state="COMPLETED",
            created_at="2026-03-10T10:00:00Z",
            updated_at="2026-03-10T10:05:00Z",
            closed_at="2026-03-10T10:05:00Z",
            total_money_cents=2300,
            net_total_money_cents=2000,
            total_discount_cents=200,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-1",
                    catalog_object_id="item-espresso",
                    name="Espresso",
                    quantity="1",
                    base_price_cents=1200,
                    gross_sales_cents=1200,
                    net_sales_cents=1200,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-1",
                    tender_type="CARD",
                    amount_cents=2300,
                    card_brand="VISA",
                )
            ],
        ),
        square_order(
            order_id="ord-200",
            state="COMPLETED",
            created_at="2026-03-10T11:00:00Z",
            updated_at="2026-03-10T11:05:00Z",
            closed_at="2026-03-10T11:05:00Z",
            total_money_cents=1100,
            net_total_money_cents=1000,
            total_tax_cents=100,
            line_items=[
                square_line(
                    uid="line-bagel",
                    catalog_object_id="item-bagel",
                    name="Bagel",
                    quantity="1",
                    base_price_cents=1000,
                    gross_sales_cents=1000,
                    net_sales_cents=1000,
                )
            ],
            tenders=[
                square_tender(
                    tender_id="tender-2",
                    tender_type="CASH",
                    amount_cents=1100,
                )
            ],
        ),
    ]
    run1_id, _, _ = create_stage_run(conn, orders=run1_orders, mode="snapshot")
    build_warehouse(conn, run_id=run1_id)

    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM fact_order_lines").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM fact_order_tenders").fetchone()[0] == 2

    run2_orders = [
        square_order(
            order_id="ord-100",
            state="COMPLETED",
            created_at="2026-03-10T10:00:00Z",
            updated_at="2026-03-12T09:00:00Z",
            closed_at="2026-03-12T09:05:00Z",
            total_money_cents=3000,
            net_total_money_cents=2600,
            total_discount_cents=200,
            total_tax_cents=200,
            line_items=[
                square_line(
                    uid="line-1",
                    catalog_object_id="item-espresso",
                    name="Espresso",
                    quantity="2",
                    base_price_cents=1200,
                    gross_sales_cents=2400,
                    net_sales_cents=2400,
                ),
                square_line(
                    uid="line-2",
                    catalog_object_id="item-cookie",
                    name="Cookie",
                    quantity="1",
                    base_price_cents=200,
                    gross_sales_cents=200,
                    net_sales_cents=200,
                ),
            ],
            tenders=[
                square_tender(
                    tender_id="tender-1",
                    tender_type="CARD",
                    amount_cents=3000,
                    card_brand="VISA",
                )
            ],
        )
    ]
    run2_id, _, _ = create_stage_run(conn, orders=run2_orders, mode="incremental")
    build_warehouse(conn, run_id=run2_id)

    assert conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM fact_order_lines").fetchone()[0] == 3
    assert conn.execute("SELECT COUNT(*) FROM fact_order_tenders").fetchone()[0] == 2

    refreshed_order = conn.execute(
        """
        SELECT total_money, net_total_money, business_date, source_run_id
        FROM fact_orders
        WHERE order_id = %s
        """,
        ("ord-100",),
    ).fetchone()
    assert refreshed_order == (
        Decimal("30.00"),
        Decimal("26.00"),
        date(2026, 3, 12),
        run2_id,
    )

    untouched_order = conn.execute(
        """
        SELECT total_money, source_run_id
        FROM fact_orders
        WHERE order_id = %s
        """,
        ("ord-200",),
    ).fetchone()
    assert untouched_order == (Decimal("11.00"), run1_id)

    refreshed_lines = conn.execute(
        """
        SELECT line_uid, quantity, net_sales_money, source_run_id
        FROM fact_order_lines
        WHERE order_id = %s
        ORDER BY line_uid
        """,
        ("ord-100",),
    ).fetchall()
    assert refreshed_lines == [
        ("line-1", Decimal("2.000"), Decimal("24.00"), run2_id),
        ("line-2", Decimal("1.000"), Decimal("2.00"), run2_id),
    ]

    refreshed_tender = conn.execute(
        """
        SELECT tender_id, amount_money, source_run_id
        FROM fact_order_tenders
        WHERE order_id = %s
        """,
        ("ord-100",),
    ).fetchone()
    assert refreshed_tender == ("tender-1", Decimal("30.00"), run2_id)
