from __future__ import annotations

import pytest

from tests.integration.helpers.square import (
    create_stage_run,
    square_line,
    square_order,
    square_tender,
)
from warehouse_pipeline.stage import StageTableLoadResult


@pytest.mark.docker_required
def test_db_stage_load_inserts_square_rows_and_records_explicit_and_duplicate_rejects(conn) -> None:
    order_v1 = square_order(
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
    )

    order_v2_duplicate = square_order(
        order_id="ord-100",
        state="COMPLETED",
        created_at="2026-03-10T10:00:00Z",
        updated_at="2026-03-10T10:06:00Z",
        closed_at="2026-03-10T10:06:00Z",
        total_money_cents=9999,
        net_total_money_cents=9999,
        customer_id="CUST-SHOULD-LOSE",
        line_items=[
            square_line(
                uid="line-1",
                catalog_object_id="item-espresso",
                name="Espresso",
                quantity="1",
                base_price_cents=9999,
                gross_sales_cents=9999,
                net_sales_cents=9999,
            )
        ],
        tenders=[
            square_tender(
                tender_id="tender-1",
                tender_type="CARD",
                amount_cents=9999,
                card_brand="MASTERCARD",
            )
        ],
    )

    order_with_invalid_line = square_order(
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
                uid="line-bad",
                catalog_object_id="item-bagel",
                name="Bagel",
                quantity="0",
                base_price_cents=1000,
                gross_sales_cents=1000,
                net_sales_cents=1000,
            )
        ],
    )

    run_id, mapped, results = create_stage_run(
        conn,
        orders=[order_v1, order_v2_duplicate, order_with_invalid_line],
    )

    assert len(mapped.rejects) == 1
    assert results["stg_square_orders"] == StageTableLoadResult(
        table_name="stg_square_orders",
        inserted_count=2,
        duplicate_reject_count=1,
        explicit_reject_count=0,
    )
    assert results["stg_square_order_lines"] == StageTableLoadResult(
        table_name="stg_square_order_lines",
        inserted_count=1,
        duplicate_reject_count=1,
        explicit_reject_count=1,
    )
    assert results["stg_square_tenders"] == StageTableLoadResult(
        table_name="stg_square_tenders",
        inserted_count=1,
        duplicate_reject_count=1,
        explicit_reject_count=0,
    )

    stage_counts = {
        "orders": conn.execute(
            "SELECT COUNT(*) FROM stg_square_orders WHERE run_id = %s",
            (run_id,),
        ).fetchone()[0],
        "lines": conn.execute(
            "SELECT COUNT(*) FROM stg_square_order_lines WHERE run_id = %s",
            (run_id,),
        ).fetchone()[0],
        "tenders": conn.execute(
            "SELECT COUNT(*) FROM stg_square_tenders WHERE run_id = %s",
            (run_id,),
        ).fetchone()[0],
        "rejects": conn.execute(
            "SELECT COUNT(*) FROM reject_rows WHERE run_id = %s",
            (run_id,),
        ).fetchone()[0],
    }

    assert stage_counts == {
        "orders": 2,
        "lines": 1,
        "tenders": 1,
        "rejects": 4,
    }

    winning_customer_id = conn.execute(
        """
        SELECT customer_id
        FROM stg_square_orders
        WHERE run_id = %s AND order_id = %s
        """,
        (run_id, "ord-100"),
    ).fetchone()[0]
    assert winning_customer_id == "CUST-1"

    reject_reason_counts = dict(
        conn.execute(
            """
            SELECT reason_code, COUNT(*)
            FROM reject_rows
            WHERE run_id = %s
            GROUP BY reason_code
            ORDER BY reason_code
            """,
            (run_id,),
        ).fetchall()
    )
    assert reject_reason_counts == {
        "duplicate_key": 3,
        "invalid_line_quantity": 1,
    }
