from __future__ import annotations

from decimal import Decimal

import pytest

from tests.integration.helpers.square import (
    create_stage_run,
    square_line,
    square_order,
    square_tender,
)
from warehouse_pipeline.dq.gates import evaluate_model_gates, render_dq_summary
from warehouse_pipeline.dq.runner import run_model_dq
from warehouse_pipeline.transform.warehouse_build import build_warehouse


def _valid_orders() -> list[dict]:
    return [
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
        )
    ]


@pytest.mark.docker_required
def test_model_dq_and_gates_pass_on_valid_square_snapshot_build(conn) -> None:
    run_id, _, _ = create_stage_run(conn, orders=_valid_orders(), mode="snapshot")
    build_warehouse(conn, run_id=run_id)

    summaries = run_model_dq(conn, run_id=run_id)
    decision = evaluate_model_gates(conn, run_id=run_id)
    summary_text = render_dq_summary(conn, run_id=run_id, decision=decision)

    assert tuple(summary.table_name for summary in summaries) == (
        "stg_square_orders",
        "stg_square_order_lines",
        "stg_square_tenders",
        "fact_orders",
        "fact_order_lines",
        "fact_order_tenders",
    )
    assert all(summary.passed for summary in summaries)
    assert decision.passed is True
    assert decision.failures == ()
    assert "DQ PASS" in summary_text
    assert "stg_square_order_lines orphan orders: 0" in summary_text

    orphan_lines = conn.execute(
        """
        SELECT metric_value
        FROM dq_results
        WHERE run_id = %s
          AND table_name = 'stg_square_order_lines'
          AND metric_name = 'orphan_orders.count'
        """,
        (run_id,),
    ).fetchone()[0]
    assert orphan_lines == Decimal("0.000000")

    fact_line_parity = conn.execute(
        """
        SELECT metric_value
        FROM dq_results
        WHERE run_id = %s
          AND table_name = 'fact_order_lines'
          AND metric_name = 'warehouse_parity.count_diff'
        """,
        (run_id,),
    ).fetchone()[0]
    assert fact_line_parity == Decimal("0.000000")


@pytest.mark.docker_required
def test_model_gates_fail_when_orphan_stage_rows_exist(conn) -> None:
    run_id, _, _ = create_stage_run(conn, orders=_valid_orders(), mode="snapshot")
    build_warehouse(conn, run_id=run_id)

    conn.execute(
        """
        INSERT INTO stg_square_order_lines (
            run_id,
            order_id,
            line_uid,
            catalog_object_id,
            name,
            variation_name,
            quantity,
            base_price_money,
            gross_sales_money,
            total_discount_money,
            total_tax_money,
            net_sales_money,
            currency_code
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            NULL,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        """,
        (
            run_id,
            "ord-missing",
            "line-orphan",
            "item-orphan",
            "Ghost Item",
            Decimal("1.000"),
            Decimal("5.00"),
            Decimal("5.00"),
            Decimal("0.00"),
            Decimal("0.00"),
            Decimal("5.00"),
            "USD",
        ),
    )

    run_model_dq(conn, run_id=run_id)
    decision = evaluate_model_gates(conn, run_id=run_id)
    summary_text = render_dq_summary(conn, run_id=run_id, decision=decision)

    assert decision.passed is False
    assert any(
        failure.table_name == "stg_square_order_lines"
        and failure.metric_name == "orphan_orders.count"
        and failure.actual == Decimal("1")
        for failure in decision.failures
    )
    assert any(
        failure.table_name == "fact_order_lines"
        and failure.metric_name == "warehouse_parity.count_diff"
        and failure.actual == Decimal("1")
        for failure in decision.failures
    )
    assert "DQ FAIL" in summary_text
    assert "stg_square_order_lines orphan orders: 1" in summary_text
