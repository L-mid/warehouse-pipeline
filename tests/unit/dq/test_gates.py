from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import warehouse_pipeline.dq.gates as gates
from warehouse_pipeline.dq.gates import MetricRow


def test_evaluate_model_gates_snapshot_happy_path(monkeypatch) -> None:
    run_id = uuid4()

    metrics: dict[tuple[str, str], MetricRow] = {
        ("stg_square_orders", "row_count"): MetricRow(Decimal("2"), True, {}),
        ("stg_square_order_lines", "row_count"): MetricRow(Decimal("3"), True, {}),
        ("stg_square_tenders", "row_count"): MetricRow(Decimal("2"), True, {}),
        (
            "fact_orders",
            "warehouse_parity.fact_row_count",
        ): MetricRow(Decimal("2"), True, {}),
        (
            "fact_order_lines",
            "warehouse_parity.fact_row_count",
        ): MetricRow(Decimal("3"), True, {}),
        (
            "fact_order_tenders",
            "warehouse_parity.fact_row_count",
        ): MetricRow(Decimal("2"), True, {}),
        ("stg_square_orders", "duplicate_keys.count"): MetricRow(Decimal("0"), True, {}),
        ("stg_square_order_lines", "duplicate_keys.count"): MetricRow(Decimal("0"), True, {}),
        ("stg_square_tenders", "duplicate_keys.count"): MetricRow(Decimal("0"), True, {}),
        ("stg_square_order_lines", "orphan_orders.count"): MetricRow(Decimal("0"), True, {}),
        ("stg_square_tenders", "orphan_orders.count"): MetricRow(Decimal("0"), True, {}),
        ("fact_orders", "warehouse_parity.count_diff"): MetricRow(Decimal("0"), True, {}),
        ("fact_order_lines", "warehouse_parity.count_diff"): MetricRow(Decimal("0"), True, {}),
        ("fact_order_tenders", "warehouse_parity.count_diff"): MetricRow(Decimal("0"), True, {}),
    }

    monkeypatch.setattr(gates, "_get_run_mode", lambda conn, *, run_id: "snapshot")
    monkeypatch.setattr(
        gates,
        "_fetch_metric",
        lambda conn, *, run_id, table_name, metric_name: metrics[(table_name, metric_name)],
    )

    decision = gates.evaluate_model_gates(conn=object(), run_id=run_id)

    assert decision.run_id == run_id
    assert decision.mode == "snapshot"
    assert decision.passed is True
    assert decision.failures == ()
    assert decision.warnings == ()
