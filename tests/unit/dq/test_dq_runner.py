from __future__ import annotations

from decimal import Decimal
from typing import cast
from uuid import uuid4

import psycopg

import warehouse_pipeline.dq.runner as runner
from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.dq_results import DQMetricRow


def test_dq_runner_happy_path(monkeypatch) -> None:
    conn = cast(psycopg.Connection[tuple], FakeConnection())
    run_id = uuid4()

    metric_rows = [
        DQMetricRow(
            run_id=run_id,
            table_name="stg_square_orders",
            check_name="stage_volume",
            metric_name="row_count",
            metric_value=Decimal("1.000000"),
            passed=True,
            details_json={"row_count": 1},
        ),
        DQMetricRow(
            run_id=run_id,
            table_name="stg_square_orders",
            check_name="stage_keys",
            metric_name="duplicate_keys.count",
            metric_value=Decimal("0.000000"),
            passed=True,
            details_json={"duplicate_key_groups": 0},
        ),
        DQMetricRow(
            run_id=run_id,
            table_name="stg_square_orders",
            check_name="freshness",
            metric_name="freshness.max_updated_at_age_hours",
            metric_value=Decimal("1.000000"),
            passed=True,
            details_json={"age_hours": "1"},
        ),
    ]

    deleted: list[tuple[object, str]] = []
    upserted: list[DQMetricRow] = []

    monkeypatch.setattr(runner, "_ensure_run_exists", lambda conn, *, run_id: None)
    monkeypatch.setattr(
        runner,
        "_build_metrics_for_table",
        lambda conn, *, table_name, run_id: metric_rows,
    )
    monkeypatch.setattr(
        runner,
        "delete_dq_results",
        lambda conn, *, run_id, table_name: deleted.append((run_id, table_name)),
    )

    def fake_upsert_dq_results(conn, *, rows) -> int:
        materialized = list(rows)
        upserted.extend(materialized)
        return len(materialized)

    monkeypatch.setattr(runner, "upsert_dq_results", fake_upsert_dq_results)

    summary = runner.run_table_dq(conn, run_id=run_id, table_name="stg_square_orders")

    assert deleted == [(run_id, "stg_square_orders")]
    assert len(upserted) == 3

    assert summary.run_id == run_id
    assert summary.table_name == "stg_square_orders"
    assert summary.metrics_written == 3
    assert summary.failed_metrics == 0
    assert summary.passed is True
