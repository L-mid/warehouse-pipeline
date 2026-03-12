from __future__ import annotations

from decimal import Decimal
from typing import cast
from uuid import uuid4

import psycopg

import warehouse_pipeline.dq.runner as runner
from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.dq_results import DQMetricRow


def test_dq_runner_happy_path(monkeypatch) -> None:
    """Tests data quality runner rows run sucessfully."""
    conn = FakeConnection()
    conn = cast(psycopg.Connection[tuple], conn)
    run_id = uuid4()

    metric_rows = [
        # three rows.
        DQMetricRow(
            run_id=run_id,
            table_name="stg_orders",
            check_name="stage_volume",
            metric_name="row_count",
            metric_value=Decimal("1.000000"),
            passed=True,
            details_json={"row_count": 1},
        ),
        DQMetricRow(
            run_id=run_id,
            table_name="stg_orders",
            check_name="stage_keys",
            metric_name="duplicate_keys.count",
            metric_value=Decimal("0.000000"),
            passed=True,
            details_json={"duplicate_key_groups": 0},
        ),
        DQMetricRow(
            run_id=run_id,
            table_name="stg_orders",
            check_name="stage_relations",
            metric_name="missing_customers.count",
            metric_value=Decimal("0.000000"),
            passed=True,
            details_json={"missing_rows": 0},
        ),
    ]

    deleted: list[tuple[object, str]] = []
    upserted: list[DQMetricRow] = []

    def fake_ensure_run_exists(conn, *, run_id) -> None:
        """Takes params and returns `None`."""
        return None

    def fake_build_metrics_for_table(conn, *, table_name: str, run_id):
        """
        Takes `table_name`, ensures it's for stg_orders,
        returns the pre-defined metric rows.
        """
        assert table_name == "stg_orders"
        return metric_rows

    def fake_delete_dq_results(conn, *, run_id, table_name: str) -> None:
        """Delete mock appended dq results for upsert."""
        deleted.append((run_id, table_name))

    def fake_upsert_dq_results(conn, *, rows) -> int:
        """Upsert `rows` into mock extended list, not real DB."""
        materialized = list(rows)
        upserted.extend(materialized)
        return len(materialized)

    #
    monkeypatch.setattr(runner, "_ensure_run_exists", fake_ensure_run_exists)
    monkeypatch.setattr(runner, "_build_metrics_for_table", fake_build_metrics_for_table)
    monkeypatch.setattr(runner, "delete_dq_results", fake_delete_dq_results)
    monkeypatch.setattr(runner, "upsert_dq_results", fake_upsert_dq_results)

    # run summary
    summary = runner.run_table_dq(conn, run_id=run_id, table_name="stg_orders")

    assert deleted == [(run_id, "stg_orders")]  # make sure they were deleted
    assert len(upserted) == 3  # only the three rows provided got inserted

    assert summary.run_id == run_id
    assert summary.table_name == "stg_orders"
    assert summary.metrics_written == 3
    assert summary.failed_metrics == 0
    assert summary.passed is True
