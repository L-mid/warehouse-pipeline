from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import warehouse_pipeline.orchestration.runner as runner_mod
from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.dq.gates import GateDecision
from warehouse_pipeline.dq.runner import DQRunSummary
from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.orchestration.contract import RunSpec
from warehouse_pipeline.publish.views import PublishResult
from warehouse_pipeline.stage import MappedSquareOrders, StageRow, StageTableLoadResult
from warehouse_pipeline.transform.warehouse_build import WarehouseBuildResult


def test_run_pipeline_snapshot_happy_path(tmp_path, monkeypatch) -> None:
    conn = FakeConnection()
    run_id = UUID("00000000-0000-0000-0000-000000000444")
    seen: dict[str, object] = {}

    times = iter(
        [
            datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
            datetime(2026, 3, 19, 9, 1, tzinfo=UTC),
        ]
    )

    monkeypatch.setattr(runner_mod, "_utcnow", lambda: next(times))
    monkeypatch.setattr(runner_mod, "connect", lambda database_url=None: conn)
    monkeypatch.setattr(runner_mod, "create_run", lambda got_conn, entry: run_id)

    monkeypatch.setattr(
        runner_mod,
        "mark_run_succeeded",
        lambda got_conn, *, run_id: seen.setdefault("marked_succeeded", run_id),
    )
    monkeypatch.setattr(
        runner_mod,
        "mark_run_failed",
        lambda got_conn, *, run_id, error_message: seen.setdefault("marked_failed", error_message),
    )

    monkeypatch.setattr(
        runner_mod,
        "read_snapshot_extract",
        lambda *, snapshot_root, snapshot_key=None: RawExtract(
            source_system="square_orders",
            mode="snapshot",
            snapshot_key=snapshot_key,
            entities={"orders": ({"id": "ord-100"},)},
            source_paths={},
            totals={"orders": 1},
            pages_fetched={"orders": 1},
            page_size=None,
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "map_square_orders",
        lambda orders: MappedSquareOrders(
            order_rows=[
                StageRow(
                    table_name="stg_square_orders",
                    source_ref=1,
                    raw_payload={"id": "ord-100"},
                    values={"order_id": "ord-100"},
                )
            ],
            order_line_rows=[],
            tender_rows=[],
            rejects=[],
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "load_square_batches",
        lambda conn, *, run_id, square: {
            "stg_square_orders": StageTableLoadResult(
                table_name="stg_square_orders",
                inserted_count=1,
                duplicate_reject_count=0,
                explicit_reject_count=0,
            )
        },
    )

    monkeypatch.setattr(
        runner_mod,
        "build_warehouse",
        lambda conn, *, run_id, step_name: WarehouseBuildResult(
            step_name=step_name,
            files_ran=(
                "100_fact_orders.sql",
                "110_fact_order_lines.sql",
                "120_fact_order_tenders.sql",
            ),
            run_id=run_id,
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "apply_views",
        lambda conn: PublishResult(
            files_ran=("900_views.sql",),
            metrics_available=(
                "010_daily_sales_summary",
                "020_daily_sales_by_tender_type",
                "030_weekly_top_items",
                "040_daily_discount_summary",
                "050_daily_order_state_summary",
            ),
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "run_model_dq",
        lambda conn, *, run_id: (
            DQRunSummary(
                run_id=run_id,
                table_name="stg_square_orders",
                metrics_written=3,
                failed_metrics=0,
                passed=True,
            ),
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "evaluate_model_gates",
        lambda conn, *, run_id: GateDecision(
            run_id=run_id,
            mode="snapshot",
            passed=True,
            failures=(),
            warnings=(),
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "render_dq_summary",
        lambda conn, *, run_id, decision: "DQ PASS: 0 hard failure(s), 0 warning(s)",
    )

    spec = RunSpec(
        mode="snapshot",
        source_system="square_orders",
        snapshot_key="smoke_v1",
        snapshot_root=tmp_path / "snapshots" / "smoke_v1",
        runs_root=tmp_path / "runs",
    )

    manifest = runner_mod.run_pipeline(spec, database_url="postgresql://unit-test")

    assert manifest.status == "succeeded"
    assert seen["marked_succeeded"] == run_id

    assert manifest.extract["counts"] == {"orders": 1}
    assert manifest.stage["stg_square_orders"]["inserted_count"] == 1

    assert manifest.transform["files_ran"] == [
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    ]
    assert manifest.publish["metrics_available"] == [
        "010_daily_sales_summary",
        "020_daily_sales_by_tender_type",
        "030_weekly_top_items",
        "040_daily_discount_summary",
        "050_daily_order_state_summary",
    ]

    assert manifest.gate["passed"] is True
    assert manifest.gate["summary_text"] == "DQ PASS: 0 hard failure(s), 0 warning(s)"

    run_dir = tmp_path / "runs" / str(run_id)
    assert (run_dir / "manifest.json").exists()
    assert (run_dir / "logs.jsonl").exists()

    assert conn.commit_calls == 5
