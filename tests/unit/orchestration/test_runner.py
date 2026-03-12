from __future__ import annotations

from uuid import UUID

import warehouse_pipeline.orchestration.runner as runner_mod
from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.dq.gates import GateDecision
from warehouse_pipeline.dq.runner import DQRunSummary
from warehouse_pipeline.extract.bundles import ExtractBundle
from warehouse_pipeline.orchestration.contract import RunSpec
from warehouse_pipeline.publish.views import PublishResult
from warehouse_pipeline.stage import MappedCarts, MappedProducts, MappedUsers, StageTableLoadResult
from warehouse_pipeline.transform.warehouse_build import WarehouseBuildResult


def test_run_pipeline_happy_path(tmp_path, monkeypatch) -> None:
    """
    Unit, we mock everything,
    and make sure that it produces a `RunManifest` return that contains expected.
    """

    conn = FakeConnection()  # mock
    # random `run_id`
    run_id = UUID("00000000-0000-0000-0000-000000000444")

    # everything that should be called goes in here
    # note: order indifferent (might want to fix that)
    seen: dict[str, object] = {}

    ## -- The patching.

    # connect, make run
    monkeypatch.setattr(runner_mod, "connect", lambda database_url=None: conn)

    monkeypatch.setattr(runner_mod, "create_run", lambda got_conn, entry: run_id)

    # markers
    monkeypatch.setattr(
        runner_mod,
        "mark_run_succeeded",
        lambda got_conn, *, run_id: seen.setdefault("marked_succeeded", run_id),
    )

    monkeypatch.setattr(
        runner_mod,
        "mark_run_failed",
        lambda got_conn, *, run_id, error_message: seen.setdefault("marked_failed", run_id),
    )

    # extraction
    monkeypatch.setattr(
        runner_mod,
        "read_snapshot_bundle",
        lambda *, snapshot_root, snapshot_key=None: ExtractBundle(
            mode="snapshot",
            snapshot_key=snapshot_key,
            users=(),
            products=(),
            carts=(),
            source_paths={},
            totals={"users": 0, "products": 0, "carts": 0},
            pages_fetched={"users": 1, "products": 1, "carts": 1},
            page_size=None,
        ),
    )

    # map to staging
    monkeypatch.setattr(runner_mod, "map_users", lambda users: MappedUsers())

    monkeypatch.setattr(runner_mod, "map_products", lambda products: MappedProducts())

    monkeypatch.setattr(runner_mod, "map_carts", lambda carts, **kwargs: MappedCarts())

    # load le stuffs
    # example used is `stg_customers` ONLY
    monkeypatch.setattr(
        runner_mod,
        "load_mapped_batches",
        lambda conn, *, run_id, users, products, carts: {
            "stg_customers": StageTableLoadResult(
                table_name="stg_customers",
                inserted_count=1,
                duplicate_reject_count=0,
                explicit_reject_count=0,
            )
        },
    )

    def fake_run_stage_dq(conn, *, run_id):
        """Update seen's dq call check to true and return a mock `DQRunSummary`."""
        seen["dq_called"] = True
        return (
            DQRunSummary(
                run_id=run_id,
                table_name="stg_customers",
                metrics_written=3,
                failed_metrics=0,
                passed=True,
            ),
        )

    monkeypatch.setattr(runner_mod, "run_stage_dq", fake_run_stage_dq)

    # dq gates
    monkeypatch.setattr(
        runner_mod,
        "evaluate_stage_gates",
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
        "build_warehouse",
        lambda conn, *, run_id, step_name: WarehouseBuildResult(
            step_name=step_name,
            files_ran=("100_dim_customer.sql",),
            run_id=run_id,
        ),
    )

    monkeypatch.setattr(
        runner_mod,
        "apply_views",
        lambda conn: PublishResult(
            files_ran=("900_views.sql",),
            metrics_available=("010_revenue_by_day_country",),
        ),
    )

    # mock `RunSpec` to input.
    spec = RunSpec(
        mode="snapshot",
        snapshot_key="smoke",
        snapshot_root=tmp_path / "snapshots" / "smoke",
        runs_root=tmp_path / "runs",
    )

    manifest = runner_mod.run_pipeline(
        spec, database_url="postgresql://unit-test"
    )  # custom mock transaction

    assert manifest.status == "succeeded"
    assert seen["dq_called"] is True
    assert seen["marked_succeeded"] == run_id
    assert manifest.dq["stg_customers"]["metrics_written"] == 3
    assert manifest.publish["files_ran"] == ["900_views.sql"]
    assert (tmp_path / "runs" / str(run_id) / "manifest.json").exists()
    assert conn.commit_calls == 5
