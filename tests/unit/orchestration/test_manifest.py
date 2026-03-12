from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

from warehouse_pipeline.orchestration.contract import RunManifest
from warehouse_pipeline.orchestration.manifest import write_manifest


def test_write_manifest_happy_path(tmp_path) -> None:
    # random `run_id``
    run_id = UUID("00000000-0000-0000-0000-000000000333")

    manifest = RunManifest(
        # give it everything
        run_id=run_id,
        mode="snapshot",
        status="succeeded",
        source_system="dummyjson",
        snapshot_key="smoke",
        started_at=datetime(2026, 3, 9, 12, 0, tzinfo=UTC),
        finished_at=datetime(2026, 3, 9, 12, 1, tzinfo=UTC),
        extract={"counts": {"users": 1, "products": 1, "carts": 1}},
        stage={"stg_customers": {"inserted_count": 1}},
        dq={"stg_customers": {"metrics_written": 3, "failed_metrics": 0, "passed": True}},
        gate={"passed": True, "failures": [], "warnings": []},
        transform={"files_ran": ["100_dim_customer.sql"]},
        publish={"files_ran": ["900_views.sql"]},
        timings_s={"extract": 0.1},
        artifacts={"logs": "runs/x/logs.jsonl"},
        error_message=None,
    )

    path = write_manifest(run_dir=tmp_path / "run-1", manifest=manifest)

    # load it up
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert path.name == "manifest.json"
    assert payload["run_id"] == str(run_id)
    assert payload["status"] == "succeeded"
    assert payload["extract"]["counts"]["users"] == 1  # specific nested example
