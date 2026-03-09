from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.orchestration.contract import RunSpec


def test_contract_happy_path(tmp_path: Path) -> None:
    """`RunSpec` stores and unpacks a value as expected."""
    snapshot_root = tmp_path / "snapshots" / "smoke"
    spec = RunSpec(
        mode="snapshot",
        snapshot_key="smoke",
        snapshot_root=snapshot_root,
    )

    assert spec.resolved_snapshot_root() == snapshot_root.resolve()