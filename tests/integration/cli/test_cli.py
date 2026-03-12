from __future__ import annotations

import json

import pytest

from warehouse_pipeline.cli.main import main


@pytest.mark.docker_required
def test_cli_happy_path(
    reinit_schema: str, dsn: str, run_artifacts_dir, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    # work a run command through CLI direct
    rc = main(
        [
            "run",
            "--mode",
            "snapshot",
            "--snapshot",
            "smoke",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    out = capsys.readouterr().out
    manifests = list(run_artifacts_dir.glob("*/manifest.json"))

    assert rc == 0
    assert "status=succeeded" in out
    assert len(manifests) == 1

    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert payload["status"] == "succeeded"
    assert payload["extract"]["counts"] == {
        "users": 1,
        "products": 1,
        "carts": 1,
    }  # specific extraction example
