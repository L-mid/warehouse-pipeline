from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID



import warehouse_pipeline.cli.commands.run as run_cmd


def test_handle_run_happy_path(monkeypatch, capsys) -> None:
    """Returns what it's given as a summary and outputs expected message."""
    seen = {}

    def fake_run_pipeline(spec):
        """Swallow spec and return a mock object that looks like `RunManifest`."""
        # hash it we 'called it'
        seen["spec"] = spec
        
        # mock `RunManifiest`
        return SimpleNamespace(
            run_id=UUID("00000000-0000-0000-0000-000000000111"),
            status="succeeded",
            mode=spec.mode,
            artifacts={"manifest": "/tmp/fake-manifest.json"},
        )

    monkeypatch.setattr(run_cmd, "run_pipeline", fake_run_pipeline)    

    # bare minimum `RunSpec` mock to see if it returns the object presented
    args = argparse.Namespace(
        mode="snapshot",
        snapshot_key="smoke",
        runs_root="tmp-runs",
        page_size=25,
    )

    rc = run_cmd.handle_run(args)

    out = capsys.readouterr().out 

    assert rc == 0
    assert seen["spec"].mode == "snapshot"
    assert seen["spec"].snapshot_key == "smoke"
    assert seen["spec"].runs_root == Path("tmp-runs")
    assert seen["spec"].page_size == 25
    assert "status=succeeded" in out
    assert "manifest=/tmp/fake-manifest.json" in out

   