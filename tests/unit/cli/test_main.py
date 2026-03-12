from __future__ import annotations

import importlib

import warehouse_pipeline.cli.commands.run as run_cmd

main_mod = importlib.import_module("warehouse_pipeline.cli.main")


def test_main_happy_path(monkeypatch) -> None:
    """Tests main calls `handle_run()` and outputs its `int` as its `rc`."""

    def fake_handle_run(args) -> int:
        """Return 7 as `int` to esnure `main()`'s return code forwards it."""
        assert args.mode == "snapshot"
        return 7

    monkeypatch.setattr(run_cmd, "handle_run", fake_handle_run)

    rc = main_mod.main(["run", "--mode", "snapshot"])
    assert rc == 7  # 7.
