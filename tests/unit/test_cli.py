from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pytest

from warehouse_pipeline.cli.main import main



def test_cli_help_prints_and_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    """CLI is accessible."""
    # Argparse exits via SystemExit for -h
    with pytest.raises(SystemExit) as e:
        main(["-h"])

    assert e.value.code == 0
    out = capsys.readouterr().out
    assert "usage: pipeline" in out
    assert "load" in out
    assert "db" in out


def test_cli_load_happy_path_calls_loader_and_prints_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Test the CLI `load` command works in isolation. 
    Core features are patched to test without Postgres.
    """
    # dummy customers file 
    customers = tmp_path / "customers.csv"
    customers.write_text("customer_id,full_name,signup_date\n1,Ada,2026-01-01\n", encoding="utf-8")

    # Patches connect() used by CLI to avoid real DB usage
    @contextmanager
    def fake_connect() -> Iterator[object]:
        """Identity connection."""
        yield object()

    # Patches load_file() used by CLI to avoid real parsing/DB
    @dataclass(frozen=True)
    class FakeSummary:
        """Identity summary fields."""
        line: str

        def render_one_line(self) -> str:
            return self.line
        
    calls: dict[str, object] = {}


    def fake_load_file(conn: object, *, input_path: Path, table_name: str) -> FakeSummary:
        """Identity loader."""
        calls["conn"] = conn
        calls["input_path"] = input_path
        calls["table_name"] = table_name
        return FakeSummary("stg_customers: total=1 loaded=1 rejected=0 run_id=TEST")


    import warehouse_pipeline.cli.main as cli_main

    # patching
    monkeypatch.setattr(cli_main, "connect", fake_connect)
    monkeypatch.setattr(cli_main, "load_file", fake_load_file)

    # cmd
    rc = main(["load", "--input", str(customers), "--table", "stg_customers"])
    assert rc == 0

    out = capsys.readouterr().out.strip()
    assert out == "stg_customers: total=1 loaded=1 rejected=0 run_id=TEST"      # prove identity constructs follow through
    assert calls["table_name"] == "stg_customers"
    assert Path(calls["input_path"]) == customers