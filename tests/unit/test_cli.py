from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from uuid import UUID

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
        run_id: str
        table_name: str
        input_path: str
        total: str
        loaded: str
        rejected: str
    
        def render_one_line(self) -> str:
            return f"{self.table_name}: total={self.total} loaded={self.loaded} rejected={self.rejected} run_id={self.run_id}"
        
    calls: dict[str, object] = {}


    def fake_load_file(conn: object, *, input_path: Path, table_name: str) -> FakeSummary:
        """Identity loader."""
        calls["conn"] = conn
        calls["input_path"] = input_path
        calls["table_name"] = table_name
        calls["run_id"] = UUID("00000000-0000-0000-0000-000000000001")
        calls["total"] = str(10)
        calls["loaded"] = str(9)
        calls["rejected"] = str(10)

        return FakeSummary(
            calls["run_id"],
            calls["table_name"],
            calls["input_path"],
            calls["total"],
            calls["loaded"],
            calls["rejected"]
        )

    def fake_run_dq(conn: object, *, run_id: object, table_name: str) -> int:
        return 0

    import warehouse_pipeline.cli.main as cli_main

    # patching
    monkeypatch.setattr(cli_main, "connect", fake_connect)
    monkeypatch.setattr(cli_main, "load_file", fake_load_file)
    monkeypatch.setattr(cli_main, "run_dq", fake_run_dq)

    # cmd
    rc = main(["load", "--input", str(customers), "--table", "stg_customers"])
    assert rc == 0

    out = capsys.readouterr().out.strip()
    # prove identity constructs follow through
    assert out == "stg_customers: total=10 loaded=9 rejected=10 run_id=00000000-0000-0000-0000-000000000001" 
    assert calls["table_name"] == "stg_customers"
    assert Path(calls["input_path"]) == customers



