from __future__ import annotations

import argparse
from pathlib import Path

import warehouse_pipeline.cli.commands.db as db_cmd


def test_handle_db_happy_path(monkeypatch, capsys) -> None:
    """Makes sure initalize db runs its course correctly."""

    seen: dict[str, Path] = {}

    def fake_initialize_database(*, sql_path: Path, database_url=None) -> None:
        """Hash any seen sql path for later."""
        seen["sql_path"] = sql_path

    monkeypatch.setattr(db_cmd, "initialize_database", fake_initialize_database)

    args = argparse.Namespace(sql="sql/schema")
    rc = db_cmd.handle_db_init(args)

    out = capsys.readouterr().out
    assert rc == 0
    assert seen["sql_path"] == Path("sql/schema")
    assert "Initialized schema from sql/schema" in out