from __future__ import annotations

from pathlib import Path
from typing import cast
from uuid import uuid4

import psycopg
import pytest

import warehouse_pipeline.transform.warehouse_build as mod  # simplified path
from warehouse_pipeline.transform.sql_plan import SqlPlan


class DummyConn:
    """Fake Postgres connector for unit test."""

    def __init__(self) -> None:
        """Store mock calls."""
        self.commit_calls = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        """Increase mock calls."""
        self.commit_calls += 1

    def rollback(self) -> None:
        """Increase mock rollback calls"""
        self.rollback_calls += 1


def test_build_warehouse_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Test if `warehouse_build` appears to process SQL as expected."""
    # uses "build_dims" as the example build.

    file_names = (  # dims
        "100_dim_customer.sql",
        "110_dim_date.sql",
    )
    paths = tuple(tmp_path / name for name in file_names)
    for path in paths:
        # real sql.
        path.write_text("SELECT 1;", encoding="utf-8")

    plan = SqlPlan(
        step_name="build_dims",
        sql_dir=tmp_path,
        file_names=file_names,
        paths=paths,
    )

    seen: list[tuple[str, dict[str, object]]] = []

    def fake_run_sql_file(conn: object, path: Path, params: dict[str, object]) -> None:
        """Appends each file seen to list."""
        seen.append((path.name, dict(params)))

    monkeypatch.setattr(mod, "resolve_sql_plan", lambda **kwargs: plan)  # just give plan
    monkeypatch.setattr(
        mod, "_run_sql_file", fake_run_sql_file
    )  # seen the file so assume could the read file, for unit

    conn = DummyConn()  # mock.
    conn = cast(psycopg.Connection[tuple], DummyConn())  # cast mock to `Connection`
    run_id = uuid4()

    result = mod.build_warehouse(
        conn,
        step_name="build_dims",
        run_id=run_id,
    )

    assert [name for name, _ in seen] == list(
        file_names
    )  # all file names MUST have been seen in order
    assert result.files_ran == file_names
    assert result.run_id == run_id  # saw correct one to return it here
