from __future__ import annotations

from pathlib import Path
from typing import cast
from uuid import uuid4

import psycopg
import pytest

import warehouse_pipeline.transform.warehouse_build as mod
from warehouse_pipeline.transform.sql_plan import SqlPlan


class DummyConn:
    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


def test_build_warehouse_happy_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file_names = (
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    )
    paths = tuple(tmp_path / name for name in file_names)
    for path in paths:
        path.write_text("SELECT 1;", encoding="utf-8")

    plan = SqlPlan(
        step_name="build_facts",
        sql_dir=tmp_path,
        file_names=file_names,
        paths=paths,
    )

    seen: list[tuple[str, dict[str, object]]] = []

    def fake_run_sql_file(conn: object, path: Path, params: dict[str, object]) -> None:
        seen.append((path.name, dict(params)))

    monkeypatch.setattr(mod, "resolve_sql_plan", lambda **kwargs: plan)
    monkeypatch.setattr(mod, "_run_sql_file", fake_run_sql_file)

    run_id = uuid4()
    conn = cast(psycopg.Connection[tuple], DummyConn())

    result = mod.build_warehouse(
        conn,
        run_id=run_id,
        step_name="build_facts",
    )

    assert [name for name, _ in seen] == list(file_names)
    assert all(params == {"run_id": run_id} for _, params in seen)
    assert result.step_name == "build_facts"
    assert result.files_ran == file_names
    assert result.run_id == run_id
