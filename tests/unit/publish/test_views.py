from __future__ import annotations

from pathlib import Path
from typing import cast

import psycopg

import warehouse_pipeline.publish.views as views


class _Desc:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn
        self.description = [
            _Desc("business_date"),
            _Desc("completed_order_count"),
            _Desc("total_money"),
        ]

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, query: str, params=None) -> None:
        self.conn.executed_sql.append(query)

    def fetchall(self):
        return [("2026-03-10", 2, "34.00")]


class _FakeConn:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


def test_views_happy_path(tmp_path: Path, monkeypatch) -> None:
    publish_dir = tmp_path / "publish"
    metrics_dir = publish_dir / "metrics"
    publish_dir.mkdir()
    metrics_dir.mkdir()

    (publish_dir / "900_views.sql").write_text(
        "CREATE OR REPLACE VIEW v_demo AS SELECT 1 AS x;",
        encoding="utf-8",
    )

    (metrics_dir / "010_daily_sales_summary.sql").write_text(
        """
        SELECT
          '2026-03-10' AS business_date,
          2 AS completed_order_count,
          '34.00' AS total_money
        """,
        encoding="utf-8",
    )

    conn_fake = _FakeConn()
    conn = cast(psycopg.Connection[tuple], conn_fake)
    called_paths: list[Path] = []

    def fake_run_sql_file(conn, path: Path) -> None:
        called_paths.append(path)

    monkeypatch.setattr(views, "run_sql_file", fake_run_sql_file)
    monkeypatch.setattr(views, "DEFAULT_METRICS_SQL_DIR", metrics_dir)

    publish_result = views.apply_views(conn, sql_dir=publish_dir)

    assert publish_result.files_ran == ("900_views.sql",)
    assert publish_result.metrics_available == ("010_daily_sales_summary",)
    assert called_paths == [publish_dir / "900_views.sql"]

    metric_result = views.run_metric_query(
        conn,
        name="010_daily_sales_summary",
        metrics_dir=metrics_dir,
    )

    assert metric_result.name == "010_daily_sales_summary"
    assert metric_result.columns == (
        "business_date",
        "completed_order_count",
        "total_money",
    )
    assert metric_result.rows == (
        {
            "business_date": "2026-03-10",
            "completed_order_count": 2,
            "total_money": "34.00",
        },
    )
    assert "SELECT" in conn_fake.executed_sql[0]
