from __future__ import annotations

from pathlib import Path
from typing import cast

import psycopg

import warehouse_pipeline.publish.views as views


class _Desc:
    """Class that can be given a name."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    """FakeCursor stub for views."""

    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn
        self.description = [
            _Desc("country"),
            _Desc("paid_orders"),
            _Desc("refunded_orders"),
        ]

    def __enter__(self) -> _FakeCursor:
        """Return `_FakeCursor`"""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Take debbuging and exit."""
        return False

    def execute(self, query: str, params=None) -> None:
        """Append executed statements to mock."""
        self.conn.executed_sql.append(query)

    def fetchall(self):
        """Fetch row. (pre defined)"""
        return [("UK", 1, 1)]


class _FakeConn:
    """Stub fake connection."""

    def __init__(self) -> None:
        """List for all executed statments."""
        self.executed_sql: list[str] = []

    def cursor(self) -> _FakeCursor:
        """Return `_FakeCursor`."""
        return _FakeCursor(self)


def test_views_happy_path(tmp_path: Path, monkeypatch) -> None:
    """Views creates it's metrics to be viewed."""
    # tmp stuff
    publish_dir = tmp_path / "publish"
    metrics_dir = publish_dir / "metrics"
    publish_dir.mkdir()
    metrics_dir.mkdir()

    (publish_dir / "900_views.sql").write_text(
        "CREATE OR REPLACE VIEW v_demo AS SELECT 1 AS x;",
        encoding="utf-8",
    )

    # `030_paid_vs_refunded_counts` used as example.
    (metrics_dir / "030_paid_vs_refunded_counts.sql").write_text(
        """
        SELECT
          'UK' AS country,
          1 AS paid_orders,
          1 AS refunded_orders
        """,
        encoding="utf-8",
    )

    conn_fake = _FakeConn()
    conn = cast(psycopg.Connection[tuple], conn_fake)
    called_paths: list[Path] = []

    def fake_run_sql_file(conn, path: Path) -> None:
        """Append 'executed' statements."""
        called_paths.append(path)

    monkeypatch.setattr(views, "run_sql_file", fake_run_sql_file)
    monkeypatch.setattr(views, "DEFAULT_METRICS_SQL_DIR", metrics_dir)

    publish_result = views.apply_views(conn, sql_dir=publish_dir)

    assert publish_result.files_ran == ("900_views.sql",)
    assert publish_result.metrics_available == ("030_paid_vs_refunded_counts",)
    assert called_paths == [publish_dir / "900_views.sql"]

    metric_result = views.run_metric_query(
        conn,
        name="030_paid_vs_refunded_counts",
        metrics_dir=metrics_dir,
    )

    assert metric_result.name == "030_paid_vs_refunded_counts"
    assert metric_result.columns == ("country", "paid_orders", "refunded_orders")
    assert metric_result.rows == (
        {
            "country": "UK",
            "paid_orders": 1,
            "refunded_orders": 1,
        },  # exactly as input
    )
    assert "SELECT" in conn_fake.executed_sql[0]  # actually 'ran command'.
