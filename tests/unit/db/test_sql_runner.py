from __future__ import annotations

from typing import cast

import psycopg

from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.sql_runner import run_sql_text


def test_run_sql_text_happy_path() -> None:
    """All statements presented run and show good automic savepoint behaviour."""
    fake_conn = FakeConnection()
    conn = cast(psycopg.Connection[tuple], fake_conn)

    run_sql_text(
        conn,
        # basic sql commands
        sql_text="""
        SELECT 1;
        SELECT 2;
        """,
        source="test.sql",
    )

    # counts as executed
    executed = [call for call in fake_conn.calls if call[0] == "cursor.execute"]

    assert len(executed) == 4
    assert "SAVEPOINT" in str(executed[0][1])
    assert "SELECT 1" in str(executed[1][1])
    assert "SELECT 2" in str(executed[2][1])
    assert "RELEASE SAVEPOINT" in str(executed[3][1])
