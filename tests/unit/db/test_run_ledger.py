from __future__ import annotations

from typing import cast
from uuid import uuid4

import psycopg

from tests.unit.db.mocks import FakeConnection
from warehouse_pipeline.db.run_ledger import RunStart, create_run, mark_run_succeeded


def test_run_ledger_happy_path() -> None:
    """The run ledger attempts an insertation (status change) and an update"""

    expected_run_id = uuid4()
    fake_conn = FakeConnection(fetchone_rows=[(expected_run_id,)])
    conn = cast(psycopg.Connection[tuple], fake_conn)

    run_id = create_run(
        conn,
        # mock info as data to create
        entry=RunStart(
            mode="snapshot",
            source_system="dummyjson",
            snapshot_key="dummyjson/v1",
            git_sha="abc123",
            args_json={"page_size": 100},
        ),
    )
    # attempt to make a status change
    mark_run_succeeded(conn, run_id=run_id)

    assert run_id == expected_run_id

    calls = [
        call for call in fake_conn.calls if call[0] == "conn.execute"
    ]  # no other random call site
    # on creation
    assert "INSERT INTO run_ledger" in str(calls[0][1])
    # on status change
    assert "UPDATE run_ledger" in str(calls[1][1])
