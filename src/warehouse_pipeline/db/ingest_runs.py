from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from psycopg import Connection


RunStatus = Literal["running", "succeeded", "failed"]   # injected in


@dataclass(frozen=True)
class IngestRun:
    """Base run ingestion information to persist as a ledger."""
    run_id: UUID
    input_path: str         # where to find the table
    table_name: str         # which table this info is being injected into
    status: RunStatus


def insert_ingest_run(conn: Connection, *, input_path: Path, table_name: str) -> UUID:
    """
    Create an `ingest_runs` row, returns `run_id`.

    Committed immediately. The run ledger will persist even if later steps error.
    """
    row = conn.execute(
        """
        INSERT INTO ingest_runs (input_path, table_name, status)
        VALUES (%s, %s, 'running')
        RETURNING run_id
        """,
        (str(input_path), table_name),
    ).fetchone()
    assert row is not None
    return row[0]       # return only `run_id`



def update_ingest_run_status(conn: Connection, *, run_id: UUID, status: RunStatus) -> None:
    """Updates the status of `ingest_runs` given a provided `run_id` and `status`."""
    conn.execute(
        "UPDATE ingest_runs SET status = %s WHERE run_id = %s",
        (status, run_id),
    )