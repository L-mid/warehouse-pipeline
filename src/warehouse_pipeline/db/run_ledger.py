from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb

RunStatus = Literal["running", "succeeded", "failed"]  # injected in
RunMode = Literal["snapshot", "live"]


@dataclass(frozen=True)
class RunStart:
    """
    Inputs needed to create a new pipeline run row.
    """

    mode: RunMode
    source_system: str = "dummyjson"
    snapshot_key: str | None = None
    git_sha: str | None = None
    args_json: Mapping[str, Any] | None = None


def create_run(conn: Connection, *, entry: RunStart) -> UUID:
    """
    Inserts a new `run_ledger` row and return its `run_id`.
    """
    row = conn.execute(  # inject in dc
        """ 
        INSERT INTO run_ledger (    
            source_system,
            mode,
            snapshot_key,
            status,
            git_sha,
            args_json
        )
        VALUES (%s, %s, %s, 'running', %s, %s)
        RETURNING run_id
        """,
        (
            entry.source_system,
            entry.mode,
            entry.snapshot_key,
            entry.git_sha,
            Jsonb(dict(entry.args_json or {})),
        ),
    ).fetchone()

    assert row is not None  # fix this assert
    return row[0]  # return only `run_id`


def set_run_status(
    conn: Connection,
    *,
    run_id: UUID,
    status: RunStatus,
    error_message: str | None = None,
    set_finished_at: bool = False,  # optionally log when the run was finished
) -> None:
    """
    Updates run status and error information.
    """
    if set_finished_at:
        conn.execute(
            """
            UPDATE run_ledger
            SET
                status = %s,
                error_message = %s,
                finished_at = now()
            WHERE run_id = %s
            """,
            (status, error_message, run_id),
        )
    else:
        # no `set_finished_at`
        conn.execute(
            """
            UPDATE run_ledger
            SET
                status = %s,
                error_message = %s
            WHERE run_id = %s
            """,
            (status, error_message, run_id),
        )


# api
def mark_run_succeeded(conn: Connection, *, run_id: UUID) -> None:
    """Mark a run as `"succeeded"`, and set `finished_at`."""
    set_run_status(
        conn, run_id=run_id, status="succeeded", error_message=None, set_finished_at=True
    )


def mark_run_failed(conn: Connection, *, run_id: UUID, error_message: str) -> None:
    """Mark a run as `"failed"`, and set `finished_at`."""
    set_run_status(
        conn, run_id=run_id, status="failed", error_message=error_message, set_finished_at=True
    )
