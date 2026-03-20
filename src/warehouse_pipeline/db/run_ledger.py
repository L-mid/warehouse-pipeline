from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from psycopg import Connection
from psycopg.types.json import Jsonb

RunStatus = Literal["running", "succeeded", "failed"]  # injected in
RunMode = Literal["snapshot", "live", "incremental"]


@dataclass(frozen=True)
class RunStart:
    """
    Inputs needed to create a new pipeline run row.
    """

    mode: RunMode
    source_system: str = "square_orders"
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


def get_last_successful_watermark(
    conn: Connection,
    *,
    source_system: str,
    watermark_column: str,
) -> datetime | None:
    """
    Return the `watermark_high` from the most recent succeeded
    incremental run for a `source_syste` and `watermark_column`.

    Returns `None` if no prior incremental run has succeeded.
    """
    row = conn.execute(
        """
        SELECT watermark_high
        FROM run_ledger
        WHERE status = 'succeeded'
            AND mode = 'incremental'
            AND source_system = %s
            AND watermark_column = %s
            AND watermark_high IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
        """,
        (source_system, watermark_column),
    ).fetchone()

    return row[0] if row is not None else None


def record_cursor_state(
    conn: Connection,
    *,
    run_id: UUID,
    cursor_state: Mapping[str, Any],
) -> None:
    """
    Persist source-specific cursor metadata for this run in a json col.
    """
    conn.execute(
        """
        UPDATE run_ledger
        SET cursor_state_json = %s
        WHERE run_id = %s
        """,
        (Jsonb(dict(cursor_state)), run_id),
    )


def record_extraction_window(
    conn: Connection,
    *,
    run_id: UUID,
    watermark_column: str,
    watermark_low: datetime,
    watermark_high: datetime,
) -> None:
    """
    Add the resolved extraction window onto this run's ledger row.
    """
    conn.execute(
        """
        UPDATE run_ledger
        SET watermark_column = %s,
            watermark_low    = %s,
            watermark_high   = %s
        WHERE run_id = %s
        """,
        (watermark_column, watermark_low, watermark_high, run_id),
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
