from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import LiteralString, cast
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.transform.sql_plan import SqlPlan, TransformStep, resolve_sql_plan


@dataclass(frozen=True)
class WarehouseBuildResult:
    """Store results of transforms to return as summary later."""

    step_name: TransformStep
    files_ran: tuple[str, ...]
    run_id: UUID


def latest_succeeded_pipeline_run_id(conn: Connection) -> UUID:
    """Gets the latest succeeded pipeline `run_id`."""
    row = conn.execute(
        """
        SELECT run_id
        FROM run_ledger
        WHERE status = 'succeeded'
        ORDER BY finished_at DESC NULLS LAST, started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("No succeeded pipeline `run_id` found in `run_ledger`.")
    return row[0]


def _trusted_sql(stmt: str) -> LiteralString:
    """Mark SQL loaded from repo-owned migration files as trusted for typing."""
    return cast(LiteralString, stmt)  # cast to `LiteralString`


def _run_sql_file(conn: Connection, path: Path, params: dict) -> None:
    """
    Read and execute an `.sql` file. (for warehouse building transforms).
    Does not commit within this function after being called.
    """
    sql = path.read_text(encoding="utf-8")

    # psycopg can execute multi-statement scripts via `execute` with `conn.execute(sql)`
    # BUT safest is to split it on semicolons, but only if also surfacing failing statements.
    # replace this with sqlparse. eventually
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, start=1):
            try:
                cur.execute(_trusted_sql(stmt), params)
            except Exception as e:
                raise RuntimeError(
                    f"Warehouse build failed in {path} on statement #{i}\n"
                    f"Postgres raised: {e}\n--- statement ---\n{stmt}\n--- end ---\n"
                ) from e


def build_warehouse(
    conn: Connection,
    *,
    run_id: UUID,
    step_name: TransformStep = "build_all",
    sql_dir: Path | None = None,
) -> WarehouseBuildResult:
    """
    Builds dimensions and facts from one pipeline run.
    """

    plan: SqlPlan = resolve_sql_plan(step_name=step_name, sql_dir=sql_dir)

    # paramaterizes using most recent `run_id`
    params: dict[str, object] = {"run_id": run_id}

    # there are no transactions here, dealt with by caller
    for path in plan.paths:
        _run_sql_file(conn, path, params)

    return WarehouseBuildResult(
        step_name=plan.step_name,
        files_ran=plan.file_names,
        run_id=run_id,
    )
