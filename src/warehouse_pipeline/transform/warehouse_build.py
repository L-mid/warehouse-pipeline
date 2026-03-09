from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.transform.sql_plan import SqlPlan, TransformStep, resolve_sql_plan


@dataclass(frozen=True)
class BuildRunIds:
    """Store `run_ids` per kind of run."""
    customers_run_id: UUID
    orders_run_id: UUID
    order_items_run_id: UUID


@dataclass(frozen=True)
class WarehouseBuildResult:
    """Store results of transforms to return as summary later."""
    step_name: TransformStep
    files_ran: tuple[str, ...]
    run_ids: BuildRunIds



def latest_succeeded_run_id(conn: Connection, table_name: str) -> UUID:
    """Fetches the latest succeeded `run_id` for use in transforms."""
    row = conn.execute(
        """
        SELECT run_id
        FROM run_ledger
        WHERE table_name = %s AND status = 'succeeded'  
        ORDER BY started_at DESC
        LIMIT 1
        """,            # inferance, built transform sql this way
        (table_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"No succeeded `run_id` found for `table_name`={table_name}")
    return row[0]


def _resolve_run_ids(
    conn: Connection,
    *,
    customers_run_id: UUID | None,
    orders_run_id: UUID | None,
    order_items_run_id: UUID | None,
) -> BuildRunIds:
    """
    Resolve final `run_ids`.
    """
    return BuildRunIds(
        customers_run_id=customers_run_id,
        orders_run_id=orders_run_id,
        order_items_run_id=order_items_run_id,
    )




def _run_sql_file(conn: Connection, path: Path, params: dict) -> None:
    """
    Read and execute a `.sql` file. (for warehouse building transforms).
    Does NOT commit within this function after being called.
    """
    sql = path.read_text(encoding="utf-8")
    
    # psycopg can execute multi-statement scripts via `execute` with `conn.execute(sql)`
    # BUT safest is to split it on semicolons, but only if also surfacing failing statements.
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, 1):
            try:
                cur.execute(stmt, params)
            except Exception as e:
                raise RuntimeError(
                    f"Warehouse build failed in {path} on statement #{i}\n"
                    f"Postgres raised: {e}\n--- statement ---\n{stmt}\n--- end ---\n"
                ) from e
            

def build_warehouse(
    conn: Connection,
    *,
    step_name: TransformStep = "build_all",
    sql_dir: Path | None = None,
    customers_run_id: UUID | None = None,
    orders_run_id: UUID | None = None,
    order_items_run_id: UUID | None = None,
) -> WarehouseBuildResult:
    """
    Builds the dim/fact tables from staging using sql.

    Uses only the latest succeeded run for each staging table.

    Returns: A `dict` of all respectively run sql transformations to their found `run_id`.
    """

    plan: SqlPlan = resolve_sql_plan(step_name=step_name, sql_dir=sql_dir)
    run_ids = _resolve_run_ids(
        conn,
        customers_run_id=customers_run_id,
        orders_run_id=orders_run_id,
        order_items_run_id=order_items_run_id,
    )
 
    # paramaterizes using most recent `run_id`
    params: dict[str, object] = {
        "customers_run_id": run_ids.customers_run_id,
        "orders_run_id": run_ids.orders_run_id,
        "order_items_run_id": run_ids.order_items_run_id,
    }


    # only one transaction per build it's all or nothing
    try:
        for path in plan.paths:
            _run_sql_file(conn, path, params)       
        # commit ONLY on success
        conn.commit()
    except Exception:
        # no retries or partial builds
        conn.rollback()
        raise

    return WarehouseBuildResult(
        step_name=plan.step_name,
        files_ran=plan.file_names,
        run_ids=run_ids,
    )