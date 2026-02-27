from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from psycopg import Connection


SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "warehouse"     # pointer to the 'warehouse/' dir.

TRANSFORMS = [
    "300_build_dim_customer.sql",
    "310_build_dim_date.sql",
    "320_build_fact_orders.sql",
    "330_build_fact_order_items.sql",
]



def latest_succeeded_run_id(conn: Connection, table_name: str) -> UUID:
    """Fetches the latest succeeded run_id for use in transforms."""
    row = conn.execute(
        """
        SELECT run_id
        FROM ingest_runs
        WHERE table_name = %s AND status = 'succeeded'
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"No succeeded `run_id` found for `table_name`={table_name}")
    return row[0]

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
    customers_run_id: UUID | None = None,
    orders_run_id: UUID | None = None,
    order_items_run_id: UUID | None = None,
) -> dict[str, UUID]:
    """
    Builds the dim/fact tables from staging using sql referenced by `TRANSFORMS`.

    Uses only the latest succeeded run for each staging table.

    Returns: A `dict` of all respectively run sql transformations to their found `run_id`.
    """

    customers_run_id = customers_run_id or latest_succeeded_run_id(conn, "stg_customers")
    orders_run_id = orders_run_id or latest_succeeded_run_id(conn, "stg_orders")
    order_items_run_id = order_items_run_id or latest_succeeded_run_id(conn, "stg_order_items")

    # paramaterizes using most recent `run_id`
    params = {
        "customers_run_id": customers_run_id,
        "orders_run_id": orders_run_id,
        "order_items_run_id": order_items_run_id,
    }

    # only one transaction per build it's all or nothing
    try:
        for fname in TRANSFORMS:
            _run_sql_file(conn, SQL_DIR / fname, params)        
        # commit ONLY on success
        conn.commit()
    except Exception:
        # no retries or partial builds
        conn.rollback()
        raise

    return params