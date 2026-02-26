from __future__ import annotations

from warehouse_pipeline.db.connect import connect


from pathlib import Path
import psycopg


 
def _run_sql_file(conn: psycopg.Connection, sql_path: Path) -> None:
    """Read and execute a `.sql` file."""
    sql = sql_path.read_text(encoding="utf-8")

    # psycopg can execute multi-statement scripts via `execute` with `conn.execute(sql)`
    # BUT safest is to split it on semicolons, but only if also surfacing failing statements.
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, 1):
            try:
                cur.execute(stmt)
            except Exception as e:
                raise RuntimeError(
                    f"DB init failed in {sql_path} on statement #{i}\n"
                    f"Postgres raised with: {e}\n"
                    f"--- statement ---\n{stmt}\n--- end ---\n"
                ) from e
    conn.commit()




def db_init(*, sql_path: Path) -> None:
    """
    Read a provided SQL init path to initalize (or re-initalize) this DB.
    
    - If `sql_path` is a dir, run all `*sql` files in sorted to ASC order.
    - If `sql_path` is just one file, it will run just that file.
    """

    with connect() as conn:
        # dir logic
        if sql_path.is_dir():       
            for p in sorted(sql_path.glob("*.sql")):    # ASC.
                _run_sql_file(conn, p) 
        # just run one file
        else:
            _run_sql_file(conn, p)
    