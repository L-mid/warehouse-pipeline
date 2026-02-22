from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.db.connect import connect



def db_init(*, sql_path: Path) -> None:
    """Read a provided SQL init path to initalize (or re-initalize) this DB."""
    sql = sql_path.read_text(encoding="utf-8")

    # psycopg can execute multi-statement scripts via `execute` with `conn.execute(sql)`
    # BUT safest is to split on semicolons, but only if also surfacing failing statements.
    statements = [s.strip() for s in sql.split(";") if s.strip()]

    with connect() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    raise RuntimeError(f"DB init failed on statement:\n{stmt}\n") from e
        conn.commit()

    