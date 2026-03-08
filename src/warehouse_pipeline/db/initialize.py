from __future__ import annotations

from pathlib import Path
from typing import Optional


from warehouse_pipeline.db.connect import connect
from warehouse_pipeline.db.sql_runner import run_sql_dir, run_sql_file

 

def _default_schema_dir() -> Path:
    """Return the initallization sql schema directory path as default schema init path."""
    return Path(__file__).resolve().parents[3] / "sql" / "schema"



def initialize_database(*, sql_path: Optional[Path] = None, database_url: Optional[str] = None) -> None:
    """
    Read a provided SQL schema init path to initalize (or re-initalize) this DB.
    
    - If `sql_path` is a dir, run all `*sql` files in sorted to ASC order.
    - If `sql_path` is just one file, it will run just that file.

    Commits once at the end if all SQL fully succeeds,
    and rolls back fully if anything fails.
    """

    target = sql_path or _default_schema_dir()  # optionally provide custom path


    with connect(database_url) as conn:
        try:
            # dir logic
            if sql_path.is_dir():       
                run_sql_dir(conn, target)
            # just run one file
            else:
                run_sql_file(conn, target)

            conn.commit()
        except Exception:
            conn.rollback()
            raise
    