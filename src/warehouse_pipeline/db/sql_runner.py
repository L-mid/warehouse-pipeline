from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import sqlparse
from psycopg import Connection
import psycopg



@dataclass(frozen=True)
class SqlExecutionError(RuntimeError):
    """
    Raised when an SQL file fails on any given specific statement.

    Contains derived info from parsing. 
    `__str__` returns formatted debugging information.
    """

    source: str
    statement_index: int
    statement: str
    original_error: str

    def __str__(self) -> str:
        return (
            f"SQL execution failed in {self.source} on statement #{self.statement_index}\n"
            f"Postgres raised: {self.original_error}\n"
            f"--- failing statement ---\n"
            f"{self.statement}\n"
            f"--- end failing statement ---"
        )

# better splitter 
def split_sql_statements(sql_text: str) -> list[str]:
    """
    Split a SQL script into individual executable statements.
    """
    # sqlparse 
    return [stmt.strip() for stmt in sqlparse.split(sql_text) if stmt.strip()]



def run_sql_text(conn: Connection, *, sql_text: str, source: str = "<memory>") -> None:
    """
    Run an SQL script atomically at the file/script level.

    If any statement fails:
    - The script is rolled back to its file-level savepoint.
    - an `SqlExecutionError` is raised with the exact failing statement
    """
    statements = split_sql_statements(sql_text)

    if not statements:
        return

    with conn.cursor() as cur:
        cur.execute("SAVEPOINT sqlrunner_file") # for atomic saving

        for idx, statement in enumerate(statements, start=1):
            try:
                cur.execute(statement)
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT sqlrunner_file")
                raise SqlExecutionError(
                    source=source,
                    statement_index=idx,
                    statement=statement,
                    original_error=str(exc),
                ) from exc

        cur.execute("RELEASE SAVEPOINT sqlrunner_file") # remove savepoints


def run_sql_file(conn: Connection, path: Path) -> None:
    """Read and execute one `.sql` file."""
    sql_text = path.read_text(encoding="utf-8")
    run_sql_text(conn, sql_text=sql_text, source=str(path))

def run_sql_files(conn: Connection, paths: Iterable[Path]) -> None:
    """Execute multiple SQL files in the exact provided order."""
    for path in paths:
        run_sql_file(conn, path)

def run_sql_dir(conn: Connection, directory: Path, *, glob: str = "*.sql") -> None:
    """
    Execute all SQL files in a directory in ASC filename order.
    """
    paths = sorted(directory.glob(glob))    # sql only by default
    if not paths:
        raise FileNotFoundError(f"No SQL files matched {glob!r} in {directory}")
    run_sql_files(conn, paths)
