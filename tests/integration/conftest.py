from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Iterator

import psycopg
import pytest



def _run_sql_file(conn: psycopg.Connection, sql_path: Path) -> None:
    """
    Executes an SQL file, surfacing the failing statement index on failure.
    """
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
                    f"SQL failed in {sql_path} on statement #{i}\n"
                    f"Postgres: {e}\n"
                    f"--- statement ---\n{stmt}\n--- end ---\n"
                ) from e
    conn.commit()


def _apply_sql_dir(conn: psycopg.Connection, dir_path: Path) -> None:
    """Reads and runs provided SQL in a directory path. Will execute all files found in ASC order."""
    for p in sorted(dir_path.glob("*.sql")):
        _run_sql_file(conn, p)    


def _drop_public_schema(conn: psycopg.Connection) -> None:
    """
    Drops everything in the public Postgres schema via `DROP SCHEMA`, 
    then creates and commits a new public schema.
    """
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    conn.commit() 



@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Recomputes pathwalk to root directory for integration scope. Uses `pyproject.toml` as its root marker."""
    marker = "pyproject.toml"
    cur = Path(__file__).resolve()      # abs
    for p in (cur, *cur.parents):
        if (p / marker).exists():
            return p
    raise RuntimeError("Could not find repo root for integration tests.")


@pytest.fixture(scope="session", autouse=True)
def docker_db(repo_root: Path) -> Iterator[None]:
    """Docker up and Docker down for Postgres, used in integration tests."""
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d"],
            cwd=repo_root,
            check=True,
            text=True,
            capture_output=True,
        )
        yield
    except subprocess.CalledProcessError as e:
        # reveal debug info from subprocess before crash
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise
    finally:
        # clean up on test fail.
        subprocess.run(
            ["docker", "compose", "down", "-v", "--remove-orphans"],
            cwd=repo_root,
            check=False,
            text=True,
            capture_output=True,
        )


@pytest.fixture(scope="session")
def dsn() -> str:
    """Returns docker enviroment for tests."""
    # The CLI/runtime uses `WAREHOUSE_DSN`. 
    # tests have `WAREHOUSE_TEST_DSN` set.
    return os.getenv("WAREHOUSE_TEST_DSN", "postgresql://postgres:postgres@localhost:5433/warehouse")



@pytest.fixture(scope="session")
def wait_for_db(dsn: str, repo_root: Path) -> None:
    """Wait until Postgres accepts connection. Raise `RuntimeError` on timeout."""
    deadline = time.time() + 10
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return
        except Exception as e:
            last_err = e
            time.sleep(0.2)

    logs = subprocess.run(
        ["docker", "compose", "logs", "--no-color", "db"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    ps = subprocess.run(
        ["docker", "compose", "ps"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    raise RuntimeError(
        f"DB not ready: {dsn}. Last error: {last_err}\n\ncompose ps:\n{ps}\n\ndb logs:\n{logs}"
        )




@pytest.fixture()
def reinit_schema(wait_for_db: None, dsn: str, repo_root: Path) -> None:
    """
    Fully drops and reinitalizes the Postgres schema for each test. 
    """
    # for now only, stops 'IF NOT EXISTS' schema drift failing tests. reconsider once schema is finished
    
    sql_root = repo_root / "sql"
    schema_dir = sql_root / "schema"
    transform_dir = sql_root / "transform"
    publish_dir = sql_root / "publish"  # only top level `*.sql` 

    with psycopg.connect(dsn, autocommit=True) as conn:
        _drop_public_schema(conn)

        # order of ex:
        ## -- schema
        _apply_sql_dir(conn, schema_dir)
        ## -- transform, dim/fact DDL and do build statements
        _apply_sql_dir(conn, transform_dir)
        ## -- publish, views/derived from transform. 
        _apply_sql_dir(conn, publish_dir)



@pytest.fixture()
def conn(reinit_schema: None, dsn: str) -> Iterator[psycopg.Connection]:
    """
    One transaction per test, commit on success, rollback on failure. Return a `psycopg` connection.
    `reinit_schema` handles db reinitalization logic.
    """
    c = psycopg.connect(dsn)
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()