from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Iterator

import psycopg
import pytest


def _find_repo_root(start: Path) -> Path:
    marker = "pyproject.toml"
    cur = start.resolve()
    for p in (cur, *cur.parents):
        if(p / marker).exists():
            return p
    raise RuntimeError(f"Could not find repo root from: {start}")


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """The absolute path to the repo root (finds by walking up to `pyproject.toml`)."""
    # walking starts from the callers location (`tests/conftest.py`)
    return _find_repo_root(Path(__file__))


# for avoiding schema drift: re-init tables fresh each test run.
TRUNCATE_ALL = """
TRUNCATE TABLE
  fact_order_items,
  fact_orders,
  dim_date,
  dim_customer,
  dq_results,
  reject_rows,
  stg_order_items,
  stg_orders,
  stg_retail_transactions,
  stg_customers,
  ingest_runs
RESTART IDENTITY CASCADE;
"""

DROP_ALL = """
DROP TABLE IF EXISTS
  fact_order_items,
  fact_orders,
  dim_date,
  dim_customer,
  dq_results,
  reject_rows,
  stg_order_items,
  stg_orders,
  stg_retail_transactions,
  stg_customers,
  ingest_runs
CASCADE;
""" 

def _run_sql_file(conn: psycopg.Connection, sql_path: Path) -> None:
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
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise
    finally:
        # clean up on test fail.
        try:
            subprocess.run(
                ["docker", "compose", "down", "-v"],
                repo_root,
                check=False,
                text=True,
                capture_output=True,
            )
        except Exception:
            pass


@pytest.fixture(scope="session")
def dsn() -> str:
    """Returns docker enviroment for tests."""
    # CLI/runtime uses WAREHOUSE_DSN. 
    # tests have WAREHOUSE_TEST_DSN set.
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
    raise RuntimeError(f"DB not ready: {dsn}. Last error: {last_err}\n\ncompose ps:\n{ps}\n\ndb logs:\n{logs}")


@pytest.fixture()
def conn(wait_for_db: None, dsn: str, repo_root: Path) -> Iterator[psycopg.Connection]:
    """One transaction per test, commit on success, rollback on failure. Return a `psycopg` connection."""
    with psycopg.connect(dsn) as c:

        # initialize the schema ONLY once per test session
        with c.cursor() as cur:
            cur.execute(DROP_ALL)   # for now only, stops 'IF NOT EXISTS' schema drift. reconsider once schema is finished
        c.commit()
        # explictly ordered running of sql inits.
        _run_sql_file(c, repo_root / "sql" / "000_init.sql")
        _run_sql_file(c, repo_root / "sql" / "020_dim_customer.sql")
        _run_sql_file(c, repo_root / "sql" / "021_dim_date.sql")
        _run_sql_file(c, repo_root / "sql" / "022_fact_orders.sql")
        _run_sql_file(c, repo_root / "sql" / "023_fact_order_items.sql")
        _run_sql_file(c, repo_root / "sql" / "900_views.sql")

        yield c     # the DB connection



@pytest.fixture(autouse=True)       # autoused in every test!
def _truncate_before_each_test(conn: psycopg.Connection) -> None:
    """
    Automatically clear all table rows before running a new test.
    Tables and schema remain existing, only rows are cleared.

    This fixture does not delete tables and schema on its own after use.
    """
    with conn.cursor() as cur:
        cur.execute(TRUNCATE_ALL)
    conn.commit()


# later: make_run(), maybe truncate_tables()


