import os
import time

import psycopg
import pytest


@pytest.fixture(scope="session")
def dsn() -> str:
    """Returns docker enviroment for tests."""
    return os.getenv("WAREHOUSE_TEST_DSN", "postgresql://postgres:postgres@localhost:5433/warehouse")


@pytest.fixture(scope="session")
def db_ready(dsn: str) -> None:
    """Wait until Postgres accepts connection (docker compose up -d should be run before pytest)."""
    deadline = time.time() + 30
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
    raise RuntimeError(f"DB not ready: {dsn}. Last error: {last_err}")

@pytest.fixture()
def conn(db_ready: None, dsn: str):
    """One transaction per test, commit on success, rollback on failure."""
    with psycopg.connect(dsn) as c:
        yield c

# later: make_run(), maybe truncate_tables()