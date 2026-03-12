from __future__ import annotations

import os

import psycopg
from psycopg import Connection

# for runtime/cli
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5433/warehouse"


def get_database_url() -> str:
    """
    Get the main database URL for runtime/CLI usage.

    Tests instead pass a different explicit URL into `connect(...)`
    or by setting `WAREHOUSE_DSN` in their own test environment.
    """
    return os.getenv("WAREHOUSE_DSN", DEFAULT_DSN)


def connect(database_url: str | None = None, *, autocommit: bool = False) -> Connection:
    """
    Open and return a psycopg connection.

    - Uses DSN from `WAREHOUSE_DSN`, falling back to `DEFAULT_DSN`.
    - Leaves autocommit OFF (commits are explicitly managed elsewhere).
    """
    url = database_url or get_database_url()
    return psycopg.connect(url, autocommit=autocommit)
