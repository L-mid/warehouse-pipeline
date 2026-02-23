from __future__ import annotations

import os
from typing import Optional

import psycopg
from psycopg import Connection

DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5433/warehouse"



def get_database_url() -> str:
    """Returns docker enviroment."""
    # CLI/runtime uses WAREHOUSE_DSN. 
    # tests have WAREHOUSE_TEST_DSN set.
    return os.getenv("WAREHOUSE_DSN", DEFAULT_DSN)          # will fetch from the env first and formost.


def connect(database_url: Optional[str] = None) -> Connection:
    """
    Return a psycopg connection.

    - Uses `DATABASE_URL`, if not provided earlier.
    - Leaves autocommit OFF (commits explicitly managed elsewhere).
    """
    url = database_url or get_database_url()
    return psycopg.connect(url)


