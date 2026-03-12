from __future__ import annotations

# fake connectors and psycopg integration for unit.
# could merge with global unit the but we'll see
from dataclasses import dataclass
from typing import Any


@dataclass
class FakeResult:
    """Store and fetch rows as results."""

    row: Any = None

    def fetchone(self) -> Any:
        """Fetch row."""
        return self.row


class FakeCursor:
    """Dummy curser mocking `psycopg`'s cursor functionality."""

    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn

    def __enter__(self) -> FakeCursor:
        """Provide calling access."""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Consume error info and exit as `False`."""
        return False

    def execute(self, query: Any, params: Any = None) -> None:
        """Mock an execution and store attempt."""
        self.conn.calls.append(("cursor.execute", query, params))

    def executemany(self, query: Any, params_seq: Any) -> None:
        """Mock executions and store all attempts at once."""
        params_list = list(params_seq)
        self.conn.calls.append(("cursor.executemany", query, params_list))


class FakeConnection:
    """Mock full `psycopg` connection by storing calls, rows, and call counts."""

    def __init__(self, *, fetchone_rows: list[Any] | None = None) -> None:
        self.calls: list[tuple[str, Any, Any]] = []
        self.fetchone_rows = list(fetchone_rows or [])
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0

    def __enter__(self) -> FakeConnection:
        """Provide calling access."""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Consume error info and exit as `False`."""
        return False

    def cursor(self) -> FakeCursor:
        """
        Return the mocked `FakeCursor` implementation 
        as the replacement `cursor` function for `psycopg`.
        """
        return FakeCursor(self)

    def execute(self, query: Any, params: Any = None) -> FakeResult:
        """Mock an execution and return a `FakeResult` row."""
        self.calls.append(("conn.execute", query, params))
        row = self.fetchone_rows.pop(0) if self.fetchone_rows else None
        return FakeResult(row)

    def commit(self) -> None:
        """Increase `self.commit_calls` by one."""
        self.commit_calls += 1

    def rollback(self) -> None:
        """Increase `self.rollback_calls` by one."""
        self.rollback_calls += 1

    def close(self) -> None:
        """Increase `self.close_calls` by one."""
        self.close_calls += 1
