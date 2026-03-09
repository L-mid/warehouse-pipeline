from __future__ import annotations

from pathlib import Path

import warehouse_pipeline.db.initialize as initialize_mod
from tests.unit.db.mocks import FakeConnection


def test_initialize_happy_path(tmp_path: Path, monkeypatch) -> None:
    """Schema is re-initalized from sql provided in ASC order, and commits when done."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()

    # write sql with commands to execute
    (schema_dir / "000_extensions.sql").write_text("SELECT 1;", encoding="utf-8")
    (schema_dir / "010_tables.sql").write_text("SELECT 2;", encoding="utf-8")


    conn = FakeConnection()
    seen: list[Path] = []  

    def fake_connect(database_url=None):
        """Take real `database_url` and return mock connection `conn`."""
        return conn

    def fake_run_sql_dir(got_conn, directory, glob="*.sql"):
        """Make sure it got the right mock connection, and append any directories seen."""
        assert got_conn is conn
        seen.append(directory)    


    monkeypatch.setattr(initialize_mod, "connect", fake_connect)
    # appends all seen
    monkeypatch.setattr(initialize_mod, "run_sql_dir", fake_run_sql_dir)

    initialize_mod.initialize_database(sql_path=schema_dir)


    assert seen == [schema_dir]         # must return the same directory path.
    assert conn.commit_calls == 1       # it commited only once when done.
    assert conn.rollback_calls == 0     # did not error  