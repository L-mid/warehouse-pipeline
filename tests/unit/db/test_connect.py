from __future__ import annotations
import importlib

connect_mod = importlib.import_module("warehouse_pipeline.db.connect")

 
def test_connect_happy_path(monkeypatch) -> None:
    """Connect actually uses provided url for `psycopg`."""

    sentinel = object()         # unique fake "connection" to return
    called: dict[str, str] = {}

    def fake_psycopg_connect(url: str, *, autocommit: bool = False):
        """Collect url in hash and return `sentinel` object."""
        called["url"] = url
        called["autocommit"] = autocommit
        return sentinel


    # set fake env url
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://example/testdb")
    monkeypatch.setattr(connect_mod.psycopg, "connect", fake_psycopg_connect)

    got = connect_mod.connect()


    assert got is sentinel
    assert called["url"] == "postgresql://example/testdb"
    assert called["autocommit"] is False    # default autocommit is false