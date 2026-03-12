from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _unit_tests_never_use_real_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Hard fails if anything calls `connect()` to try and access postgres in unit tests.

    Autoused by all tests under `tests/unit/` and works by
    modifying the correct enviroment variables to invalid forms.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://invalid/invalid")
    monkeypatch.delenv("WAREHOUSE_TEST_DSN", raising=False)


# could potentally fixutre the http response handlers
