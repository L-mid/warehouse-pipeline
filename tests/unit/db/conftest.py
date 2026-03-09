from __future__ import annotations

from tests.unit.db.mocks import FakeConnection

import pytest

@pytest.fixture()
def fake_conn() -> FakeConnection:
    """Return a mocked connection for a transaction in unit tests."""
    return FakeConnection()
