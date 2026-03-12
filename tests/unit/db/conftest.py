from __future__ import annotations

import pytest

from tests.unit.db.mocks import FakeConnection


@pytest.fixture()
def fake_conn() -> FakeConnection:
    """Return a mocked connection for a transaction in unit tests."""
    return FakeConnection()
