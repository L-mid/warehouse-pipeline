from __future__ import annotations

from pathlib import Path

import pytest


def _find_repo_root(start: Path) -> Path:
    """Walks upward until finding `pyproject.toml`. Allows tests to run from anywhere under `tests/`."""
    marker = "pyproject.toml"
    cur = start.resolve()
    for p in (cur, *cur.parents):
        if(p / marker).exists():
            return p
    raise RuntimeError(f"Could not find repo root from: {start}")

@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Returns the absolute path to the repo root (finds by walking up to `pyproject.toml`)."""
    # walking starts from the callers location (`tests/conftest.py`)
    return _find_repo_root(Path(__file__))


@pytest.fixture(scope="session")
def dummyjson_snapshots_dir(repo_root: Path) -> Path:
    """
    Path from root to the dummyjson ingested data snapshots directory. 
    `data/snapshots/dummyjson/{v1|smoke}/...`
    Uses `repo_root` (`pyproject.toml`) placement as its base.
    """
    return repo_root / "data" / "snapshots" / "dummyjson"


@pytest.fixture()
def run_artifacts_dir(tmp_path: Path) -> Path:
    """
    Returns temporary directory `<tmp_path>/runs/` for runs in tests.
    Easy isolated artifacts dir for each individual test.
    """
    d = tmp_path / "runs"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture()
def fixed_run_id() -> str:
    """Returns a deterministic default `run_id`: `test_run_0001`."""
    return "test_run_0001"


def pytest_configure(config: pytest.Config) -> None:
    """Add lines for ini."""
    config.addinivalue_line(
        "markers",
        "integration: touches external deps (docker/postgres/http/etc.)",
    )
    config.addinivalue_line(
        "markers",
        "unit: local-only test (no external deps)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """
    Auto-mark tests by which folder they're in:
    - `tests/integration/**` -> @pytest.mark.integration
    - `tests/unit/**`        -> @pytest.mark.unit
    """
    for item in items:
        p = str(item.fspath).replace("\\", "/")
        if "/tests/integration/" in p:
            item.add_marker(pytest.mark.integration)
        if "/tests/unit/" in p:
            item.add_marker(pytest.mark.unit)


            