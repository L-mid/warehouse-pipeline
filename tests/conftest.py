from __future__ import annotations

import os
import shutil
import subprocess
from functools import lru_cache
from pathlib import Path

import pytest


def _find_repo_root(start: Path) -> Path:
    """
    Walks upward until finding `pyproject.toml`.
    Allows tests to run from anywhere under `tests/`.
    """
    marker = "pyproject.toml"
    cur = start.resolve()
    for p in (cur, *cur.parents):
        if (p / marker).exists():
            return p
    raise RuntimeError(f"Could not find repo root from: {start}")


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Returns the absolute path to the repo root (finds by walking up to `pyproject.toml`)."""
    # walking starts from the callers location (`tests/conftest.py`)
    return _find_repo_root(Path(__file__))


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


## -- resolve docker


@lru_cache(maxsize=1)
def _cmd_ok(*cmd: str) -> bool:
    """Test a suproc runs ok in this environment."""

    try:
        completed = subprocess.run(
            list(cmd),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=8,
        )
    except (OSError, subprocess.SubprocessError):
        return False

    return completed.returncode == 0


@lru_cache(maxsize=1)
def docker_usable() -> bool:
    """Decide whether docker is usable."""
    if shutil.which("docker") is None:
        return False

    return _cmd_ok("docker", "info") and _cmd_ok("docker", "compose", "version")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """
    Auto mark tests by folder and skip marker-driven tests as is appropriate.
    """
    in_ci = os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}
    docker_ok = docker_usable()

    skip_no_docker = pytest.mark.skip(reason="docker/compose unavailable on this machine")
    skip_non_ci = pytest.mark.skip(reason="non_ci test skipped in CI")

    for item in items:
        p = str(item.fspath).replace("\\", "/")

        if "/tests/unit/" in p:
            item.add_marker(pytest.mark.unit)

        if "/tests/integration/" in p:
            item.add_marker(pytest.mark.integration)

        if "/tests/integration/whole_pipeline_runs/" in p:
            item.add_marker(pytest.mark.heavy_integration)

        if "docker_required" in item.keywords and not docker_ok:
            item.add_marker(skip_no_docker)

        if in_ci and "non_ci" in item.keywords:
            item.add_marker(skip_non_ci)
