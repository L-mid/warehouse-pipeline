from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _require_tool(name: str) -> str:
    """Required installations."""
    path = shutil.which(name)
    if path is None:
        raise SystemExit(
            f"Missing required tool: {name}\n"
            "Install dev dependencies first, e.g.:\n"
            "  pip install -e '.[dev]'"
        )
    return path


def _run(cmd: list[str], *, extra_env: dict[str, str] | None = None) -> None:
    """Run headless using `subprocess`."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    print("$", " ".join(cmd))
    completed = subprocess.run(cmd, cwd=ROOT, env=env)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def main() -> None:
    """Run this repository's ci gate locally."""
    # make sure these are around
    ruff = _require_tool("ruff")
    pyright = _require_tool("pyright")
    pytest_bin = _require_tool("pytest")

    # formatter check
    _run([ruff, "format", "--check", "."])

    # lint check
    _run([ruff, "check", "."])

    # type check
    _run([pyright])

    # exact CI-like headless pytest behavior
    _run([pytest_bin, "-m", "not non_ci"], extra_env={"CI": "true"})


if __name__ == "__main__":
    main()
