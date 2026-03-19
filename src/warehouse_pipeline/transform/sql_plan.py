from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

TransformStep = Literal["build_dims", "build_facts", "build_all"]

# where the dir is located.
DEFAULT_SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "transform"


# name of transform, and files.
_PLAN_FILES: dict[TransformStep, tuple[str, ...]] = {
    "build_facts": (
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    ),
    "build_all": (
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    ),
}


@dataclass(frozen=True)
class SqlPlan:
    """Dataclass for organizing SQL execution data."""

    step_name: TransformStep
    sql_dir: Path
    file_names: tuple[str, ...]
    paths: tuple[Path, ...]


def resolve_sql_plan(
    *,
    step_name: TransformStep = "build_all",  # default build all
    sql_dir: Path | None = None,
) -> SqlPlan:
    """
    Resolves SQL files to run for a given transform step.
    Returns an object with all needed data to exceute.
    """
    resolved_sql_dir = (sql_dir or DEFAULT_SQL_DIR).resolve()  # optionally provide custom path

    # sanities, no missing, no unknown.
    try:
        file_names = _PLAN_FILES[step_name]
    except KeyError as e:
        valid = ", ".join(_PLAN_FILES)
        raise ValueError(f"Unknown transform step: {step_name!r}. Valid steps: {valid}") from e

    paths = tuple(resolved_sql_dir / name for name in file_names)

    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        joined = "\n".join(missing)
        raise FileNotFoundError(f"Missing transform SQL file(s):\n{joined}")

    return SqlPlan(
        step_name=step_name,
        sql_dir=resolved_sql_dir,
        file_names=file_names,
        paths=paths,
    )
