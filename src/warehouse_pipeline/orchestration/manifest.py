from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from warehouse_pipeline.orchestration.contract import RunManifest


def _jsonable(value: Any) -> Any:
    """Makes values jsonable, best effort only."""
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def manifest_path(run_dir: Path) -> Path:
    """Makes a standard path for one single run's manifest."""
    return run_dir / "manifest.json"


def manifest_to_dict(manifest: RunManifest) -> dict[str, Any]:
    """Convert the manifest into a JSON-safe dict so it can be written."""
    return _jsonable(asdict(manifest))


def write_manifest(*, run_dir: Path, manifest: RunManifest) -> Path:
    """
    Writes `runs/<run_id>/manifest.json` to the provided `run_dir`.
    Returns the path it wrote to.
    """
    run_dir = run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    path = manifest_path(run_dir)
    path.write_text(
        json.dumps(manifest_to_dict(manifest), indent=2, sort_keys=True),  # keys are sorted.
        encoding="utf-8",
    )
    return path
