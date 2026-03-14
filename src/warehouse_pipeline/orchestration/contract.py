from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from warehouse_pipeline.extract.bundles import snapshot_root_for_key
from warehouse_pipeline.transform.sql_plan import TransformStep

RunStatus = Literal["succeeded", "failed"]

DEFAULT_INCREMENTAL_OVERLAP_WINDOW = timedelta(days=7)


@dataclass(frozen=True)
class RunSpec:
    """
    The inputs that define one fully end-to-end pipeline execution.
    """

    mode: Literal["snapshot", "live", "incremental"]
    # `snapshot` is a perisisted write of the API.
    # `live` extracts all pages live.
    # `incremental` refills in runs designated.
    source_system: str = "dummyjson"
    snapshot_key: str | None = "v1"  # `v1` | `smoke`
    snapshot_root: Path | None = None  # where snapshot
    runs_root: Path = Path("runs")  # where runs
    page_size: int = 100
    git_sha: str | None = None
    transform_step: TransformStep = "build_all"  # `build_all` |
    publish_views: bool = True
    args_json: dict[str, Any] = field(default_factory=dict)

    # incremental fields that are ignored for snapshot and live
    watermark_column: str = "order_ts"
    since: datetime | None = None  # explicit low-watermark override
    until: datetime | None = None  # explicit high-watermark override
    overlap_window: timedelta = field(default_factory=lambda: DEFAULT_INCREMENTAL_OVERLAP_WINDOW)

    def resolved_snapshot_root(self) -> Path:
        """If doing `snapshot` mode, its snapshot root."""
        if self.mode != "snapshot":
            raise ValueError("`resolved_snapshot_root()` is only valid for snapshot runs")
        if self.snapshot_root is not None:
            return self.snapshot_root.resolve()
        if not self.snapshot_key:
            raise ValueError("`snapshot_key` is required when mode='snapshot'")
        return snapshot_root_for_key(self.snapshot_key)


@dataclass(frozen=True)
class RunManifest:
    """
    The final persisted results summary for one pipeline run.
    """

    run_id: UUID
    mode: str
    status: RunStatus
    source_system: str
    snapshot_key: str | None
    started_at: datetime
    finished_at: datetime

    # the pipeline's stages results
    extract: dict[str, Any]
    stage: dict[str, Any]
    dq: dict[str, Any]
    gate: dict[str, Any]
    transform: dict[str, Any]
    publish: dict[str, Any]

    timings_s: dict[str, float]
    artifacts: dict[str, str]
    error_message: str | None = None

    extraction_window: dict[str, Any] = field(default_factory=dict)
