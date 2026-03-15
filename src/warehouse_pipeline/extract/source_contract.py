from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol

from warehouse_pipeline.extract.bundles import ExtractBundle
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow

BoundaryMode = Literal["inclusive", "exclusive"]


@dataclass(frozen=True)
class PullResult:
    """
    What one source pull returns to orchestration.
    """

    bundle: ExtractBundle
    meta: dict[str, Any]


class SourceAdapter(Protocol):
    """
    Contract that every source system implements.
    """

    source_system: str

    def validate_watermark_column(self, watermark_column: str) -> None:
        """
        Raise `ValueError` if this source does not support the requested cursor field.
        """
        ...

    def default_high_watermark(
        self,
        *,
        watermark_column: str,
        run_started_at: datetime,
    ) -> datetime | None:
        """
        Return the default high watermark for this source, if any.
        Return `None` to fall back to `run_started_at`.
        """
        ...

    def pull_full(self, *, page_size: int) -> PullResult:
        """
        Fetch a full source pull for live mode.
        """
        ...

    def pull_incremental(
        self,
        *,
        page_size: int,
        window: ExtractionWindow,
    ) -> PullResult:
        """
        Fetch an incremental source pull for incremental mode.
        """
        ...
