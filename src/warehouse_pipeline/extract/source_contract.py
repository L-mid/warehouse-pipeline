from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol

from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow

BoundaryMode = Literal["inclusive", "exclusive"]


@dataclass(frozen=True)
class PullResult:
    """
    What one source pull returns to orchestration.
    """

    extract: RawExtract
    meta: dict[str, Any]


class SourceAdapter(Protocol):
    @property
    def source_system(self) -> str: ...

    def validate_watermark_column(self, watermark_column: str) -> None: ...

    def default_high_watermark(
        self,
        *,
        watermark_column: str,
        run_started_at: datetime,
    ) -> datetime | None: ...

    def pull_full(self, *, page_size: int) -> PullResult: ...

    def pull_incremental(
        self,
        *,
        page_size: int,
        window: ExtractionWindow,
    ) -> PullResult: ...
