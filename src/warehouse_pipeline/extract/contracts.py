from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

ExtractMode = Literal["snapshot", "live", "incremental"]


@dataclass(frozen=True)
class RawExtract:
    """
    A raw extraction artifact.
    """

    source_system: str
    mode: ExtractMode
    entities: dict[str, tuple[dict[str, Any], ...]]
    snapshot_key: str | None = None
    source_paths: dict[str, str] = field(default_factory=dict)
    totals: dict[str, int] = field(default_factory=dict)
    pages_fetched: dict[str, int] = field(default_factory=dict)
    page_size: int | None = None

    def count(self, entity_name: str) -> int:
        """Count how mnay entities were extracted."""
        return len(self.entities.get(entity_name, ()))
