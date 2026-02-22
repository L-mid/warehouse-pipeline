from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class LoadSummary:
    """Schema for all summary data that will be recorded."""
    run_id: UUID
    table_name: str
    input_path: str
    total: int
    loaded: int
    rejected: int

    def render_one_line(self) -> str:
        """How each line of summary is formatted for the terminal."""
        return f"{self.table_name}: total={self.total} loaded={self.loaded} rejected={self.rejected} run_id={self.run_id}"

