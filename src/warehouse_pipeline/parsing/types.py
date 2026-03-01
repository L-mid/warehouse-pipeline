from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping
 
 
class RejectCode(str, Enum):
    """Typed rejection classifications."""
    missing_required = "missing_required"
    invalid_int = "invalid_int"                 # also used for bool parsing errors
    invalid_numeric = "invalid_numeric"
    invalid_timestamp = "invalid_timestamp"     # also used for date parsing errors
    duplicate_key = "duplicate_key"
    unknown_field = "unknown_field"

 
@dataclass(frozen=True, slots=True)
class ParsedRow:
    """Accepted row's expected schema."""
    values: dict[str, Any]
    source_row: int
    raw_payload: Mapping[str, Any]
 
    def to_mapping(self) -> Mapping[str, Any]:
        """
        Values ready for staging insert. Must match the `stg_*` column names
        (excluding `run_id`/`created_at`).
        """
        return self.values


@dataclass(frozen=True, slots=True)
class RejectRow:
    """Rejected row's contents."""
    reason_code: RejectCode
    reason_detail: str
    raw_payload: Mapping[str, Any]  # the raw unmutated object being parsed.
    source_row: int                 # 1-based row/line number (nor sure what for)


 

