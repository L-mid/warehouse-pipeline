# all of these need formal testing.

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class RejectCode(str, Enum):
    """Typed rejection classifications."""
    missing_required = "missing_required"
    invalid_int = "invalid_int"
    invalid_numeric = "invalid_numeric"
    invalid_timestamp = "invalid_timestamp"     # also used for date parsing errors
    unknown_field = "unknown_field"


@dataclass(frozen=True, slots=True)
class ParsedRow:
    """Accepted row's contents."""
    values: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RejectRow:
    """Rejected row's contents."""
    reason_code: RejectCode
    detail: str
    raw_payload: Any            # the raw unmutated object being parsed.
    source_row: int             # 1-based row/line number (nor sure what for)




