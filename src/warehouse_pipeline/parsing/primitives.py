from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from .types import RejectCode

 
@dataclass(frozen=True, slots=True)
class ParseError(Exception):
    """Handles rejected fields, with additonal rejection details from error messages."""
    code: RejectCode            # dot-offs used to classify rejection type encountered
    detail: str                 # error message encountered that led to rejection.


# the string `"none"` is considered a vaild catagory in this repo.
_NULL_STRINGS = {"", "null", "na", "n/a"}


def normalize_cell(v: Any) -> Any:
    """Transform pre-parsed cells from CSV/JSONL into normalized shape."""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in _NULL_STRINGS:      # all accepted 'NA' synonyms
            return None
        return s
    return v


## -- casing for fields

def text_lower(s: str) -> str:
    """Lower casing transform for a value."""
    return s.lower()
def text_upper(s: str) -> str:
    """Upper casing transform for a value."""
    return s.upper()

def any_upper(v: Any) -> Any:
    """
    Upper casing transform, only affects `str` values. 
    Otherwise returns value unchanged.
    """
    return v.upper() if isinstance(v, str) else v

def any_lower(v: Any) -> Any:
    """
    Lower casing transform, only affects `str` values. 
    Otherwise returns value unchanged.
    """
    return v.lower() if isinstance(v, str) else v



## -- text / str fields

def parse_required_text(v: Any, *, field: str) -> str:
    """
    Assigns required text/`str` needed to parse a sucessful row. 
    Raises on: 
    - `None` typed input.
    - non `str` typed input.
    - empty strings.
    """
    v = normalize_cell(v)
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required text")
    if not isinstance(v, str):
        # reject altogether.
        raise ParseError(RejectCode.unknown_field, f"{field}: expected text, got {type(v).__name__}")
    s = v.strip()
    if s == "":
        raise ParseError(RejectCode.missing_required, f"{field}: missing required text")
    return s


def parse_optional_text(v: Any) -> str | None:
    """
    Assign optional text for row, or assign `None` on no successful expected match.
    """
    v = normalize_cell(v)
    if v is None:
        return None
    return str(v).strip()



## -- Other typed fields (`None` raises)

def parse_int(v: Any, *, field: str) -> int:
    """Parse integers. Raise on non `int` or `None`."""
    v = normalize_cell(v)
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required int")
    try:
        # Non `int` guard: "12.3" or "1e-4" should fail, not be sneakily coerced to `int`
        if isinstance(v, str) and (("." in v) or ("e" in v.lower())):
            raise ValueError(f"Imput number is non-integer: {type(v).__name__}")
        return int(v)
    except Exception:
        raise ParseError(RejectCode.invalid_int, f"{field}: invalid int value {v!r}")


def parse_numeric_12_2(v: Any, *, field: str) -> Decimal:
    """
    Parse vaild coercions to Postgres preferred presision in <= 12 total digits.
    Raise on non `Decimal` or `None`.
    """
    v = normalize_cell(v)
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required numeric")
    try:
        d = Decimal(str(v))     # convert possible int -> str first
    except (InvalidOperation, ValueError):
        raise ParseError(RejectCode.invalid_numeric, f"{field}: invalid numeric value {v!r}")

    # here: enforce scale=2 like numeric(12,2) for Postgres
    d2 = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # enforces precision <= 12 total digits (Postgres numeric(12,2))
    # total digits (ignoring sign and decimal dot)
    digits = len(d2.as_tuple().digits)
    if digits > 12:
        raise ParseError(RejectCode.invalid_numeric, f"{field}: numeric exceeds precision (12,2): {v!r}")
    return d2


def parse_date_yyyy_mm_dd(v: Any, *, field: str) -> date:
    """Parse date. Raise on non `str`, then on non successful `date` coercion, or `None`."""
    v = normalize_cell(v)
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required date")
    if not isinstance(v, str):
        raise ParseError(RejectCode.invalid_timestamp, f"{field}: invalid date value {v!r}")
    try:
        return date.fromisoformat(v)
    except Exception:
        # reuses invalid_timestamp per the fixed reject-code typings
        raise ParseError(RejectCode.invalid_timestamp, f"{field}: invalid date (expected YYYY-MM-DD): {v!r}")


def parse_timestamptz_iso(v: Any, *, field: str) -> datetime:
    """
    Accepts the ISO forms:
    - `2026-02-10T12:34:56Z`
    - `2026-02-10 12:34:56+00:00`
    - `2026-02-10T12:34:56`  (assumption: UTC if tz missing)
    
    No other normalization or guards within: raises on invaild format outside these and on `None`.
    """
    v = normalize_cell(v)
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required timestamp")

    if not isinstance(v, str):
        raise ParseError(RejectCode.invalid_timestamp, f"{field}: invalid timestamp value {v!r}")

    # normalization, convert some common non-conformitory iso
    s = v.strip().replace("Z", "+00:00")
    s = s.replace(" ", "T")  # allows space-separated

    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        # reject immediately.
        raise ParseError(RejectCode.invalid_timestamp, f"{field}: invalid timestamp (ISO): {v!r}")

    # assumption, UTC conversion of non specified timestaps.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def parse_bool_01(v: object, *, field: str) -> bool:
    if v is None:
        raise ParseError(RejectCode.missing_required, f"{field}: missing required bool")

    if isinstance(v, bool):
        return v

    s = str(v).strip().lower()
    # the allowed bool matches
    if s in ("1", "true", "t", "yes", "y"): return True
    if s in ("0", "false", "f", "no", "n"): return False

    raise ParseError(RejectCode.invalid_int, f"{field}: invalid boolean '{v}' (expected 0/1 or true/false)")