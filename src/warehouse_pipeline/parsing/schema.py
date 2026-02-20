from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from .primitives import ParseError, normalize_cell
from .types import ParsedRow, RejectCode, RejectRow

# Typing: 
# Getter is a list of dict, value pairs.
# Parser is a list of list, value pairs.
Getter = Callable[[Mapping[str, Any]], Any]
Parser = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class FieldSpec:
    """Any given field's configurable expectations."""
    out_name: str               # internally mapped name of this field.
    getter: Getter              # how to fetch (normalize) this field's value.
    parser: Parser              # how to parse this field's value.
    required: bool = True       # whether or not this field's value must exist.

 
@dataclass(frozen=True, slots=True)
class RowParser:
    """
    Parse a single row of fields. 
    
    Either:
    - return the successfully parsed row as a `ParsedRow`, 
    - or record it as a `RejectRow`.
    
    Rejection order is always in:
    - 1st: `unknown_field` (if `reject_unknown_fields` = True)
    - 2nd: first `missing_required` order in `FieldSpec`'s index
    - 3rd: first type/format error, order in `FieldSpec`'s index
    """
    fields: Sequence[FieldSpec]             # every field in this row in a `[]` to pull from.
    known_fields: set[str]                  # pre-registered fields expected to exist. 
    reject_unknown_fields: bool = True      # bool, reject any non pre-registered fields

    def parse(self, raw_row: Mapping[str, Any], *, source_row: int, raw_payload: Any) -> ParsedRow | RejectRow:
        """
        Normalize then parse contents from CSV/JSONL.
        Returns an accepted `ParsedRow`, or a rejected `RejectRow` (upon any early return).
        """
        # creates `normalized` map for mapping known keys to known fields.
        normalized: dict[str, Any] = {}
        for k, v in raw_row.items():
            kk = str(k).strip()
            normalized[kk] = normalize_cell(v)

        # 1st rejection reason: Unknown fields (if `True`) (debug canonicalizaion introducing bugs)
        if self.reject_unknown_fields:
            unknown = sorted(set(normalized.keys()) - self.known_fields)
            if unknown:
                return RejectRow(
                    reason_code=RejectCode.unknown_field,
                    detail=f"unknown fields: {unknown}",
                    raw_payload=raw_payload,
                    source_row=source_row,
                )

        ## -- Parsing loop
        out: dict[str, Any] = {}
        for f in self.fields:
            try:
                # normalize the current field's value.
                raw_v = f.getter(normalized)           
                raw_v = normalize_cell(raw_v)

                # 2nd rejection reason: required field is `None`.
                if raw_v is None:
                    # if required, reject. 
                    if f.required:
                        raise ParseError(RejectCode.missing_required, f"{f.out_name}: missing required")
                    # if optional, this field's value is genuinely `None`.
                    out[f.out_name] = None
                    continue
                
                # field's value passed filters and is now assigned.
                out[f.out_name] = f.parser(raw_v)

            # 3rd rejection reason: typing / formatting error.
            except ParseError as e:
                return RejectRow(
                    reason_code=e.code,
                    detail=e.detail,
                    raw_payload=raw_payload,
                    source_row=source_row,
                )

        return ParsedRow(out)