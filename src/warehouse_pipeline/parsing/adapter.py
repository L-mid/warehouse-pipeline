from __future__ import annotations

from typing import Any, Mapping

from warehouse_pipeline.parsing.types import RejectCode, RejectRow


def adapt_row(      # validate row
    raw: Mapping[str, Any],
    *,
    aliases: Mapping[str, str],
    source_row: int,
    raw_payload: Any,
    reject_unknown_input_fields: bool = True,
) -> dict[str, Any] | RejectRow:
    """
    Analyze `raw` row for unknown keys.

    maps raw input keys to provided `aliases` keys.
    If `reject_unknown_input_fields` is `True`: 
    - Rejects unknown input fields deterministically before any projection or mutation can drop them.

    `aliases` is the allowed input key set and also the mapping to canonical keys.
    example:
      `{"Customer Id": "customer_id", "customer_id": "customer_id", ...}`
    """    
    # Order items in ASC, reproducible". 
    items = sorted(((str(k).strip(), v) for k, v in raw.items()), key=lambda kv: kv[0])

    unknown: list[str] = []
    out: dict[str, Any] = {}

    for k, v in items:
        canon = aliases.get(k)
        if canon is None:
            unknown.append(k)
            continue
        out[canon] = v

    if reject_unknown_input_fields and unknown:
        return RejectRow(
            reason_code=RejectCode.unknown_field,
            detail=f"unknown input fields: {unknown}",
            raw_payload=raw_payload,
            source_row=source_row,
        )

    return out    