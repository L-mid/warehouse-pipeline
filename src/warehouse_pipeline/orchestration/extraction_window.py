from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class ExtractionWindow:
    """
    The resolved extraction range for one incremental run.
    """

    watermark_column: str
    low: datetime  # inclusive
    high: datetime  # exclusive
    prior_watermark: datetime | None  # what run_ledger reported
    overlap: timedelta  # how much subtracted from prior_watermark
    is_first_run: bool


def resolve_extraction_window(
    *,
    watermark_column: str,
    prior_watermark: datetime | None,
    run_started_at: datetime,
    since: datetime | None = None,
    until: datetime | None = None,
    overlap: timedelta = timedelta(0),
    default_high: datetime | None = None,
) -> ExtractionWindow:
    """
    Compute the used extraction window from inputs and prior state.
    """
    high = (
        until
        if until is not None
        else (default_high if default_high is not None else run_started_at)
    )

    if since is not None:
        # `since` explicitly overrides `low` if provided
        low = since
    elif prior_watermark is not None:
        low = prior_watermark - overlap
    else:
        raise ValueError(
            "No prior successful watermark found and --since was not provided. "
            "Run a full pull first, or pass an explicit --since to seed the watermark."
        )

    if low > high:
        raise ValueError(
            f"Extraction window is inverted: low={low.isoformat()} > high={high.isoformat()}. "
            f"Check --since/--until values."
        )

    return ExtractionWindow(
        watermark_column=watermark_column,
        low=low,
        high=high,
        prior_watermark=prior_watermark,
        overlap=overlap,
        is_first_run=prior_watermark is None,
    )
