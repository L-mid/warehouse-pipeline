from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from warehouse_pipeline.orchestration.extraction_window import resolve_extraction_window


def test_resolve_extraction_window_happy_path() -> None:
    """Test the extraction window's default high is used when until is missing."""
    started_at = datetime(2026, 3, 14, tzinfo=UTC)
    default_high = datetime(2025, 1, 1, tzinfo=UTC)
    since = datetime(2024, 1, 1, tzinfo=UTC)

    window = resolve_extraction_window(
        watermark_column="order_ts",
        prior_watermark=None,
        run_started_at=started_at,
        since=since,
        until=None,
        overlap=timedelta(0),
        default_high=default_high,
    )

    assert window.low == since
    assert window.high == default_high
    assert window.is_first_run is True


def test_resolve_extraction_window_uses_prior_watermark_minus_overlap() -> None:
    """Prior watermark minus overlap must work."""
    started_at = datetime(2026, 3, 14, tzinfo=UTC)
    prior_watermark = datetime(2024, 12, 20, tzinfo=UTC)
    default_high = datetime(2024, 12, 31, tzinfo=UTC)

    window = resolve_extraction_window(
        watermark_column="order_ts",
        prior_watermark=prior_watermark,
        run_started_at=started_at,
        since=None,
        until=None,
        overlap=timedelta(days=7),
        default_high=default_high,
    )

    assert window.low == datetime(2024, 12, 13, tzinfo=UTC)
    assert window.high == default_high
    assert window.prior_watermark == prior_watermark
    assert window.overlap == timedelta(days=7)
    assert window.is_first_run is False


def test_resolve_extraction_window_requires_since_or_prior_watermark() -> None:
    """Will not pull a full run on the first instance of running incremental mode."""
    started_at = datetime(2026, 3, 14, tzinfo=UTC)

    with pytest.raises(ValueError, match="No prior successful watermark found"):
        resolve_extraction_window(
            watermark_column="order_ts",
            prior_watermark=None,
            run_started_at=started_at,
            since=None,
            until=None,
            overlap=timedelta(days=7),
            default_high=datetime(2024, 12, 31, tzinfo=UTC),
        )
