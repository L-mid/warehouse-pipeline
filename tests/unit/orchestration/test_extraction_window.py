from __future__ import annotations

from datetime import UTC, datetime, timedelta

from warehouse_pipeline.orchestration.extraction_window import resolve_extraction_window


def test_resolve_extraction_window_uses_happy_path() -> None:
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
