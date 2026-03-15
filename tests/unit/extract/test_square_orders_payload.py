from datetime import UTC, datetime

from warehouse_pipeline.extract.sources.square_orders_source import SquareOrdersSource


def test_square_search_orders_payload_uses_updated_at_and_asc_sort() -> None:
    src = SquareOrdersSource(
        access_token="x",
        location_ids=("L1",),
    )

    body = src._base_search_body(
        watermark_column="updated_at",
        low=datetime(2026, 3, 1, tzinfo=UTC),
        high=datetime(2026, 3, 8, tzinfo=UTC),
        page_size=500,
    )

    assert body["return_entries"] is False
    assert body["limit"] == 500
    assert body["query"]["filter"]["date_time_filter"]["updated_at"]["start_at"] == (
        "2026-03-01T00:00:00+00:00"
    )
    assert body["query"]["sort"] == {
        "sort_field": "UPDATED_AT",
        "sort_order": "ASC",
    }
