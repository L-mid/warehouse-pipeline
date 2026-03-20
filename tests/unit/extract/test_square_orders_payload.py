from __future__ import annotations

from datetime import UTC, datetime, timedelta

from warehouse_pipeline.extract.sources.square_orders_source import SquareOrdersSource
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow


def test_square_orders_source_pull_incremental_builds_updated_at_window_and_meta(
    monkeypatch,
) -> None:
    src = SquareOrdersSource(
        access_token="token",
        location_ids=("LOC-1", "LOC-2"),
    )

    seen: dict[str, object] = {}

    def fake_search_orders(
        self,
        *,
        body: dict[str, object],
    ) -> tuple[list[dict[str, object]], int]:
        seen["body"] = body
        return ([{"id": "ord-100"}], 2)

    monkeypatch.setattr(SquareOrdersSource, "_search_orders", fake_search_orders)

    window = ExtractionWindow(
        watermark_column="updated_at",
        low=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        high=datetime(2026, 3, 8, 0, 0, tzinfo=UTC),
        prior_watermark=datetime(2026, 3, 7, 0, 0, tzinfo=UTC),
        overlap=timedelta(days=1),
        is_first_run=False,
    )

    result = src.pull_incremental(page_size=500, window=window)

    assert seen["body"] == {
        "location_ids": ["LOC-1", "LOC-2"],
        "limit": 500,
        "return_entries": False,
        "query": {
            "filter": {
                "date_time_filter": {
                    "updated_at": {
                        "start_at": "2026-03-01T00:00:00+00:00",
                        "end_at": "2026-03-08T00:00:00+00:00",
                    }
                }
            },
            "sort": {
                "sort_field": "UPDATED_AT",
                "sort_order": "ASC",
            },
        },
    }

    assert result.extract.source_system == "square_orders"
    assert result.extract.mode == "incremental"
    assert result.extract.entities == {"orders": ({"id": "ord-100"},)}
    assert result.extract.totals == {"orders": 1}
    assert result.extract.pages_fetched == {"orders": 2}
    assert result.extract.page_size == 500

    assert result.meta == {
        "source_system": "square_orders",
        "strategy": "square_search_orders_incremental",
        "native_incremental": True,
        "watermark_column": "updated_at",
        "low": "2026-03-01T00:00:00+00:00",
        "high": "2026-03-08T00:00:00+00:00",
        "low_boundary": "inclusive",
        "high_boundary": "inclusive",
        "orders_pulled": 1,
        "pages_fetched": 2,
        "sort_field": "UPDATED_AT",
        "sort_order": "ASC",
    }
