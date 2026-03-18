from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.extract.source_contract import PullResult
from warehouse_pipeline.orchestration.extraction_window import ExtractionWindow


@dataclass(frozen=True)
class SquareOrdersSource:
    """
    Native incremental source for Square Orders.
    """

    access_token: str
    location_ids: tuple[str, ...]
    square_version: str = "2026-01-22"
    base_url: str = "https://connect.squareupsandbox.com"

    source_system: str = "square_orders"

    @classmethod
    def from_env(cls) -> SquareOrdersSource:
        """Setup token, locations, and location_ids for Square."""
        token = os.environ["SQUARE_ACCESS_TOKEN"]
        raw_locations = os.environ["SQUARE_LOCATION_IDS"]
        location_ids = tuple(x.strip() for x in raw_locations.split(",") if x.strip())
        if not location_ids:
            raise ValueError("SQUARE_LOCATION_IDS must contain at least one location id")
        return cls(
            access_token=token,
            location_ids=location_ids,
        )

    def validate_watermark_column(self, watermark_column: str) -> None:
        """
        Watermark column must be one of, `created_at`, `updated_at`, `closed_at`,
        as they are the time attributes from Square.
        """
        allowed = {"updated_at"}  # updated_at as time
        if watermark_column not in allowed:
            raise ValueError(f"Square Orders `watermark_column` must be one of {sorted(allowed)}")

    def default_high_watermark(
        self,
        *,
        watermark_column: str,
        run_started_at: datetime,
    ) -> datetime | None:
        """"""
        self.validate_watermark_column(watermark_column)
        return run_started_at

    def pull_full(self, *, page_size: int) -> PullResult:
        orders, pages = self._search_orders(
            body={
                "location_ids": list(self.location_ids),
                "limit": min(page_size, 1000),
                "return_entries": False,
                "query": {
                    "sort": {
                        "sort_field": "UPDATED_AT",
                        "sort_order": "ASC",
                    }
                },
            }
        )

        extract = RawExtract(
            source_system=self.source_system,
            mode="live",
            entities={"orders": tuple(orders)},
            totals={"orders": len(orders)},
            pages_fetched={"orders": pages},
            page_size=page_size,
        )

        return PullResult(
            extract=extract,
            meta={
                "source_system": self.source_system,
                "strategy": "square_search_orders_full",
                "orders_pulled": len(orders),
                "pages_fetched": pages,
                "sort_field": "UPDATED_AT",
                "sort_order": "ASC",
            },
        )

    def pull_incremental(
        self,
        *,
        page_size: int,
        window: ExtractionWindow,
    ) -> PullResult:
        self.validate_watermark_column(window.watermark_column)

        orders, pages = self._search_orders(
            body={
                "location_ids": list(self.location_ids),
                "limit": min(page_size, 1000),
                "return_entries": False,
                "query": {
                    "filter": {
                        "date_time_filter": {
                            "updated_at": {
                                "start_at": window.low.isoformat(),
                                "end_at": window.high.isoformat(),
                            }
                        }
                    },
                    "sort": {
                        "sort_field": "UPDATED_AT",
                        "sort_order": "ASC",
                    },
                },
            }
        )

        extract = RawExtract(
            source_system=self.source_system,
            mode="incremental",
            entities={"orders": tuple(orders)},
            totals={"orders": len(orders)},
            pages_fetched={"orders": pages},
            page_size=page_size,
        )

        return PullResult(
            extract=extract,
            meta={
                "source_system": self.source_system,
                "strategy": "square_search_orders_incremental",
                "native_incremental": True,
                "watermark_column": window.watermark_column,
                "low": window.low.isoformat(),
                "high": window.high.isoformat(),
                "low_boundary": "inclusive",
                "high_boundary": "inclusive",
                "orders_pulled": len(orders),
                "pages_fetched": pages,
                "sort_field": "UPDATED_AT",
                "sort_order": "ASC",
            },
        )

    def _search_orders(self, *, body: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
        orders: list[dict[str, Any]] = []
        cursor: str | None = None
        pages = 0

        with httpx.Client(
            base_url=self.base_url.rstrip("/"),
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Square-Version": self.square_version,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "warehouse-pipeline/0.5.0",
            },
        ) as client:
            while True:
                request_body = dict(body)
                if cursor is not None:
                    request_body["cursor"] = cursor

                resp = client.post("/v2/orders/search", json=request_body)
                resp.raise_for_status()

                payload = resp.json()
                batch = payload.get("orders", [])
                if not isinstance(batch, list):
                    raise ValueError("Square SearchOrders returned non-list 'orders' payload")

                for order in batch:
                    if not isinstance(order, dict):
                        raise ValueError("Square SearchOrders returned a non-object order payload")
                    orders.append(order)

                pages += 1
                cursor = payload.get("cursor")
                if not cursor:
                    break

        return orders, pages
