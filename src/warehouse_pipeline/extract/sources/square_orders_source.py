from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

from warehouse_pipeline.extract.bundles import ExtractBundle
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
        return cls(
            access_token=token,
            location_ids=location_ids,
        )

    def validate_watermark_column(self, watermark_column: str) -> None:
        """
        Watermark column must be one of, `created_at`, `updated_at`, `closed_at`,
        as they are the time attributes from Square.
        """
        allowed = {"created_at", "updated_at", "closed_at"}
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
        # for this real API source, high is simply when the run started
        return run_started_at

    def pull_full(self, *, page_size: int) -> PullResult:
        # A full pull can still be implemented as an unfiltered SearchOrders,
        # but for now forcing incremental-only behavior to keep the contract clean.
        # will rethink the 'modes' entirely later
        raise NotImplementedError(
            "SquareOrdersSource is intended to be used through incremental SearchOrders."
        )

    def pull_incremental(
        self,
        *,
        page_size: int,
        window: ExtractionWindow,
    ) -> PullResult:
        self.validate_watermark_column(window.watermark_column)

        orders = self._search_orders_window(
            watermark_column=window.watermark_column,
            low=window.low,
            high=window.high,
            page_size=page_size,
        )
        # For now, because the current stage contract still expects DummyJson style
        # users/products/carts, do NOT try to force-map Square into that shape here.
        # Returns an empty placeholder bundle until stage/transform are redesigned.
        #
        # If want a temporary bridge, this adapter can emit a raw artifact directory
        # or a Square specific extract object
        # instead of ExtractBundle but probably will just ingore.
        # until later

        bundle = ExtractBundle(
            mode="live",
            users=(),
            products=(),
            carts=(),
            totals={"orders": len(orders)},
            pages_fetched={},
            page_size=page_size,
            source_paths={},
        )

        return PullResult(
            bundle=bundle,
            meta={
                "source_system": self.source_system,
                "native_incremental": True,
                "selection_strategy": "server_side_search_orders",
                "watermark_column": window.watermark_column,
                "low": window.low.isoformat(),
                "high": window.high.isoformat(),
                "low_boundary": "inclusive",
                "high_boundary": "inclusive",
                "sort_field": self._square_sort_field(window.watermark_column),
                "sort_order": "ASC",
                "orders_pulled": len(orders),
            },
        )

    def _search_orders_window(
        self,
        *,
        watermark_column: str,
        low: datetime,
        high: datetime,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """Search orders window, ."""

        out: list[dict[str, Any]] = []
        cursor: str | None = None

        body = self._base_search_body(
            watermark_column=watermark_column,
            low=low,
            high=high,
            page_size=page_size,
        )

        with httpx.Client(
            base_url=self.base_url.rstrip("/"),
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {self.access_token}",
                "Square-Version": self.square_version,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "warehouse-pipeline/0.4.0",
            },
        ) as client:
            while True:
                request_body = dict(body)
                if cursor is not None:
                    request_body["cursor"] = cursor

                resp = client.post("/v2/orders/search", json=request_body)
                resp.raise_for_status()

                payload = resp.json()
                out.extend(payload.get("orders", []))

                cursor = payload.get("cursor")
                # cursor until curser is gone
                if not cursor:
                    break

        return out

    def _base_search_body(
        self,
        *,
        watermark_column: str,
        low: datetime,
        high: datetime,
        page_size: int,
    ) -> dict[str, Any]:
        """Return data for ex."""

        return {
            "location_ids": list(self.location_ids),
            "limit": min(page_size, 1000),
            "return_entries": False,
            "query": {
                "filter": {
                    "date_time_filter": {
                        watermark_column: {
                            "start_at": low.isoformat(),
                            "end_at": high.isoformat(),
                        }
                    }
                },
                "sort": {
                    "sort_field": self._square_sort_field(watermark_column),
                    "sort_order": "ASC",
                },
            },
        }

    def _square_sort_field(self, watermark_column: str) -> str:
        return {
            "created_at": "CREATED_AT",
            "updated_at": "UPDATED_AT",
            "closed_at": "CLOSED_AT",
        }[watermark_column]
