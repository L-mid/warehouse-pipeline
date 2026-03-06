from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient
from warehouse_pipeline.extract.paginator import PaginationResult, fetch_all_pages
from warehouse_pipeline.extract.snapshot_store import SnapshotStore


def extract_dummyjson_snapshots(
    *,
    snapshot_root: Path,
    page_size: int = 100,
    client: DummyJsonClient | None = None,
) -> dict[str, Path]:
    """
    Fetch all `DummyJSON` resources in live mode, and write pinned snapshots.

    Returns a dict like:
        `{
            "users": Path(.../users.json),
            "products": Path(.../products.json),
            "carts": Path(.../carts.json),
        }`
    For use as snapshots further down the line.
    """
    store = SnapshotStore(snapshot_root)

    owns_client = client is None
    live_client = client or DummyJsonClient()

    try:
        users: PaginationResult = fetch_all_pages(
            fetch_page=live_client.get_users_page,
            get_items=lambda page: page.users,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )
        products: PaginationResult = fetch_all_pages(
            fetch_page=live_client.get_products_page,
            get_items=lambda page: page.products,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )
        carts: PaginationResult = fetch_all_pages(
            fetch_page=live_client.get_carts_page,
            get_items=lambda page: page.carts,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )

        out: dict[str, Path] = {}
        out["users"] = store.write_json(
            "users",
            {
                "users": [x.model_dump(mode="json") for x in users.items],
                "total": users.total,
                "skip": 0,
                "limit": page_size,
            },
        )
        out["products"] = store.write_json(
            "products",
            {
                "products": [x.model_dump(mode="json") for x in products.items],
                "total": products.total,
                "skip": 0,
                "limit": page_size,
            },
        )
        out["carts"] = store.write_json(
            "carts",
            {
                "carts": [x.model_dump(mode="json") for x in carts.items],
                "total": carts.total,
                "skip": 0,
                "limit": page_size,
            },
        )
        return out
    finally:
        if owns_client:
            # off
            live_client.close()