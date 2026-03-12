from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient
from warehouse_pipeline.extract.models import (
    DummyCart,
    DummyProduct,
    DummyUser,
    parse_carts_page,
    parse_products_page,
    parse_users_page,
)
from warehouse_pipeline.extract.paginator import PaginationResult, fetch_all_pages
from warehouse_pipeline.extract.snapshot_store import SnapshotStore

DEFAULT_SNAPSHOT_BASE_DIR = Path(__file__).resolve().parents[3] / "data" / "snapshots" / "dummyjson"


@dataclass(frozen=True)
class ExtractBundle:
    """
    Fully validated source bundle for one pipeline run.

    Gives staging something to stage.
    """

    mode: Literal["snapshot", "live"]
    users: tuple[DummyUser, ...]
    products: tuple[DummyProduct, ...]
    carts: tuple[DummyCart, ...]
    snapshot_key: str | None = None
    source_paths: dict[str, str] = field(default_factory=dict)
    totals: dict[str, int] = field(default_factory=dict)
    pages_fetched: dict[str, int] = field(default_factory=dict)
    page_size: int | None = None


def snapshot_root_for_key(snapshot_key: str, *, base_dir: Path | None = None) -> Path:
    """Resolves the pinned snapshot directory for a snapshot key such as `v1` or `smoke`."""
    root = (
        base_dir or DEFAULT_SNAPSHOT_BASE_DIR
    ).resolve()  # fallback to `data/snapshots/dummyjson`.
    return root / snapshot_key  # give a key


def read_snapshot_bundle(
    *,
    snapshot_root: Path,
    snapshot_key: str | None = None,
) -> ExtractBundle:
    """
    Reads pinned snapshot files and validates them into some typed extract models.
    """
    root = snapshot_root.resolve()
    store = SnapshotStore(root)

    users_payload = store.read_json("users")
    products_payload = store.read_json("products")
    carts_payload = store.read_json("carts")

    users_page = parse_users_page(users_payload)
    products_page = parse_products_page(products_payload)
    carts_page = parse_carts_page(carts_payload)

    return ExtractBundle(
        mode="snapshot",
        snapshot_key=snapshot_key,
        users=tuple(users_page.users),
        products=tuple(products_page.products),
        carts=tuple(carts_page.carts),
        source_paths={
            "users": str(store.path_for("users")),
            "products": str(store.path_for("products")),
            "carts": str(store.path_for("carts")),
        },
        totals={
            "users": users_page.total,
            "products": products_page.total,
            "carts": carts_page.total,
        },
        pages_fetched={
            "users": 1,
            "products": 1,
            "carts": 1,
        },
        page_size=None,
    )


def fetch_live_bundle(
    *,
    page_size: int = 100,
    client: DummyJsonClient | None = None,
) -> ExtractBundle:
    """
    Extract all `DummyJSON` resources at once, for live mode and return one validated bundle.
    """
    owns_client = client is None
    live_client = client or DummyJsonClient()

    try:
        users: PaginationResult[DummyUser] = fetch_all_pages(
            fetch_page=live_client.get_users_page,
            get_items=lambda page: page.users,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )
        products: PaginationResult[DummyProduct] = fetch_all_pages(
            fetch_page=live_client.get_products_page,
            get_items=lambda page: page.products,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )
        carts: PaginationResult[DummyCart] = fetch_all_pages(
            fetch_page=live_client.get_carts_page,
            get_items=lambda page: page.carts,
            get_total=lambda page: page.total,
            get_skip=lambda page: page.skip,
            get_limit=lambda page: page.limit,
            page_size=page_size,
        )

        return ExtractBundle(
            mode="live",
            users=tuple(users.items),
            products=tuple(products.items),
            carts=tuple(carts.items),
            totals={
                "users": users.total,
                "products": products.total,
                "carts": carts.total,
            },
            pages_fetched={
                "users": users.pages_fetched,
                "products": products.pages_fetched,
                "carts": carts.pages_fetched,
            },
            page_size=page_size,
        )
    finally:
        if owns_client:
            # make sure close connect to live
            live_client.close()


# idea for a `scripts/fetch_dummyjson_snapshot.py` later.
def write_snapshot_bundle(bundle: ExtractBundle, *, snapshot_root: Path) -> dict[str, Path]:
    """
    Writes and persists an extract bundle to the standard snapshot layout.
    """
    store = SnapshotStore(snapshot_root.resolve())

    out: dict[str, Path] = {}
    out["users"] = store.write_json(
        "users",
        {
            "users": [x.model_dump(mode="json") for x in bundle.users],
            "total": len(bundle.users),
            "skip": 0,
            "limit": bundle.page_size or max(len(bundle.users), 1),
        },
    )
    out["products"] = store.write_json(
        "products",
        {
            "products": [x.model_dump(mode="json") for x in bundle.products],
            "total": len(bundle.products),
            "skip": 0,
            "limit": bundle.page_size or max(len(bundle.products), 1),
        },
    )
    out["carts"] = store.write_json(
        "carts",
        {
            "carts": [x.model_dump(mode="json") for x in bundle.carts],
            "total": len(bundle.carts),
            "skip": 0,
            "limit": bundle.page_size or max(len(bundle.carts), 1),
        },
    )
    return out
