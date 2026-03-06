from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from warehouse_pipeline.extract import extract_dummyjson_snapshots
from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient



USERS = [
    {
        "id": 1,
        "firstName": "Ada",
        "lastName": "Lovelace",
        "email": "ada@example.com",
    },
    {
        "id": 2,
        "firstName": "Grace",
        "lastName": "Hopper",
        "email": "grace@example.com",
    },
]

PRODUCTS = [
    {
        "id": 10,
        "title": "Tea",
        "category": "groceries",
        "price": 4.99,
        "stock": 12,
    },
    {
        "id": 11,
        "title": "Biscuits",
        "category": "groceries",
        "price": 2.49,
        "stock": 30,
    },
]

CARTS = [
    {
        "id": 100,
        "userId": 1,
        "total": 9.98,
        "discountedTotal": 9.98,
        "totalProducts": 1,
        "totalQuantity": 2,
        "products": [
            {
                "id": 10,
                "quantity": 2,
                "price": 4.99,
                "total": 9.98,
                "discountedPrice": 4.99,
            }
        ],
    }
]


def _slice_page(items: list[dict], *, key: str, skip: int, limit: int) -> dict:
    """Slices page for data."""
    return {
        key: items[skip : skip + limit],
        "total": len(items),
        "skip": skip,
        "limit": limit,
    }


class _DummyJsonHandler(BaseHTTPRequestHandler):
    """DummyJosn-ish mock handlers for pulling `JSON`."""

    def do_GET(self) -> None:
        """Provide `JSON` fetching from 'pages' as expected."""
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        skip = int(query.get("skip", ["0"])[0])
        limit = int(query.get("limit", ["100"])[0])

        if parsed.path == "/users":
            body = _slice_page(USERS, key="users", skip=skip, limit=limit)
        elif parsed.path == "/products":
            body = _slice_page(PRODUCTS, key="products", skip=skip, limit=limit)
        elif parsed.path == "/carts":
            body = _slice_page(CARTS, key="carts", skip=skip, limit=limit)
        else:
            self.send_response(404) 
            self.end_headers()
            return

        raw = json.dumps(body).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()

        # write to it
        self.wfile.write(raw)


    def log_message(self, format: str, *args) -> None:
        # silences potential test server noise
        return
    


@pytest.fixture()
def dummyjson_server() -> Iterator[str]:
    """Local offline server for testing HTTP requesting with CI non-flakily."""
    server = HTTPServer(("127.0.0.1", 0), _DummyJsonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        # shut down properly
        server.shutdown()
        thread.join()



def test_extract_flow_writes_all_snapshots(
    tmp_path: Path,
    dummyjson_server: str,
) -> None:
    """Test a full extraction flow with HTTP requests on an offline local server (for CI)."""

    with DummyJsonClient(base_url=dummyjson_server, min_interval_s=0.0) as client:
        out = extract_dummyjson_snapshots(
            snapshot_root=tmp_path / "dummyjson",   # tmp root
            page_size=1,    # force pagination
            client=client,
        )

    # got them all
    assert set(out.keys()) == {"users", "products", "carts"}

    users = json.loads((tmp_path / "dummyjson" / "users.json").read_text(encoding="utf-8"))
    products = json.loads((tmp_path / "dummyjson" / "products.json").read_text(encoding="utf-8"))
    carts = json.loads((tmp_path / "dummyjson" / "carts.json").read_text(encoding="utf-8"))


    assert users["total"] == 2
    assert len(users["users"]) == 2

    assert products["total"] == 2
    assert len(products["products"]) == 2

    assert carts["total"] == 1  # there was one cart entry only
    assert len(carts["carts"]) == 1