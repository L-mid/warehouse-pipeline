from __future__ import annotations

from pathlib import Path

import httpx

from warehouse_pipeline.extract import extract_dummyjson_snapshots
from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient


def test_extract_dummyjson_snapshots_happy_path(tmp_path: Path) -> None:
    """Test extraction functions from init."""
    
    def handler(request: httpx.Request) -> httpx.Response:
        """Returns a useable `httpx.Response` to actually injest from."""
        if request.url.path == "/users":
            return httpx.Response(
                200,
                json={
                    "users": [
                        {
                            "id": 1,
                            "firstName": "Ada",
                            "lastName": "Lovelace",
                            "email": "ada@example.com",
                        }
                    ],
                    "total": 1,
                    "skip": 0,
                    "limit": 100,
                },
                request=request,
            )

        if request.url.path == "/products":
            return httpx.Response(
                200,
                json={
                    "products": [
                        {
                            "id": 10,
                            "title": "Tea",
                            "category": "groceries",
                            "price": 4.99,
                            "stock": 12,
                        }
                    ],
                    "total": 1,
                    "skip": 0,
                    "limit": 100,
                },
                request=request,
            )

        if request.url.path == "/carts":
            return httpx.Response(
                200,
                json={
                    "carts": [
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
                    ],
                    "total": 1,
                    "skip": 0,
                    "limit": 100,
                },
                request=request,
            )

        return httpx.Response(404, request=request)

    http_client = httpx.Client(
        base_url="https://dummyjson.com",
        transport=httpx.MockTransport(handler),
    )

    client = DummyJsonClient(
        client=http_client,
        min_interval_s=0.0,
    )


    # extraction test
    out = extract_dummyjson_snapshots(
        snapshot_root=tmp_path / "dummyjson",
        page_size=100,
        client=client,
    )

    assert set(out.keys()) == {"users", "products", "carts"}    # got em all
    assert (tmp_path / "dummyjson" / "users.json").exists()
    assert (tmp_path / "dummyjson" / "products.json").exists()
    assert (tmp_path / "dummyjson" / "carts.json").exists()