from __future__ import annotations

import httpx

from warehouse_pipeline.extract.dummyjson_client import DummyJsonClient


def test_client_retries_429_then_succeeds() -> None:
    """Does not ingore `Retry-After` msg and retries only appropriately."""
    # fetches and asserts Users page as example

    calls = {"count": 0}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """Returns a useable `httpx.Response` to actually injest from."""
        
        calls["count"] += 1

        if calls["count"] == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "0"},
                request=request,
            )

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
                "limit": 1,
            },
            request=request,
        )

    http_client = httpx.Client(
        base_url="https://dummyjson.com",
        transport=httpx.MockTransport(handler),
    )

    
    # retrier logic
    client = DummyJsonClient(
        client=http_client,
        sleeper=sleeps.append,
        min_interval_s=0.0,
        max_attempts=3,
    )

    page = client.get_users_page(limit=1, skip=0)

    assert page.users[0].id == 1
    assert calls["count"] == 2      # one on `Retry-After`, another retry after. 
                                    # no more, no less