from __future__ import annotations

import random
import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from warehouse_pipeline.extract.models import (
    CartsPage,
    ProductsPage,
    UsersPage,
    parse_carts_page,
    parse_products_page,
    parse_users_page,
)

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
## -- only continue to attempt to retrive on these!


class DummyJsonClientError(RuntimeError):
    """Raised if live `DummyJSON` extraction fails."""


class DummyJsonClient:
    """
    HTTP client for `DummyJSON`.

    One persistent HTTP session with a timeout on every request.
    - retries only retryable failures and honors `Retry-After` msg for `429s`.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://dummyjson.com",  #
        timeout_s: float = 10.0,
        max_attempts: int = 4,
        initial_backoff_s: float = 0.5,
        max_backoff_s: float = 8.0,
        min_interval_s: float = 0.25,
        client: httpx.Client | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        # sanity
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if min_interval_s < 0:
            raise ValueError("min_interval_s must be >= 0")

        self._max_attempts = max_attempts
        self._initial_backoff_s = initial_backoff_s
        self._max_backoff_s = max_backoff_s
        self._min_interval_s = min_interval_s
        self._sleep = sleeper
        self._clock = clock
        self._next_allowed_at = 0.0

        self._owns_client = client is None
        # optionally accept other custom client
        self._client = client or httpx.Client(  # default: `httpx.Client` pre-initalized here.
            base_url=base_url.rstrip("/"),
            timeout=timeout_s,
            headers={
                "Accept": "application/json",
                "User-Agent": "warehouse-pipeline/0.3.0",
            },
        )

    def __enter__(self) -> DummyJsonClient:
        """Enter session."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close session."""
        self.close()

    def close(self) -> None:
        """Close the client."""
        if self._owns_client:
            self._client.close()

    def get_users_page(self, limit: int, skip: int) -> UsersPage:
        """Request json from `DummyJSON`'s `/users` page. Return parsed `UsersPage`."""
        payload = self._request_json("/users", params={"limit": limit, "skip": skip})
        return parse_users_page(payload)

    def get_products_page(self, limit: int, skip: int) -> ProductsPage:
        """Request json from `DummyJSON`'s `/products` page. Return parsed `ProductsPage`."""
        payload = self._request_json("/products", params={"limit": limit, "skip": skip})
        return parse_products_page(payload)

    def get_carts_page(self, limit: int, skip: int) -> CartsPage:
        """Request json from `DummyJSON`'s `/carts` page. Return parsed `CartsPage`."""
        payload = self._request_json("/carts", params={"limit": limit, "skip": skip})
        return parse_carts_page(payload)

    def _request_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,  # limit, skip
    ) -> dict[str, Any]:
        """Request to fetch json from `DummyJSON`."""

        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):  # always attempt once.
            self._wait_for_turn()  # wait

            try:
                response = self._client.get(path, params=params)
            except httpx.RequestError as exc:
                last_error = exc
                if attempt == self._max_attempts:
                    raise DummyJsonClientError(
                        f"GET {path} failed after {attempt} attempts: {exc}"
                    ) from exc

                self._sleep(self._compute_backoff_s(attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt == self._max_attempts:
                    raise DummyJsonClientError(
                        f"GET {path} failed after {attempt} attempts "
                        f"with status={response.status_code} body={response.text[:200]!r}"
                    )

                self._sleep(self._retry_delay_s(response, attempt))
                continue

            if response.status_code >= 400:
                raise DummyJsonClientError(
                    f"GET {path} failed permanently with "
                    f"status={response.status_code} body={response.text[:200]!r}"
                    # and we backoff
                )

            # must have fetched valid json
            try:
                payload = response.json()
            except ValueError as exc:
                raise DummyJsonClientError(f"GET {path} returned non-JSON content") from exc

            if not isinstance(payload, dict):
                raise DummyJsonClientError(f"GET {path} returned JSON, but not an object payload")

            return payload

        # the request loop should never deplete before a return or raise.
        raise DummyJsonClientError("request loop exhausted unexpectedly") from last_error

    def _wait_for_turn(self) -> None:
        """
        Ensures minimum gap between request starts.
        """
        now = self._clock()
        if now < self._next_allowed_at:
            self._sleep(self._next_allowed_at - now)
            now = self._clock()

        self._next_allowed_at = now + self._min_interval_s
        # `_request_json` itself will wait.

    def _retry_delay_s(self, response: httpx.Response, attempt: int) -> float:
        """If `Retry-After` encountered, respect it."""
        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                # ignore malformed `Retry-After` and simply continue
                pass

        return self._compute_backoff_s(attempt)

    def _compute_backoff_s(self, attempt: int) -> float:
        """
        Exponential backoff with light jitter timing.

        Attempt 1: ~0.5s,
        Attempt 2: ~1.0s,
        Attempt 3: ~2.0s,
        ...
        """
        base = min(
            self._initial_backoff_s * (2 ** (attempt - 1)),
            self._max_backoff_s,
        )
        jitter = random.uniform(0.0, base * 0.25)
        return base + jitter  # always add
