from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, TypeVar


PageT = TypeVar("PageT")
ItemT = TypeVar("ItemT")


@dataclass(frozen=True)
class PaginationResult(Generic[ItemT]):
    """Overview of result after injestion."""
    items: list[ItemT]
    total: int
    pages_fetched: int
    page_size: int


def fetch_all_pages(
    *,
    fetch_page: Callable[[int, int], PageT],
    get_items: Callable[[PageT], list[ItemT]],
    get_total: Callable[[PageT], int],
    get_skip: Callable[[PageT], int],
    get_limit: Callable[[PageT], int],
    page_size: int = 100,
    max_pages: int = 1000,
) -> PaginationResult[ItemT]:
    """
    An offset/limit paginator.

    Request pages in order, aggregates all items.
    Stops when total is reached and protect against repeated offsets or infinite loops.
    """
    # sanity
    if page_size <= 0:
        raise ValueError("`page_size` must be > 0")
    if max_pages <= 0:
        raise ValueError("`max_pages` must be > 0")

    all_items: list[ItemT] = []
    seen_skips: set[int] = set()
    pages_fetched = 0
    next_skip = 0
    stable_total: int | None = None   

    while True:
        if pages_fetched >= max_pages:
            raise RuntimeError(f"Pagination exceeded max_pages={max_pages}")
        if next_skip in seen_skips:
            raise RuntimeError(f"Paginator saw repeated skip={next_skip}")

        seen_skips.add(next_skip)     # set method. watch it in memory.

        page = fetch_page(page_size, next_skip)
        page_items = list(get_items(page))
        page_total = int(get_total(page))
        page_skip = int(get_skip(page))
        page_limit = int(get_limit(page))

        # page total should be sane.
        if stable_total is None:
            stable_total = page_total
        elif page_total != stable_total:
            raise RuntimeError(
                f"Total changed across pages: {stable_total} to {page_total}"
            )
        

        all_items.extend(page_items)        # add all items
        pages_fetched += 1

        # breaks
        if not page_items:
            break
        if len(all_items) >= stable_total:
            break

        next_skip = page_skip + page_limit  


    total = stable_total if stable_total is not None else len(all_items)


    # Trim in case an API returns too much.
    return PaginationResult(
        items=all_items[:total],
        total=total,
        pages_fetched=pages_fetched,
        page_size=page_size,
    )

