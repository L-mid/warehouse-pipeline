from __future__ import annotations

from warehouse_pipeline.extract.models import UsersPage
from warehouse_pipeline.extract.paginator import fetch_all_pages


def test_fetch_all_pages_happy_path() -> None:
    """"""
    # `UsersPage` as example to fetch pages from
    pages = {

        # parse locally, avoid dependency on `parse_users_page` from `models.py` 
        # and make two 'pages'.  
        0: UsersPage.model_validate(
            {
                "users": [
                    {
                        "id": 1,
                        "firstName": "Ada",
                        "lastName": "Lovelace",
                        "email": "ada@example.com",
                    }
                ],
                "total": 2,
                "skip": 0,
                "limit": 1,
            }
        ),
        1: UsersPage.model_validate(
            {
                "users": [
                    {
                        "id": 2,
                        "firstName": "Grace",
                        "lastName": "Hopper",
                        "email": "grace@example.com",
                    }
                ],
                "total": 2,
                "skip": 1,
                "limit": 1,
            }
        ),
    }


    def fetch_page(limit: int, skip: int) -> UsersPage:
        """Fetch page, only one at once."""
        assert limit == 1
        return pages[skip]


    # page fetching logic
    result = fetch_all_pages(
        fetch_page=fetch_page,
        get_items=lambda page: page.users,
        get_total=lambda page: page.total,
        get_skip=lambda page: page.skip,
        get_limit=lambda page: page.limit,
        page_size=1,
    )

    # two pages:
    assert result.total == 2
    assert result.pages_fetched == 2
    assert [user.id for user in result.items] == [1, 2]


    