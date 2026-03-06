from __future__ import annotations

from warehouse_pipeline.extract.models import parse_users_page

def test_parse_users_page_happy_path() -> None:
    """Parsing can actually parse and return vaild `json` with real contents."""
    # The Users page is parsed as the example here for now.

    page = parse_users_page(
        
        # input mock `json` in
        {
            "users": [
                {
                    "id": 1,
                    "firstName": "Ada",
                    "lastName": "Lovelace",
                    "email": "ada@example.com",
                    "address": {"city": "London", "country": "UK"},
                    "company": {"name": "Analytical Engines Ltd"},
                }
            ],
            "total": 1,
            "skip": 0,
            "limit": 100,
        }
    )


    assert page.total == 1  # one page 
    assert page.users[0].firstName == "Ada"
    assert page.users[0].address is not None
    assert page.users[0].address.city == "London"


