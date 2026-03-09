from __future__ import annotations

from warehouse_pipeline.extract.models import DummyAddress, DummyCompany, DummyUser
from warehouse_pipeline.stage.map_users import map_users



def test_map_users_happy_path() -> None:
    """Users maps into `stg_customers` rows and a user lookup."""
    users = [
        DummyUser(
            id=1,
            firstName="Ada",
            lastName="Lovelace",
            email="ADA@EXAMPLE.COM",
            phone="123",
            address=DummyAddress(city="London", country="UK"),
            company=DummyCompany(name="Analytical Engines Ltd"),
        )
    ]

    mapped = map_users(users)

    assert len(mapped.rows) == 1
    assert mapped.rejects == []
    assert 1 in mapped.user_lookup

    row = mapped.rows[0]
    assert row.table_name == "stg_customers"
    assert row.values["customer_id"] == 1
    assert row.values["full_name"] == "Ada Lovelace"
    assert row.values["email"] == "ada@example.com"
    assert row.values["city"] == "London"
    assert row.values["country"] == "UK"   