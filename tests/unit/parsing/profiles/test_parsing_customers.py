from __future__ import annotations

from warehouse_pipeline.parsing.profiles.customers import parse_customer_row
from warehouse_pipeline.parsing.types import ParsedRow, RejectCode, RejectRow


def test_customer_happy_path() -> None:
    """Good fields parse into a row successfully."""
    raw = {
        "customer_id": "dE014d010c7ab0c",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "subscription_date": "2021-07-26",
        "country": "GB",
        "company": "Stewart-Flynn",
        "city": "Rowlandberg",
        "phone_1": "846-790-4623x4715",
        "phone_2": "(422)787-2331x71127",
        "website": "http://example.com/",
    }
    res = parse_customer_row(raw, source_row=1)
    assert isinstance(res, ParsedRow)
    
    assert res.values["customer_id"] == "dE014d010c7ab0c"

    # `full_name` derived from `full_name` + `last_name`: should exist
    assert res.values["first_name"] == "Ada"
    assert res.values["last_name"] == "Lovelace"
    assert res.values["full_name"] == "Ada Lovelace"

    assert res.values["email"] == "ada@example.com"
    assert str(res.values["subscription_date"]) == "2021-07-26"
    assert res.values["country"] == "GB"
    assert res.values["company"] == "Stewart-Flynn"
    assert res.values["city"] == "Rowlandberg"
    assert res.values["phone_1"] == "846-790-4623x4715"
    assert res.values["phone_2"] == "(422)787-2331x71127"
    assert res.values["website"] == "http://example.com/"



def test_customer_missing_required_customer_id() -> None:
    """Missing `customer_id` -> rejected row."""
    raw = {
        "customer_id": "",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "subscription_date": "2021-07-26",
    }
    res = parse_customer_row(raw, source_row=2)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.missing_required
    assert "customer_id" in res.reason_detail


def test_customer_invalid_date_uses_invalid_timestamp_code() -> None:
    """`subscription_date` timestamp code not possible -> rejected row."""
    raw = {
        "customer_id": "dE014d010c7ab0c",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "subscription_date": "2021-02-30",
    }
    res = parse_customer_row(raw, source_row=3)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.invalid_timestamp
    assert "subscription_date" in res.reason_detail


def test_customer_unknown_field_rejected() -> None:
    """Extra unknown field -> rejected row."""
    raw = {
        "customer_id": "dE014d010c7ab0c",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "subscription_date": "2021-07-26",
        "extra": "x",
    }
    res = parse_customer_row(raw, source_row=4)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.unknown_field
    assert "extra" in res.reason_detail