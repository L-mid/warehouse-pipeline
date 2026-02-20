from __future__ import annotations

from warehouse_pipeline.parsing.profiles.customers import parse_customer_row
from warehouse_pipeline.parsing.types import ParsedRow, RejectCode, RejectRow


def test_customer_happy_path() -> None:
    """Good fields parse into a row successfully."""
    raw = {
        "customer_id": "123",
        "full_name": "Ada Lovelace",
        "email": "ada@example.com",
        "signup_date": "2026-02-19",
        "country": "GB",
    }
    res = parse_customer_row(raw, source_row=1)
    assert isinstance(res, ParsedRow)
    assert res.values["customer_id"] == 123
    assert res.values["full_name"] == "Ada Lovelace"
    assert res.values["email"] == "ada@example.com"
    assert str(res.values["signup_date"]) == "2026-02-19"
    assert res.values["country"] == "GB"


def test_customer_missing_required_customer_id() -> None:
    """Missing `customer_id` -> rejected row."""
    raw = {"customer_id": "", "full_name": "Ada", "signup_date": "2026-02-19"}
    res = parse_customer_row(raw, source_row=2)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.missing_required
    assert "customer_id" in res.detail


def test_customer_invalid_int() -> None:
    """`customer_id` is not `int` -> rejected row."""
    raw = {"customer_id": "abc", "full_name": "Ada", "signup_date": "2026-02-19"}
    res = parse_customer_row(raw, source_row=3)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.invalid_int


def test_customer_invalid_date_uses_invalid_timestamp_code() -> None:
    """`signup_date` timestamp code not possible -> rejected row."""
    raw = {"customer_id": "1", "full_name": "Ada", "signup_date": "2026-02-30"}
    res = parse_customer_row(raw, source_row=4)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.invalid_timestamp
    assert "signup_date" in res.detail


def test_customer_unknown_field_rejected() -> None:
    """Extra unknown field -> rejected row."""
    raw = {"customer_id": "1", "full_name": "Ada", "signup_date": "2026-02-19", "extra": "x"}
    res = parse_customer_row(raw, source_row=5)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.unknown_field
    assert "extra" in res.detail