from __future__ import annotations

from warehouse_pipeline.parsing.profiles.retail_transactions import parse_retail_transaction_row
from warehouse_pipeline.parsing.types import ParsedRow, RejectCode, RejectRow

def test_retail_happy_path() -> None:
    """Good fields parse into a row successfully."""
    raw = {
        "date": "2026-02-19",
        "week": "8",
        "sku": "SKU-123",
        "product_category": "Apparel",
        "gender": "Men",
        "marketplace": "Amazon",
        "fulfillment": "FBA",
        "color": "Black",
        "size": "M",
        "list_price": "19.99",
        "discount_pct": "0.10",
        "promo_type": "None",
        "ad_spend": "5.00",
        "impressions": "1000",
        "clicks": "25",
        "cvr": "0.025",
        "units_sold": "3",
        "revenue": "53.97",
        "rating": "4.2",
        "reviews": "120",
        "competitor_price_index": "0.95",
        "stock_on_hand": "88",
        "stockout_flag": "0",
        "holiday_flag": "0",
    }
    res = parse_retail_transaction_row(raw, source_row=1)
    assert isinstance(res, ParsedRow)

    assert str(res.values["date"]) == "2026-02-19"
    assert res.values["week"] == 8
    assert res.values["sku"] == "SKU-123"
    assert res.values["product_category"] == "Apparel"
    assert res.values["gender"] == "Men"
    assert res.values["marketplace"] == "Amazon"
    assert res.values["fulfillment"] == "FBA"

    assert res.values["color"] == "Black"
    assert res.values["size"] == "M"

    # numeric(12,2) quantization: parsing all numerics via numeric(12,2).
    # this will round, ex: cvr=0.0329 to 0.03, competitor_price_index=0.961 to 0.96.
    assert str(res.values["list_price"]) == "19.99"
    assert str(res.values["discount_pct"]) == "0.10"
    assert str(res.values["ad_spend"]) == "5.00"
    assert str(res.values["cvr"]) == "0.03"  # 0.03 with ROUND_HALF_UP used in numeric normalization
    assert res.values["impressions"] == 1000
    assert res.values["clicks"] == 25
    assert res.values["units_sold"] == 3
    assert str(res.values["revenue"]) == "53.97"
    assert str(res.values["rating"]) == "4.20"
    assert res.values["reviews"] == 120
    assert str(res.values["competitor_price_index"]) == "0.95"
    assert res.values["stock_on_hand"] == 88
    assert res.values["stockout_flag"] == 0
    assert res.values["holiday_flag"] == 0


def test_retail_missing_required_sku() -> None:
    """Missing `sku` -> rejected row."""
    raw = {
        "date": "2026-02-19",
        "week": "8",
        "sku": "",
        "product_category": "Apparel",
        "gender": "Men",
        "marketplace": "Amazon",
        "fulfillment": "FBA",
        "list_price": "19.99",
        "discount_pct": "0.10",
        "promo_type": "None",
        "ad_spend": "5.00",
        "impressions": "1000",
        "clicks": "25",
        "cvr": "0.025",
        "units_sold": "3",
        "revenue": "53.97",
        "rating": "4.2",
        "reviews": "120",
        "competitor_price_index": "0.95",
        "stock_on_hand": "88",
        "stockout_flag": "0",
        "holiday_flag": "0",
    }
    res = parse_retail_transaction_row(raw, source_row=2)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.missing_required
    assert "sku" in res.reason_detail


def test_retail_invalid_int_week() -> None:
    """`week` is not int -> rejected row."""
    raw = {
        "date": "2026-02-19",
        "week": "eight",
        "sku": "SKU-123",
        "product_category": "Apparel",
        "gender": "Men",
        "marketplace": "Amazon",
        "fulfillment": "FBA",
        "list_price": "19.99",
        "discount_pct": "0.10",
        "promo_type": "None",
        "ad_spend": "5.00",
        "impressions": "1000",
        "clicks": "25",
        "cvr": "0.025",
        "units_sold": "3",
        "revenue": "53.97",
        "rating": "4.2",
        "reviews": "120",
        "competitor_price_index": "0.95",
        "stock_on_hand": "88",
        "stockout_flag": "0",
        "holiday_flag": "0",
    }
    res = parse_retail_transaction_row(raw, source_row=3)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.invalid_int
    assert "week" in res.reason_detail


def test_retail_invalid_date_uses_invalid_timestamp_code() -> None:
    """Bad `date` -> rejected row."""
    raw = {
        "date": "2026-02-30",
        "week": "8",
        "sku": "SKU-123",
        "product_category": "Apparel",
        "gender": "Men",
        "marketplace": "Amazon",
        "fulfillment": "FBA",
        "list_price": "19.99",
        "discount_pct": "0.10",
        "promo_type": "None",
        "ad_spend": "5.00",
        "impressions": "1000",
        "clicks": "25",
        "cvr": "0.025",
        "units_sold": "3",
        "revenue": "53.97",
        "rating": "4.2",
        "reviews": "120",
        "competitor_price_index": "0.95",
        "stock_on_hand": "88",
        "stockout_flag": "0",
        "holiday_flag": "0",
    }
    res = parse_retail_transaction_row(raw, source_row=4)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.invalid_timestamp
    assert "date" in res.reason_detail


def test_retail_unknown_field_rejected() -> None:
    """Extra unknown field -> rejected row."""
    raw = {
        "date": "2026-02-19",
        "week": "8",
        "sku": "SKU-123",
        "product_category": "Apparel",
        "gender": "Men",
        "marketplace": "Amazon",
        "fulfillment": "FBA",
        "list_price": "19.99",
        "discount_pct": "0.10",
        "promo_type": "None",
        "ad_spend": "5.00",
        "impressions": "1000",
        "clicks": "25",
        "cvr": "0.025",
        "units_sold": "3",
        "revenue": "53.97",
        "rating": "4.2",
        "reviews": "120",
        "competitor_price_index": "0.95",
        "stock_on_hand": "88",
        "stockout_flag": "0",
        "holiday_flag": "0",
        "extra": "x",
    }
    res = parse_retail_transaction_row(raw, source_row=5)
    assert isinstance(res, RejectRow)
    assert res.reason_code == RejectCode.unknown_field
    assert "extra" in res.reason_detail



#