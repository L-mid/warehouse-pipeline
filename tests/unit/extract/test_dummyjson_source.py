from warehouse_pipeline.extract.sources.dummyjson_source import DummyJsonSource


def test_dummyjson_source_rejects_non_order_ts_watermark() -> None:
    src = DummyJsonSource()

    try:
        src.validate_watermark_column("updated_at")
    except ValueError as exc:
        assert "order_ts" in str(exc)
    else:
        raise AssertionError("expected ValueError")
