from warehouse_pipeline.cli import build_parser, main


def test_cli_init_exports_public_api() -> None:
    """CLI imports ok."""
    assert callable(build_parser)
    assert callable(main)
