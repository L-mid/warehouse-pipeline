from __future__ import annotations

from warehouse_pipeline.extract.source_contract import SourceAdapter
from warehouse_pipeline.extract.sources.square_orders_source import SquareOrdersSource


def get_source_adapter(source_system: str) -> SourceAdapter:
    """
    Resolve one source adapter from the `source_system` name.
    """
    if source_system == "square_orders":
        return SquareOrdersSource.from_env()

    raise ValueError(f"Unsupported source_system={source_system!r}")
