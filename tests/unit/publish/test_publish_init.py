from __future__ import annotations

import warehouse_pipeline.publish as publish
from warehouse_pipeline.publish.views import apply_views, list_metric_queries, run_metric_query


def test_publish_init_exports_public_api() -> None:
    """Init import paths work"""
    assert set(publish.__all__) == {
        "MetricQueryResult",
        "PublishResult",
        "apply_views",
        "list_metric_queries",
        "run_metric_query",
    }

    assert publish.apply_views is apply_views
    assert publish.list_metric_queries is list_metric_queries
    assert publish.run_metric_query is run_metric_query