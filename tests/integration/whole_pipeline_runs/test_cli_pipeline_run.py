from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, LiteralString

import psycopg
import pytest

import warehouse_pipeline.orchestration.runner as runner_mod
from warehouse_pipeline.cli.main import main
from warehouse_pipeline.extract.bundles import ExtractBundle
from warehouse_pipeline.extract.models import (
    DummyAddress,
    DummyCart,
    DummyCartProduct,
    DummyProduct,
    DummyUser,
)
from warehouse_pipeline.extract.source_contract import PullResult


def _read_json(path: Path) -> dict:
    """Read `.json` from provided path."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _tail_lines(path: Path, *, n: int = 20) -> list[str]:
    """Show read lines from provided `path`, default limit is 20 lines."""
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return lines[-n:]


def _collect_pipeline_debug(*, dsn: str, run_artifacts_dir: Path) -> dict:
    """Collect diagnostic data from a run and return in a `debug` `dict`."""
    run_dirs = sorted(p for p in run_artifacts_dir.iterdir() if p.is_dir())

    manifest = {}
    log_tail: list[str] = []

    with psycopg.connect(dsn, autocommit=True) as conn:
        latest_row = conn.execute(
            """
            SELECT run_id::text
            FROM run_ledger
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()

        latest_run_id = latest_row[0] if latest_row is not None else None

        ledger_rows = conn.execute(
            """
            SELECT
                run_id::text,
                mode,
                COALESCE(snapshot_key, ''),
                status,
                COALESCE(error_message, ''),
                watermark_column,
                watermark_low::text,
                watermark_high::text
            FROM run_ledger
            ORDER BY started_at DESC
            LIMIT 5
            """
        ).fetchall()

        def _count(
            conn: psycopg.Connection[Any],
            query: LiteralString,
            params: Sequence[object] | None = None,
        ) -> int:
            """Run a `COUNT(*)` query safely, failing on a `None` row return."""
            row = conn.execute(query, params).fetchone()
            assert row is not None, f"COUNT query returned no rows: {query}"
            return int(row[0])

        table_counts = {
            "run_ledger": _count(conn, "SELECT COUNT(*) FROM run_ledger"),
            "stg_customers": _count(conn, "SELECT COUNT(*) FROM stg_customers"),
            "stg_products": _count(conn, "SELECT COUNT(*) FROM stg_products"),
            "stg_orders": _count(conn, "SELECT COUNT(*) FROM stg_orders"),
            "stg_order_items": _count(conn, "SELECT COUNT(*) FROM stg_order_items"),
            "reject_rows": _count(conn, "SELECT COUNT(*) FROM reject_rows"),
            "dq_results": _count(conn, "SELECT COUNT(*) FROM dq_results"),
            "dim_customer": _count(conn, "SELECT COUNT(*) FROM dim_customer"),
            "fact_orders": _count(conn, "SELECT COUNT(*) FROM fact_orders"),
            "fact_order_items": _count(conn, "SELECT COUNT(*) FROM fact_order_items"),
            "v_fact_orders_latest": _count(conn, "SELECT COUNT(*) FROM v_fact_orders_latest"),
        }

        dq_preview = conn.execute(
            """
            SELECT table_name, metric_name, passed, metric_value::text
            FROM dq_results
            ORDER BY table_name ASC, metric_name ASC
            LIMIT 20
            """
        ).fetchall()

        reject_preview = conn.execute(
            """
            SELECT table_name, reason_code, LEFT(reason_detail, 200)
            FROM reject_rows
            ORDER BY reject_id ASC
            LIMIT 20
            """
        ).fetchall()

    if latest_run_id is not None:
        latest_run_dir = run_artifacts_dir / latest_run_id
        manifest = _read_json(latest_run_dir / "manifest.json")
        log_tail = _tail_lines(latest_run_dir / "logs.jsonl", n=40)

    return {
        "run_dirs": [p.name for p in run_dirs],
        "manifest": manifest,
        "log_tail": log_tail,
        "ledger_rows": ledger_rows,
        "table_counts": table_counts,
        "dq_preview": dq_preview,
        "reject_preview": reject_preview,
    }


def _failure_blob(*, rc: int, debug: dict) -> str:
    """Formats a debuggable failure blob for these whole pipeline tests."""
    return (
        "pipeline CLI returned non-zero\n"
        f"rc={rc}\n"
        f"run_dirs={debug['run_dirs']}\n"
        f"manifest={json.dumps(debug['manifest'], indent=2, sort_keys=True)}\n"
        f"log_tail=\n" + "\n".join(debug["log_tail"]) + "\n"
        f"run_ledger_recent={debug['ledger_rows']}\n"
        f"table_counts={debug['table_counts']}\n"
        f"dq_preview={debug['dq_preview']}\n"
        f"reject_preview={debug['reject_preview']}\n"
    )


def _assert_artifacts_exist(manifest: dict) -> None:
    """Asserts that the key run artifacts exist on disk post run."""
    artifacts = manifest["artifacts"]
    assert Path(artifacts["run_dir"]).exists()
    assert Path(artifacts["manifest"]).exists()
    assert Path(artifacts["logs"]).exists()


## -- these exist to avoid http calls. and yes should probaby be in conftest/helpers later
def _fake_user(user_id: int, *, country: str = "UK") -> DummyUser:
    """Return a fake user's info."""
    return DummyUser(
        id=user_id,
        firstName=f"User{user_id}",
        lastName="Test",
        email=f"user{user_id}@example.com",
        phone="000000",
        address=DummyAddress(city="London", country=country),
        company=None,
    )


def _fake_product(product_id: int, *, title: str, price: float) -> DummyProduct:
    """Return a fake product's info."""
    return DummyProduct(
        id=product_id,
        title=title,
        category="widgets",
        price=price,
        stock=100,
        brand="Acme",
        discountPercentage=0.0,
        rating=4.5,
    )


def _fake_line(
    product_id: int,
    *,
    quantity: int,
    price: float,
    title: str | None = None,
    discounted_total: float | None = None,
) -> DummyCartProduct:
    """Return a fake cart product line."""
    total = round(quantity * price, 2)
    return DummyCartProduct(
        id=product_id,
        title=title or f"Product {product_id}",
        quantity=quantity,
        price=price,
        total=total,
        discountPercentage=0.0,
        discountedTotal=discounted_total if discounted_total is not None else total,
    )


def _fake_cart(
    cart_id: int,
    user_id: int,
    *lines: DummyCartProduct,
    discounted_total: float | None = None,
) -> DummyCart:
    """Return a fake cart's info."""
    total = round(sum(float(line.total) for line in lines), 2)
    discounted = round(
        discounted_total
        if discounted_total is not None
        else sum(float(line.discountedTotal or line.total) for line in lines),
        2,
    )
    total_quantity = sum(line.quantity for line in lines)

    return DummyCart(
        id=cart_id,
        userId=user_id,
        total=total,
        discountedTotal=discounted,
        totalProducts=len(lines),
        totalQuantity=total_quantity,
        products=list(lines),
    )


def _fake_live_bundle(*, carts: tuple[DummyCart, ...]) -> ExtractBundle:
    """
    Fake creating a fake extract bundle out
    of all the fake parsed info to hand over.
    """
    users = (
        _fake_user(1, country="UK"),
        _fake_user(2, country="CA"),
    )
    products = (
        _fake_product(100, title="Widget Basic", price=10.0),
        _fake_product(101, title="Widget Plus", price=15.0),
        _fake_product(200, title="Widget Max", price=40.0),
    )

    return ExtractBundle(
        mode="live",
        users=users,
        products=products,
        carts=carts,
        totals={
            "users": len(users),
            "products": len(products),
            "carts": len(carts),
        },
        pages_fetched={
            "users": 1,
            "products": 1,
            "carts": 1,
        },
        page_size=100,
    )


@pytest.mark.docker_required
@pytest.mark.heavy_integration
def test_cli_run_pipeline_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """Run a full run from CLI to result code on smoke data."""
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    rc = main(
        [
            "run",
            "--mode",
            "snapshot",
            "--snapshot",
            "smoke",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)

    assert rc == 0, _failure_blob(rc=rc, debug=debug)  # formatted error data on failure

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "snapshot"
    assert manifest["snapshot_key"] == "smoke"
    _assert_artifacts_exist(manifest)

    assert debug["table_counts"]["reject_rows"] == 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] == 1
    assert debug["table_counts"]["fact_order_items"] == 1
    assert debug["table_counts"]["v_fact_orders_latest"] == 1

    # run success!


@pytest.mark.docker_required
@pytest.mark.heavy_integration
def test_cli_run_pipeline_incremental_default_overlap_reprocesses_recent_rows(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """
    Show how overlap-window incrementals work.
    (patched with non-http calling mock extractor)

    The first run seeds the watermark with an explicit since/until,
    second run uses the default 7-day overlap against the prior watermark.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    # order_id=20 lands early in the synthetic year, so outside the trailing overlap
    old_order = _fake_cart(
        20,
        2,
        _fake_line(200, quantity=1, price=40.0, discounted_total=40.0),
        discounted_total=40.0,
    )

    # order_id=350 lands in mid-December under derive_order_ts, so inside the overlap
    recent_order_v1 = _fake_cart(
        350,
        1,
        _fake_line(100, quantity=1, price=10.0, discounted_total=10.0),
        _fake_line(101, quantity=1, price=15.0, discounted_total=15.0),
        discounted_total=25.0,
    )
    recent_order_v2 = _fake_cart(
        350,
        1,
        _fake_line(100, quantity=1, price=12.0, discounted_total=12.0),
        discounted_total=12.0,
    )

    fixed_high = datetime(2024, 12, 31, 0, 0, 0, tzinfo=UTC)
    seen_page_sizes: list[int] = []
    seen_windows = []

    class FakeDummyJsonSource:
        source_system = "dummyjson"

        def __init__(self) -> None:
            self._incremental_results = [
                PullResult(
                    bundle=_fake_live_bundle(carts=(old_order, recent_order_v1)),
                    meta={
                        "source_system": "dummyjson",
                        "native_incremental": False,
                        "selection_strategy": "full_pull_plus_client_side_filter",
                        "carts_pre_filter": 2,
                        "carts_post_filter": 2,
                    },
                ),
                PullResult(
                    bundle=_fake_live_bundle(carts=(recent_order_v2,)),
                    meta={
                        "source_system": "dummyjson",
                        "native_incremental": False,
                        "selection_strategy": "full_pull_plus_client_side_filter",
                        "carts_pre_filter": 2,
                        "carts_post_filter": 1,
                    },
                ),
            ]

        def validate_watermark_column(self, watermark_column: str) -> None:
            assert watermark_column == "order_ts"

        def default_high_watermark(
            self,
            *,
            watermark_column: str,
            run_started_at: datetime,
        ) -> datetime | None:
            assert watermark_column == "order_ts"
            return fixed_high

        def pull_full(self, *, page_size: int):
            raise AssertionError("test should not call pull_full in incremental mode")

        def pull_incremental(self, *, page_size: int, window):
            seen_page_sizes.append(page_size)
            seen_windows.append(window)
            assert self._incremental_results, "unexpected extra incremental pull"
            return self._incremental_results.pop(0)

    fake_source = FakeDummyJsonSource()

    monkeypatch.setattr(
        runner_mod,
        "get_source_adapter",
        lambda source_system: fake_source,
    )

    # first run: seed watermark explicitly
    rc1 = main(
        [
            "run",
            "--mode",
            "incremental",
            "--since",
            "2024-01-01T00:00:00+00:00",
            "--until",
            "2024-12-20T00:00:00+00:00",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )
    assert rc1 == 0

    # second run: prior watermark minus default 7-day overlap, high from adapter
    rc2 = main(
        [
            "run",
            "--mode",
            "incremental",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)
    assert rc2 == 0, _failure_blob(rc=rc2, debug=debug)

    assert seen_page_sizes == [100, 100]
    assert len(seen_windows) == 2

    first_window = seen_windows[0]
    assert first_window.watermark_column == "order_ts"
    assert first_window.prior_watermark is None
    assert first_window.low == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    assert first_window.high == datetime(2024, 12, 20, 0, 0, 0, tzinfo=UTC)

    second_window = seen_windows[1]
    assert second_window.watermark_column == "order_ts"
    assert second_window.prior_watermark == datetime(2024, 12, 20, 0, 0, 0, tzinfo=UTC)
    assert second_window.low == datetime(2024, 12, 13, 0, 0, 0, tzinfo=UTC)
    assert second_window.high == fixed_high

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "incremental"

    # orchestration-owned metadata only
    ew = manifest["extraction_window"]
    assert ew["mode"] == "incremental"
    assert ew["watermark_column"] == "order_ts"
    assert ew["prior_watermark"] == "2024-12-20T00:00:00+00:00"
    assert ew["low"] == "2024-12-13T00:00:00+00:00"
    assert ew["high"] == "2024-12-31T00:00:00+00:00"
    assert ew["overlap_applied_s"] == 7 * 24 * 60 * 60

    # source-owned metadata lives under extract.source
    source = manifest["extract"]["source"]
    assert source["source_system"] == "dummyjson"
    assert source["native_incremental"] is False
    assert source["selection_strategy"] == "full_pull_plus_client_side_filter"
    assert source["carts_pre_filter"] == 2
    assert source["carts_post_filter"] == 1

    assert debug["table_counts"]["run_ledger"] == 2
    assert debug["table_counts"]["fact_orders"] == 2
    assert debug["table_counts"]["fact_order_items"] == 2

    with psycopg.connect(dsn, autocommit=True) as conn:
        run_ids = conn.execute(
            """
            SELECT run_id::text
            FROM run_ledger
            WHERE mode = 'incremental'
            ORDER BY started_at ASC
            """
        ).fetchall()
        assert len(run_ids) == 2

        first_run_id = run_ids[0][0]
        second_run_id = run_ids[1][0]

        order_20 = conn.execute(
            """
            SELECT total_usd::text, source_run_id::text
            FROM fact_orders
            WHERE order_id = 20
            """
        ).fetchone()
        order_350 = conn.execute(
            """
            SELECT total_usd::text, source_run_id::text
            FROM fact_orders
            WHERE order_id = 350
            """
        ).fetchone()

        items_20 = conn.execute(
            """
            SELECT line_id, net_usd::text, source_run_id::text
            FROM fact_order_items
            WHERE order_id = 20
            ORDER BY line_id
            """
        ).fetchall()
        items_350 = conn.execute(
            """
            SELECT line_id, product_id, net_usd::text, source_run_id::text
            FROM fact_order_items
            WHERE order_id = 350
            ORDER BY line_id
            """
        ).fetchall()

    assert order_20 == ("40.00", first_run_id)
    assert order_350 == ("12.00", second_run_id)

    assert items_20 == [(1, "40.00", first_run_id)]
    assert items_350 == [(1, 100, "12.00", second_run_id)]


@pytest.mark.heavy_integration
@pytest.mark.non_ci
@pytest.mark.live_http
@pytest.mark.docker_required
def test_cli_run_pipeline_live_dummyjson_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """
    Run the full pipeline from CLI using the real live `DummyJSON` extraction path.
    Requires internet, CI does not run this test.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    rc = main(
        [
            "run",
            "--mode",
            "live",
            "--page-size",
            "100",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)

    assert rc == 0, _failure_blob(rc=rc, debug=debug)

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "live"
    assert manifest["snapshot_key"] is None
    _assert_artifacts_exist(manifest)

    # anything > 0 ok for live
    assert manifest["extract"]["counts"]["users"] > 0
    assert manifest["extract"]["counts"]["products"] > 0
    assert manifest["extract"]["counts"]["carts"] > 0

    # pages >= 1 is expected from extraction
    assert manifest["extract"]["pages_fetched"]["users"] >= 1
    assert manifest["extract"]["pages_fetched"]["products"] >= 1
    assert manifest["extract"]["pages_fetched"]["carts"] >= 1

    # in order to pass, dq checks must pass to avoid raise,
    # implicitly testing reject count for live mode is within tol

    # inner db and dq checks look non empty and ok
    assert debug["table_counts"]["stg_customers"] > 0
    assert debug["table_counts"]["stg_products"] > 0
    assert debug["table_counts"]["stg_orders"] > 0
    assert debug["table_counts"]["stg_order_items"] > 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] > 0
    assert debug["table_counts"]["fact_order_items"] > 0
    assert debug["table_counts"]["v_fact_orders_latest"] > 0


@pytest.mark.heavy_integration
@pytest.mark.non_ci
@pytest.mark.live_http
@pytest.mark.docker_required
def test_cli_run_pipeline_incremental_dummyjson_happy_path(
    reinit_schema,
    dsn: str,
    run_artifacts_dir,
    monkeypatch,
) -> None:
    """
    Run the full pipeline from CLI using incremental mode against live DummyJson.

    This is still a windowed live pull backed by a synthetic `DummyJson` `order_ts`,
    so the test only proves:
    - the extraction window resolves,
    - the run succeeds,
    - the manifest/log/ledger carry the window metadata.
    """
    monkeypatch.setenv("WAREHOUSE_DSN", dsn)

    rc = main(
        [
            "run",
            "--mode",
            "incremental",
            "--page-size",
            "100",
            "--since",
            "2024-01-01T00:00:00+00:00",
            "--until",
            "2025-01-01T00:00:00+00:00",
            "--runs-root",
            str(run_artifacts_dir),
        ]
    )

    debug = _collect_pipeline_debug(dsn=dsn, run_artifacts_dir=run_artifacts_dir)
    assert rc == 0, _failure_blob(rc=rc, debug=debug)

    manifest = debug["manifest"]
    assert manifest["status"] == "succeeded"
    assert manifest["mode"] == "incremental"
    assert manifest["snapshot_key"] is None
    _assert_artifacts_exist(manifest)

    extraction_window = manifest["extraction_window"]
    assert extraction_window["mode"] == "incremental"
    assert extraction_window["watermark_column"] == "order_ts"
    assert extraction_window["low"] == "2024-01-01T00:00:00+00:00"
    assert extraction_window["high"] == "2025-01-01T00:00:00+00:00"

    source = manifest["extract"]["source"]
    assert source["source_system"] == "dummyjson"
    assert source["native_incremental"] is False
    assert source["selection_strategy"] == "full_pull_plus_client_side_filter"
    assert source["watermark_column"] == "order_ts"
    assert source["low"] == "2024-01-01T00:00:00+00:00"
    assert source["high"] == "2025-01-01T00:00:00+00:00"
    assert source["carts_pre_filter"] > 0
    assert source["carts_post_filter"] > 0
    assert source["carts_post_filter"] <= source["carts_pre_filter"]

    assert manifest["extract"]["mode"] == "incremental"
    assert manifest["extract"]["counts"]["users"] > 0
    assert manifest["extract"]["counts"]["products"] > 0
    assert manifest["extract"]["counts"]["carts"] > 0

    assert debug["table_counts"]["stg_customers"] > 0
    assert debug["table_counts"]["stg_products"] > 0
    assert debug["table_counts"]["stg_orders"] > 0
    assert debug["table_counts"]["stg_order_items"] > 0
    assert debug["table_counts"]["dq_results"] > 0
    assert debug["table_counts"]["fact_orders"] > 0
    assert debug["table_counts"]["fact_order_items"] > 0
    assert debug["table_counts"]["v_fact_orders_latest"] > 0
