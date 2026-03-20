from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import UUID

from psycopg import Connection

from warehouse_pipeline.db.connect import connect
from warehouse_pipeline.db.run_ledger import (
    RunStart,
    create_run,
    get_last_successful_watermark,
    mark_run_failed,
    mark_run_succeeded,
    record_cursor_state,
    record_extraction_window,
)
from warehouse_pipeline.dq.gates import (
    GateDecision,
    evaluate_model_gates,
    render_dq_summary,
)
from warehouse_pipeline.dq.runner import DQRunSummary, run_model_dq
from warehouse_pipeline.extract import read_snapshot_extract
from warehouse_pipeline.extract.contracts import RawExtract
from warehouse_pipeline.extract.source_registry import get_source_adapter
from warehouse_pipeline.orchestration.contract import RunManifest, RunSpec
from warehouse_pipeline.orchestration.extraction_window import (
    ExtractionWindow,
    resolve_extraction_window,
)
from warehouse_pipeline.orchestration.logging import RunLogger
from warehouse_pipeline.orchestration.manifest import write_manifest
from warehouse_pipeline.publish.views import PublishResult, apply_views
from warehouse_pipeline.stage.load import load_square_batches
from warehouse_pipeline.stage.map_square_orders import map_square_orders
from warehouse_pipeline.transform.warehouse_build import WarehouseBuildResult, build_warehouse


class PipelineGateFailed(RuntimeError):
    """Raised internally if a run fails the DQ gates."""

    def __init__(self, decision: GateDecision) -> None:
        """Impose GateDescision onto this class."""
        self.decision = decision
        super().__init__(
            f"pipeline run failed DQ gates: "
            f"{len(decision.failures)} hard failure(s), "
            f"{len(decision.warnings)} warning(s)"
        )


def _utcnow() -> datetime:
    """Coerces to UTC datetime."""
    return datetime.now(UTC)


def _run_artifacts_dir(spec: RunSpec, run_id: UUID) -> Path:
    """Creates and resolve then return the artifacts directory using the provided `run_id`."""
    return spec.runs_root.resolve() / str(run_id)


def _default_incremental_high(spec: RunSpec, *, started_at: datetime) -> datetime | None:
    """
    Pick a sane fallback high watermark when the source does not expose a real
    upstream timestamp cursor (DummyJson does not).
    """
    adapter = get_source_adapter(spec.source_system)
    return adapter.default_high_watermark(
        watermark_column=spec.watermark_column,
        run_started_at=started_at,
    )


def _resolve_and_record_window(
    conn: Connection,
    *,
    spec: RunSpec,
    run_id: UUID,
    started_at: datetime,
    logger: RunLogger,
) -> ExtractionWindow:
    """
    Compute the extraction window for an incremental run,
    stamp it onto run_ledger, and log it.
    """
    adapter = get_source_adapter(spec.source_system)
    adapter.validate_watermark_column(spec.watermark_column)

    prior = get_last_successful_watermark(
        conn,
        source_system=spec.source_system,
        watermark_column=spec.watermark_column,
    )

    window = resolve_extraction_window(
        watermark_column=spec.watermark_column,
        prior_watermark=prior,
        run_started_at=started_at,
        since=spec.since,
        until=spec.until,
        overlap=spec.overlap_window,
        default_high=_default_incremental_high(spec, started_at=started_at),
    )

    record_extraction_window(
        conn,
        run_id=run_id,
        watermark_column=window.watermark_column,
        watermark_low=window.low,
        watermark_high=window.high,
    )

    record_cursor_state(
        conn,
        run_id=run_id,
        cursor_state={
            "source_system": spec.source_system,
            "watermark_column": window.watermark_column,
            "low": window.low.isoformat(),
            "high": window.high.isoformat(),
            "low_boundary": "inclusive",
            "high_boundary": "exclusive",
        },
    )

    conn.commit()

    logger.event(
        "extraction_window_resolved",
        source_system=spec.source_system,
        watermark_column=window.watermark_column,
        low=window.low.isoformat(),
        high=window.high.isoformat(),
        prior_watermark=prior.isoformat() if prior else None,
        overlap_s=window.overlap.total_seconds(),
        is_first_run=window.is_first_run,
    )

    return window


def _extract_raw(
    spec: RunSpec,
    *,
    window: ExtractionWindow | None = None,
) -> tuple[RawExtract, dict[str, Any]]:
    if spec.mode == "snapshot":
        extract = read_snapshot_extract(
            snapshot_root=spec.resolved_snapshot_root(),
            snapshot_key=spec.snapshot_key,
        )
        return extract, {}

    adapter = get_source_adapter(spec.source_system)

    if spec.mode == "live":
        result = adapter.pull_full(page_size=spec.page_size)
        return result.extract, result.meta

    assert window is not None, "incremental mode requires a resolved window"
    result = adapter.pull_incremental(page_size=spec.page_size, window=window)
    return result.extract, result.meta


def _summarize_extract(
    extract: RawExtract,
    *,
    mode_override: str | None = None,
) -> dict[str, Any]:
    """Summarizes extraction results and return its `dict`."""

    return {
        "mode": mode_override or extract.mode,
        "snapshot_key": extract.snapshot_key,
        "counts": {name: len(rows) for name, rows in extract.entities.items()},
        "totals": dict(extract.totals),
        "pages_fetched": dict(extract.pages_fetched),
        "page_size": extract.page_size,
        "source_paths": dict(extract.source_paths),
    }


def _summarize_extraction_window(window: ExtractionWindow | None) -> dict[str, Any]:
    """Summarize orchestration-owned extraction window metadata."""
    if window is None:
        return {}

    return {
        "mode": "incremental",
        "watermark_column": window.watermark_column,
        "prior_watermark": (
            window.prior_watermark.isoformat() if window.prior_watermark is not None else None
        ),
        "low": window.low.isoformat(),
        "high": window.high.isoformat(),
        "overlap_applied_s": int(window.overlap.total_seconds()),
        "is_first_run": window.is_first_run,
    }


def _summarize_stage(stage_results: dict[str, Any]) -> dict[str, Any]:
    """Summarizes staging results per table."""
    return {table_name: asdict(result) for table_name, result in stage_results.items()}


def _summarize_dq(summaries: tuple[DQRunSummary, ...]) -> dict[str, Any]:
    """Summarizes all DQ metrics results."""
    return {
        summary.table_name: {
            "metrics_written": summary.metrics_written,
            "failed_metrics": summary.failed_metrics,
            "passed": summary.passed,
        }
        for summary in summaries
    }


def _summarize_gate(decision: GateDecision | None) -> dict[str, Any]:
    """Summarizes the DQ gate's verdict."""
    if decision is None:
        return {}

    return {
        "mode": decision.mode,
        "passed": decision.passed,
        "failures": [asdict(x) for x in decision.failures],
        "warnings": [asdict(x) for x in decision.warnings],
    }


def _summarize_transform(result: WarehouseBuildResult | None) -> dict[str, Any]:
    """Summarizes the transformations building step."""
    if result is None:
        return {}
    return {
        "step_name": result.step_name,
        "files_ran": list(result.files_ran),
        "run_id": str(result.run_id),
    }


def _summarize_publish(result: PublishResult | None) -> dict[str, Any]:
    """Summarizes the publishing step for views."""
    if result is None:
        return {}
    return {
        "files_ran": list(result.files_ran),
        "metrics_available": list(result.metrics_available),
    }


def run_pipeline(spec: RunSpec, *, database_url: str | None = None) -> RunManifest:
    """
    Run one end-to-end pipeline execution.
    - create `run_ledger` row, and commit that
    - stage, and commit that
    - dq, and commit that
    - gate, it's read only no commit here
    - transform and publish, and commit that together
    - mark final `run_ledger` status and commit that
    - end run.
    """
    started_at = _utcnow()
    finished_at = started_at
    timings_s: dict[str, float] = {}

    # collect summaries
    extract_summary: dict[str, Any] = {}
    stage_summary: dict[str, Any] = {}
    dq_summary: dict[str, Any] = {}
    gate_summary: dict[str, Any] = {}
    transform_summary: dict[str, Any] = {}
    publish_summary: dict[str, Any] = {}
    error_message: str | None = None
    status = "failed"

    with connect(database_url) as conn:
        # Connect to initalize the run ledger before anything else
        # keeps it commited even after rollback if error
        run_id = create_run(
            conn,
            entry=RunStart(
                mode=spec.mode,
                source_system=spec.source_system,
                snapshot_key=spec.snapshot_key if spec.mode == "snapshot" else None,
                git_sha=spec.git_sha,
                args_json={
                    "mode": spec.mode,
                    "snapshot_key": spec.snapshot_key,
                    "page_size": spec.page_size,
                    "transform_step": spec.transform_step,
                    **dict(spec.args_json),
                },
            ),
        )
        conn.commit()  # commited.

        ## -- inits.
        run_dir = _run_artifacts_dir(spec, run_id)
        logger = RunLogger(run_id=run_id, log_path=run_dir / "logs.jsonl")

        logger.event("run_started", mode=spec.mode, source_system=spec.source_system)

        try:
            ## -- extraction window for incremental mode
            window: ExtractionWindow | None = None
            extraction_window_summary: dict[str, Any] = {}
            source_meta: dict[str, Any] = {}

            if spec.mode == "incremental":
                window = _resolve_and_record_window(
                    conn,
                    spec=spec,
                    run_id=run_id,
                    started_at=started_at,
                    logger=logger,
                )
                extraction_window_summary = _summarize_extraction_window(window)

            ## -- extraction
            t0 = perf_counter()
            logger.phase_started("extract")
            raw_extract, source_meta = _extract_raw(spec, window=window)
            extract_summary = _summarize_extract(raw_extract, mode_override=spec.mode)
            if source_meta:
                extract_summary["source"] = source_meta
            timings_s["extract"] = perf_counter() - t0
            logger.phase_finished(
                "extract",
                duration_s=timings_s["extract"],
                counts=extract_summary["counts"],
            )

            ## -- map obtained to staging
            t0 = perf_counter()
            logger.phase_started("stage_map")
            mapped_square = map_square_orders(raw_extract.entities.get("orders", ()))
            timings_s["stage_map"] = perf_counter() - t0
            logger.phase_finished(
                "stage_map",
                duration_s=timings_s["stage_map"],
                order_rows=len(mapped_square.order_rows),
                order_line_rows=len(mapped_square.order_line_rows),
                tender_rows=len(mapped_square.tender_rows),
                rejects=len(mapped_square.rejects),
            )

            ## -- stage load
            t0 = perf_counter()
            logger.phase_started("stage_load")
            stage_results = load_square_batches(
                conn,
                run_id=run_id,
                square=mapped_square,
            )
            conn.commit()
            stage_summary = _summarize_stage(stage_results)
            timings_s["stage_load"] = perf_counter() - t0
            logger.phase_finished(
                "stage_load",
                duration_s=timings_s["stage_load"],
                tables=list(stage_summary),
            )

            # -- transform + publish
            t0 = perf_counter()
            logger.phase_started("transform_publish")
            transform_result = build_warehouse(
                conn,
                run_id=run_id,
                step_name=spec.transform_step,
            )
            publish_result = apply_views(conn)
            conn.commit()

            transform_summary = _summarize_transform(transform_result)
            publish_summary = _summarize_publish(publish_result)
            timings_s["transform_publish"] = perf_counter() - t0
            logger.phase_finished(
                "transform_publish",
                duration_s=timings_s["transform_publish"],
                transform_files=transform_summary.get("files_ran", []),
                publish_files=publish_summary.get("files_ran", []),
            )

            # -- DQ + gate
            t0 = perf_counter()
            logger.phase_started("dq")
            dq_results = run_model_dq(conn, run_id=run_id)
            gate_decision = evaluate_model_gates(conn, run_id=run_id)

            dq_summary = _summarize_dq(dq_results)
            gate_summary = _summarize_gate(gate_decision)
            gate_summary["summary_text"] = render_dq_summary(
                conn,
                run_id=run_id,
                decision=gate_decision,
            )

            conn.commit()

            timings_s["dq"] = perf_counter() - t0
            logger.phase_finished(
                "dq",
                duration_s=timings_s["dq"],
                passed=gate_decision.passed,
                hard_failures=len(gate_decision.failures),
                warnings=len(gate_decision.warnings),
            )

            if not gate_decision.passed:
                raise PipelineGateFailed(gate_decision)

            ## -- succeed the run.

            mark_run_succeeded(conn, run_id=run_id)
            conn.commit()  # commit that we suceeded and are done

            # status updated here too and timer stopped, for manifest
            status = "succeeded"
            finished_at = _utcnow()

            logger.event("run_succeeded")

        except Exception as exc:
            conn.rollback()
            error_message = str(exc)
            finished_at = _utcnow()
            logger.error("pipeline", error_message=error_message)

            mark_run_failed(conn, run_id=run_id, error_message=error_message)
            conn.commit()  # commit in the legder that the run failed.

            # for manifest
            status = "failed"
            logger.event("run_failed")

        manifest = RunManifest(
            run_id=run_id,
            mode=spec.mode,
            status=status,
            source_system=spec.source_system,
            extraction_window=extraction_window_summary,
            snapshot_key=spec.snapshot_key if spec.mode == "snapshot" else None,  # default snapshot
            started_at=started_at,
            finished_at=finished_at,
            extract=extract_summary,
            stage=stage_summary,
            dq=dq_summary,
            gate=gate_summary,
            transform=transform_summary,
            publish=publish_summary,
            timings_s={k: round(v, 6) for k, v in timings_s.items()},
            artifacts={
                "run_dir": str(run_dir),
                "manifest": str(run_dir / "manifest.json"),
                "logs": str(run_dir / "logs.jsonl"),
            },
            error_message=error_message,
        )
        write_manifest(run_dir=run_dir, manifest=manifest)

        # return itself as a summary too. good for tests
        return manifest
