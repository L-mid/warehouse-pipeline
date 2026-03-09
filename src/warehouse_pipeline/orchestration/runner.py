from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import UUID



from warehouse_pipeline.db.connect import connect
from warehouse_pipeline.db.run_ledger import RunStart, create_run, mark_run_failed, mark_run_succeeded
from warehouse_pipeline.dq.gates import GateDecision, evaluate_stage_gates
from warehouse_pipeline.dq.runner import DQRunSummary, run_stage_dq
from warehouse_pipeline.extract import fetch_live_bundle, read_snapshot_bundle
from warehouse_pipeline.extract.bundles import ExtractBundle
from warehouse_pipeline.orchestration.contract import RunManifest, RunSpec
from warehouse_pipeline.orchestration.logging import RunLogger
from warehouse_pipeline.orchestration.manifest import write_manifest
from warehouse_pipeline.publish.views import PublishResult, apply_views
from warehouse_pipeline.stage.load import load_mapped_batches
from warehouse_pipeline.stage.map_carts import map_carts
from warehouse_pipeline.stage.map_products import map_products
from warehouse_pipeline.stage.map_users import map_users
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

def _extract_bundle(spec: RunSpec) -> ExtractBundle:
    """Extractions of either the `snapshot` mode OR the `live` mode expectaions."""
    # snapshot path
    if spec.mode == "snapshot":
        snapshot_root = spec.resolved_snapshot_root()
        return read_snapshot_bundle(snapshot_root=snapshot_root, snapshot_key=spec.snapshot_key)
    # live mode path
    return fetch_live_bundle(page_size=spec.page_size)


def _summarize_extract(bundle: ExtractBundle) -> dict[str, Any]:
    """Summarizes extraction results and return its `dict`."""
    return {
        "mode": bundle.mode,
        "snapshot_key": bundle.snapshot_key,
        "counts": {
            "users": len(bundle.users),
            "products": len(bundle.products),
            "carts": len(bundle.carts),
        },
        "totals": dict(bundle.totals),
        "pages_fetched": dict(bundle.pages_fetched),
        "page_size": bundle.page_size,
        "source_paths": dict(bundle.source_paths),
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
        conn.commit()   # commited.


        ## -- inits.
        run_dir = _run_artifacts_dir(spec, run_id)
        logger = RunLogger(run_id=run_id, log_path=run_dir / "logs.jsonl")
    
        logger.event("run_started", mode=spec.mode, source_system=spec.source_system)

        try:
            
            ## -- extraction
            t0 = perf_counter()
            logger.phase_started("extract")
            bundle = _extract_bundle(spec)
            extract_summary = _summarize_extract(bundle)
            timings_s["extract"] = perf_counter() - t0
            logger.phase_finished("extract", duration_s=timings_s["extract"], counts=extract_summary["counts"])


            ## -- map obtained to staging 
            t0 = perf_counter()
            logger.phase_started("stage_map")
            mapped_users = map_users(bundle.users)
            mapped_products = map_products(bundle.products)
            mapped_carts = map_carts(
                bundle.carts,
                product_lookup=mapped_products.product_lookup,
                user_lookup=mapped_users.user_lookup,
            )            
            timings_s["stage_map"] = perf_counter() - t0
            logger.phase_finished(
                "stage_map",
                duration_s=timings_s["stage_map"],
                customer_rows=len(mapped_users.rows),
                product_rows=len(mapped_products.rows),
                order_rows=len(mapped_carts.order_rows),
                order_item_rows=len(mapped_carts.order_item_rows),
            )           

            ## -- staging
            t0 = perf_counter()
            logger.phase_started("stage_load")
            stage_results = load_mapped_batches(
                conn,
                run_id=run_id,
                users=mapped_users,
                products=mapped_products,
                carts=mapped_carts,
            )
            conn.commit()       # commit staged tables.
            stage_summary = _summarize_stage(stage_results)
            timings_s["stage_load"] = perf_counter() - t0
            logger.phase_finished("stage_load", duration_s=timings_s["stage_load"], tables=list(stage_summary))

            ## -- dq
            t0 = perf_counter()
            logger.phase_started("dq")
            dq_results = run_stage_dq(conn, run_id=run_id)
            conn.commit()       # commit dq table checks in 
            dq_summary = _summarize_dq(dq_results)
            timings_s["dq"] = perf_counter() - t0
            logger.phase_finished(
                "dq",
                duration_s=timings_s["dq"],
                tables=list(dq_summary),
            )

            ## -- gate
            t0 = perf_counter()
            logger.phase_started("gate")
            gate_decision = evaluate_stage_gates(conn, run_id=run_id)
            gate_summary = _summarize_gate(gate_decision)
            timings_s["gate"] = perf_counter() - t0
            logger.phase_finished(
                "gate",
                duration_s=timings_s["gate"],
                passed=gate_decision.passed,
                failures=len(gate_decision.failures),
                warnings=len(gate_decision.warnings),
            )
            if not gate_decision.passed:
                raise PipelineGateFailed(gate_decision)     # will error if metrics not within tol
            # no commit


            ## -- transform, publish results.
            t0 = perf_counter()
            logger.phase_started("transform_publish")
            transform_result = build_warehouse(
                conn,
                run_id=run_id,
                step_name=spec.transform_step,
            )    
            publish_result = apply_views(conn) if spec.publish_views else None
            conn.commit()   # commit transforms and anything published together
            transform_summary = _summarize_transform(transform_result)
            publish_summary = _summarize_publish(publish_result)
            timings_s["transform_publish"] = perf_counter() - t0
            logger.phase_finished(
                "transform_publish",
                duration_s=timings_s["transform_publish"],
                transform_files=transform_summary.get("files_ran", []),
                publish_files=publish_summary.get("files_ran", []),
            )

            ## -- succeed the run.

            mark_run_succeeded(conn, run_id=run_id)
            conn.commit()   # commit that we suceeded and are done
            
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
            conn.commit()   # commit in the legder that the run failed.

            # for manifest
            status = "failed"
            logger.event("run_failed")   


        manifest = RunManifest(
            run_id=run_id,
            mode=spec.mode,
            status=status,
            source_system=spec.source_system,
            snapshot_key=spec.snapshot_key if spec.mode == "snapshot" else None,    # default snapshot
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







