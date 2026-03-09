from __future__ import annotations

import json
from uuid import UUID

from warehouse_pipeline.orchestration.logging import RunLogger
import pytest

def test_run_logger_happy_path(tmp_path) -> None:
    """A read of `RunLogger`'s write exposes expected logged fields."""
    
    # random `run_id`
    run_id = UUID("00000000-0000-0000-0000-000000000222")
    log_path = tmp_path / "logs.jsonl"

    logger = RunLogger(run_id=run_id, log_path=log_path, echo=False)
    logger.event("run_started", mode="snapshot")
    logger.phase_started("extract")
    logger.phase_finished("extract", duration_s=0.123, rows=1)
    
    # test error recording.
    logger.error("pipeline", error_message="boom")


    lines = log_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in lines]

    assert len(records) == 4
    assert records[0]["event"] == "run_started"
    assert records[0]["run_id"] == str(run_id)
    assert records[2]["event"] == "phase_finished"
    assert records[2]["phase"] == "extract"
    assert records[3]["event"] == "phase_failed"
    assert records[3]["error_message"] == "boom"