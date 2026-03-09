from __future__ import annotations

import warehouse_pipeline.dq as dq
from warehouse_pipeline.dq.gates import evaluate_stage_gates
from warehouse_pipeline.dq.runner import run_stage_dq, run_table_dq


def test_dq_init() -> None:
    assert set(dq.__all__) == {
        "DQRunSummary",
        "GateDecision",
        "GateFailure",
        "evaluate_stage_gates",
        "run_stage_dq",
        "run_table_dq",
    }

    assert dq.run_stage_dq is run_stage_dq
    assert dq.run_table_dq is run_table_dq
    assert dq.evaluate_stage_gates is evaluate_stage_gates