from __future__ import annotations

import warehouse_pipeline.dq as dq
from warehouse_pipeline.dq.gates import evaluate_model_gates, render_dq_summary
from warehouse_pipeline.dq.runner import run_model_dq, run_table_dq


def test_dq_init() -> None:
    assert set(dq.__all__) == {
        "DQRunSummary",
        "GateDecision",
        "GateFailure",
        "evaluate_model_gates",
        "render_dq_summary",
        "run_model_dq",
        "run_table_dq",
    }

    assert dq.run_model_dq is run_model_dq
    assert dq.run_table_dq is run_table_dq
    assert dq.evaluate_model_gates is evaluate_model_gates
    assert dq.render_dq_summary is render_dq_summary
