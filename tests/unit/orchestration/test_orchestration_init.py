from warehouse_pipeline.orchestration import RunManifest, RunSpec, run_pipeline


def test_orchestration_init_happy_path() -> None:
    """Orchestration imports ok."""
    assert RunSpec is not None
    assert RunManifest is not None
    assert callable(run_pipeline)
