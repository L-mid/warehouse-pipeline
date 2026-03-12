from warehouse_pipeline.cli.commands import register_db_commands, register_run_commands


def test_commands_init_exports_registration_helpers() -> None:
    """Test cli commands init ok."""
    assert callable(register_db_commands)
    assert callable(register_run_commands)
