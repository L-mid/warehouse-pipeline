from warehouse_pipeline.db.staging_writers import TABLE_SPECS


def test_table_specs_key_cols_are_tuples():
    """No syntax errors in `TABLE_SPECS`."""
    for name, spec in TABLE_SPECS.items():
        assert isinstance(spec.key_cols, tuple), (name, spec.key_cols)
        assert all(isinstance(c, str) for c in spec.key_cols)
        assert len(spec.key_cols) >= 1