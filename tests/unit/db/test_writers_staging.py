from __future__ import annotations

from warehouse_pipeline.db.writers.staging import TABLE_SPECS


def test_table_specs_key_cols_are_tuples():
    """Ensure no syntax errors in `TABLE_SPECS`."""
    for name, spec in TABLE_SPECS.items():
        assert isinstance(spec.key_cols, tuple), (name, spec.key_cols)
        assert all(isinstance(c, str) for c in spec.key_cols)
        assert len(spec.key_cols) >= 1

