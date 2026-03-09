from __future__ import annotations

from pathlib import Path
import pytest

from warehouse_pipeline.transform.sql_plan import resolve_sql_plan


def test_resolve_sql_plan_happy_path(tmp_path: Path) -> None:
    """Returns everything on the "build_all" paramater."""
    
    names = [
        "100_dim_customer.sql",
        "110_dim_date.sql",
        "120_fact_orders.sql",
        "130_fact_order_items.sql",
    ]
    for name in names:
        (tmp_path / name).write_text("-- ok\n", encoding="utf-8") 

    plan = resolve_sql_plan(step_name="build_all", sql_dir=tmp_path)


    assert plan.step_name == "build_all"
    assert plan.file_names == tuple(names)
    assert [p.name for p in plan.paths] == names    # unpack from where it came from