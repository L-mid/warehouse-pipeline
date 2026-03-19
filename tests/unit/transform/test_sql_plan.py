from __future__ import annotations

from pathlib import Path

from warehouse_pipeline.transform.sql_plan import resolve_sql_plan


def test_resolve_sql_plan_build_all_happy_path(tmp_path: Path) -> None:
    names = [
        "100_fact_orders.sql",
        "110_fact_order_lines.sql",
        "120_fact_order_tenders.sql",
    ]
    for name in names:
        (tmp_path / name).write_text("-- ok\n", encoding="utf-8")

    plan = resolve_sql_plan(step_name="build_all", sql_dir=tmp_path)

    assert plan.step_name == "build_all"
    assert plan.file_names == tuple(names)
    assert [path.name for path in plan.paths] == names
