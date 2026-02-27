from __future__ import annotations


from warehouse_pipeline.cli.loader import load_file
from warehouse_pipeline.warehouse.build import build_warehouse


def test_warehouse_build_grains(conn, repo_root) -> None:
    """Assert no dups/fanout from the transformation proccess of various mixed `csv` examples."""
    # pipeline load (which produces `run_ids` and also marks status=`succeeded` on success)
    load_file(conn, input_path=repo_root / "data" / "sample" / "customers-1000.csv", table_name="stg_customers")
    load_file(conn, input_path=repo_root / "data" / "sample" / "orders.csv", table_name="stg_orders")
    load_file(conn, input_path=repo_root / "data" / "sample" / "order_items.csv", table_name="stg_order_items")

    # pipeline warehouse build
    build_warehouse(conn)

    # some grain assertions examples
    
    # orders
    dup_orders = conn.execute(
        """
        SELECT order_id FROM fact_orders
        GROUP BY order_id HAVING COUNT(*) > 1
        """
    ).fetchall()
    assert dup_orders == []

    # order_items
    dup_items = conn.execute(
        """
        SELECT order_id, line_id FROM fact_order_items
        GROUP BY order_id, line_id HAVING COUNT(*) > 1
        """
    ).fetchall()
    assert dup_items == []