# The Fanout trap (order grain × item grain)

## What fanout is

Fanout happens when joining a 1-row-per-entity table (e.g `fact_orders`, one row per `order_id`) to a many-rows-per-entity table (e.g. `fact_order_items`, many rows per `order_id`).

After the join, each order row is repeated once per matching item row!

## Why the wrong query is wrong

If you join `fact_orders` to `fact_order_items` and then `SUM(fact_orders.total_usd)`, this is summing an order-level measure on a rowset that has been expanded to item grain. This causes inflation of its values.

An Example:
- Order `A` has `total_usd = 100`
- Order `A` has `3` items

After the join, order `A` appears 3 times, so `SUM(total_usd)` contributes `100 + 100 + 100 = 300` for that one order.

This is the inflation caused by fanout.

## Why the fix is correct

The fix is correct because it restores the intended grain before applying an aggregation.

- If you wanted revenue at this item grain, sum an item-grain measure (e.g. `SUM(fact_order_items.net_usd)`).
- If you wanted the revenue at order grain, either:
  - don't join at all (use `fact_orders` directly), or
  - pre-aggregate items to order grain (1 row per `order_id`) and only then join anything.

All correct fixes share the same invariant principle:

> Before you aggregete, the rows you aggregate over must match the grain of the metric or you will have problems.

## Where to find what I'm talking about

The SQL examples under `sql/extras`:

- Wrong (inflates): `sql/extras/050_fanout_trap_wrong.sql`
- Right (correct): `sql/extras/051_fanout_trap_right.sql`

Test for both:

- `tests/integration/test_fanout_trap_and_distinct.py`