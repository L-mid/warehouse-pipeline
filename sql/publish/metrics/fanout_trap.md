# The Fanout trap (order grain × item grain)

If you join a table at grain A (orders) to a table at grain B (order_items),
every order row is repeated once per item.

So any order-level measure (like `fact_orders.total_usd`) will be over-counted
when summed after the join.

**The Fix:** sum at the correct grain (items), or pre-aggregate items to orders (to the same grain is the idea) before joining to ensure correct.
