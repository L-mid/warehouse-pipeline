from __future__ import annotations

from decimal import Decimal

# Freshness
FRESHNESS_WARN_HOURS = Decimal("24")
FRESHNESS_HARD_HOURS = Decimal("72")

# Volume baseline
VOLUME_BASELINE_MIN_RUNS = 3
VOLUME_BASELINE_LOOKBACK = 5

VOLUME_RATIO_WARN_LOW = Decimal("0.70")
VOLUME_RATIO_HARD_LOW = Decimal("0.50")

VOLUME_RATIO_WARN_HIGH = Decimal("1.50")
VOLUME_RATIO_HARD_HIGH = Decimal("2.00")
