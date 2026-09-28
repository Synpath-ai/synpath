"""synpath.engine.orders -- the order types the venues do not hold.

Stops (market, limit, trailing), icebergs, one-cancels-the-other and
brackets, TWAP, pegs, and the two taking types: an engine-held market order
that walks the book inside a price bound, and a smart taker that does it in
clips over time. Day is not here: it is rewritten to a venue-held GTD at the
session end, because an expiry must outlive the process (`day.py`).

Each type is a `ManagedOrder`: a state machine whose state is written to the
journal on every change and restored on start, submitting ordinary venue
orders as children so the risk rules, the ledger and reconciliation see them
like anything else.

```python
from decimal import Decimal
from synpath.trading.types import OrderRequest, OrderType, Side

await engine.submit(OrderRequest(
    market_id="kalshi:KXX", side=Side.SELL, amount=Decimal("20"),
    type=OrderType.TRAILING_STOP, stop_price=Decimal("0.40"),
    params={"trail": "0.03", "trigger_source": "touch"},
))
```
"""
from __future__ import annotations

from .base import Child, Context, ManagedOrder, State
from .day import as_gtd, session_expiry
from .iceberg import Iceberg
from .manager import REGISTRY, ManagedOrders, register
from .oco import OCO, Bracket
from .peg import Peg
from .routed import RoutedLimit
from .stop import StopLimit, StopMarket, TrailingStop
from .taker import MarketOrder, SmartTaker
from .twap import TWAP

__all__ = [
    "ManagedOrder", "ManagedOrders", "Context", "Child", "State", "register", "REGISTRY",
    "StopMarket", "StopLimit", "TrailingStop", "Iceberg", "OCO", "Bracket", "TWAP", "Peg",
    "MarketOrder", "SmartTaker", "as_gtd", "session_expiry",
]
