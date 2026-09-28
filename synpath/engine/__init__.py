"""synpath.engine -- order entry with a journal, a ledger and risk rules.

```python
from synpath.engine import Engine, EngineConfig, RiskConfig

async with Engine({"kalshi": kalshi}, EngineConfig(journal_path="trading.db"),
                  risk=RiskConfig(max_order_contracts=Decimal("100"))) as engine:
    order = await engine.submit(request)
```

The engine writes every decision to a journal before it acts, so a process
killed mid-submit restarts knowing what was in doubt; runs pre-trade risk
rules that name themselves when they refuse; books fills into a ledger that
nets on the YES leg and rolls up by instrument, market, strategy and
account; and refuses to trade at all if another engine holds the journal.

Part of the base install: `pip install synpath`.
"""
from __future__ import annotations

from .engine import Engine, EngineConfig, Recovery
from .events import EngineEvent, EventBus, Subscription
from .fair_values import FairValue, FairValues
from .journal import Intent, IntentState, Journal, JournalEvent, LeaseLost
from .ledger import Ledger, PositionState, Rollup
from .orders import ManagedOrder, ManagedOrders
from .risk import Decision, KillSwitch, RiskConfig, RiskEngine

__all__ = [
    "Engine", "EngineConfig", "Recovery",
    "Journal", "JournalEvent", "Intent", "IntentState", "LeaseLost",
    "EventBus", "EngineEvent", "Subscription",
    "Ledger", "PositionState", "Rollup",
    "RiskConfig", "RiskEngine", "Decision", "KillSwitch",
    "FairValues", "FairValue",
    "ManagedOrder", "ManagedOrders",
]
