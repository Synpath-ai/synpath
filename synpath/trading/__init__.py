"""synpath.trading — order entry, the foundations.

Everything in this package is async-native and money is `Decimal`. Every
public name here is re-exported from the top-level package, so
`from synpath import OrderRequest` and `from synpath.trading import
OrderRequest` are the same thing.

```python
from synpath import KalshiTrading, OrderRequest, Side, OrderType, TimeInForce, load_credentials
```

The venue signing stacks are part of the base install (`pip install
synpath`). Should one be missing from an unusual environment, importing this
package still works -- the types, money helpers and credential loading need
nothing extra -- and the first thing that needs a signer says what to install.
"""
from __future__ import annotations

from .base import TradingExchange
from .errors import (
    CredentialsMissing,
    DuplicateClientOrderId,
    InsufficientFunds,
    InvalidOrder,
    MarketHalted,
    OrderNotFound,
    OrderRejected,
    PermissionDenied,
    RateBudgetExceeded,
    RiskRejected,
)
from .types import (
    Account,
    Balance,
    EditRequest,
    FeeEstimate,
    Fill,
    HeldBy,
    Liquidity,
    Order,
    OrderRequest,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
    Precision,
    Settlement,
    SettlementState,
    Side,
    TimeInForce,
)

from .kalshi import KalshiTrading


def __getattr__(name: str):
    # The Polymarket adapters pull in eth-account and PyJWT; importing them
    # lazily keeps `import synpath.trading` working for a Kalshi-only install.
    if name == "PolymarketTrading":
        from .polymarket import PolymarketTrading
        return PolymarketTrading
    if name == "PolymarketUSTrading":
        from .polymarket_us import PolymarketUSTrading
        return PolymarketUSTrading
    if name == "PolymarketUSExchangeTrading":
        from .polymarket_us_exchange import PolymarketUSExchangeTrading
        return PolymarketUSExchangeTrading
    if name == "OpinionTrading":
        from .opinion import OpinionTrading
        return OpinionTrading
    if name == "HyperliquidTrading":
        from .hyperliquid import HyperliquidTrading
        return HyperliquidTrading
    raise AttributeError(name)


__all__ = [
    "TradingExchange", "KalshiTrading", "PolymarketTrading", "PolymarketUSTrading", "PolymarketUSExchangeTrading",
    "OpinionTrading", "HyperliquidTrading",
    "Account", "Balance", "EditRequest", "FeeEstimate", "Fill", "HeldBy", "Liquidity",
    "Order", "OrderRequest", "OrderStatus", "OrderType", "Position", "PositionSide",
    "Precision", "Settlement", "SettlementState", "Side", "TimeInForce",
    "CredentialsMissing", "DuplicateClientOrderId", "InsufficientFunds", "InvalidOrder",
    "MarketHalted", "OrderNotFound", "OrderRejected", "PermissionDenied",
    "RateBudgetExceeded", "RiskRejected",
]
