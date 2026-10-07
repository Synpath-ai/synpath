"""synpath — one API for prediction markets.

```python
import synpath

kalshi = synpath.Kalshi()
markets = kalshi.fetch_markets(limit=5)

market = markets[0]
yes = market.yes
print(market.title, yes.quote.bid, yes.quote.ask, yes.quote.last)

book = kalshi.fetch_order_book(yes.id)
print(book.best_bid, book.best_ask)
```

Every venue exposes the same methods and returns the same types. What it will
not do is make two venues look more alike than they are: the unit a volume is
counted in, where a candle's prices came from, and whether a price is missing
or merely zero are all carried in the response instead of being smoothed away.

Check `exchange.has` before calling — a capability a venue lacks raises
`NotSupported` rather than returning an empty list.
"""
from __future__ import annotations

from . import ids
from .base import Capability, Exchange, HttpClient, RateLimiter
from .errors import (
    AuthenticationError,
    BadRequest,
    BadSymbol,
    ExchangeError,
    ExchangeNotAvailable,
    MarketNotFound,
    NetworkError,
    NotSupported,
    RateLimitExceeded,
    RequestTimeout,
    SynpathError,
)
from .kalshi import Kalshi
from .polymarket import Polymarket
from .polymarket_us import PolymarketUS
from .opinion import Opinion
from .hyperliquid import Hyperliquid
from .predict_fun import PredictFun
from .limitless import Limitless
from .matching import match_event, match_market
from .history import fetch_order_book_at, fetch_order_book_range, fetch_trades_range
from .bucket import Bucket, BucketMember, BucketOrderReport, BucketPosition
from .types import (
    BookModel,
    Candle,
    Event,
    EventMatch,
    FeeSchedule,
    Outcome,
    Market,
    MarketLink,
    MarketMatch,
    MarketStats,
    MarketStatus,
    OrderBook,
    OrderLevel,
    Page,
    PriceSource,
    Quote,
    Series,
    Trade,
    HistoryMetadata,
    HistoryCoverage,
    HistoricalTrade,
    HistoricalOrderBook,
    HistoricalBookChange,
    HistoricalBookSegment,
    OrderBookAtResponse,
    OrderBookRangeResponse,
    TradesRangeResponse,
)

__version__ = "0.6.0"

exchanges: dict[str, type[Exchange]] = {
    "kalshi": Kalshi,
    "polymarket": Polymarket,
    "polymarket_us": PolymarketUS,
    "opinion": Opinion,
    "hyperliquid": Hyperliquid,
    "predict_fun": PredictFun,
    "limitless": Limitless,
}
"""Every venue this library speaks, by id."""


def exchange(venue: str, **kwargs) -> Exchange:
    """Construct a venue adapter by id: `synpath.exchange("kalshi")`."""
    try:
        return exchanges[venue](**kwargs)
    except KeyError:
        raise BadRequest(
            f"unknown venue {venue!r}; available: {', '.join(sorted(exchanges))}"
        ) from None


# Order entry lives in `synpath.trading`, but it is part of the same install
# and the same API, so its public names are available here too:
# `from synpath import KalshiTrading, OrderRequest`. The subpackage import
# keeps working. Imported last, after the read API it builds on.
from .trading import (
    TradingExchange, KalshiTrading, PolymarketTrading, PolymarketUSTrading, PolymarketUSExchangeTrading,
    OpinionTrading, HyperliquidTrading, PredictFunTrading, LimitlessTrading,
    Account, Balance, EditRequest, FeeEstimate, Fill, HeldBy, Liquidity,
    Order, OrderRequest, OrderStatus, OrderType, Position, PositionSide,
    Precision, Settlement, SettlementState, Side, TimeInForce,
    CredentialsMissing, DuplicateClientOrderId, InsufficientFunds, InvalidOrder,
    MarketHalted, OrderNotFound, OrderRejected, PermissionDenied,
    RateBudgetExceeded, RiskRejected,
)
from .trading.credentials import load_credentials, require
from .client import Client

# Live streams live in `synpath.ws` and are exported here too. The event types
# come in eagerly; the stream classes are resolved on first use through
# `__getattr__` below, the same way `synpath.ws` itself defers them. The
# streams' base event class is `synpath.ws.Event`; here it is `StreamEvent`,
# because `synpath.Event` is already a venue event (a group of markets).
from .ws import (
    Stream, StreamStats, BookEvent, BookLevel, QuoteEvent, TradeEvent, OrderEvent, FillEvent, PositionEvent,
    BalanceEvent, MarketStatusEvent, VenueEvent, StreamStatusEvent, LocalBook,
)
from .ws import Event as StreamEvent

_STREAM_CLASSES = (
    "KalshiStream", "PolymarketMarketStream", "PolymarketUserStream", "PolymarketUSMarketStream",
    "PolymarketUSPrivateStream", "PolymarketUSExchangeOrderStream", "PolymarketUSExchangeDropCopyStream",
    "PolymarketUSExchangeTradeCaptureStream", "PolymarketUSExchangePositionChangeStream",
    "PolymarketUSExchangeInstrumentStream", "PolymarketUSExchangePositionStream",
    "PolymarketUSExchangeMarketDataStream", "PolymarketUSExchangeBalanceLedgerStream",
    "OpinionMarketStream", "OpinionUserStream", "HyperliquidMarketStream", "HyperliquidUserStream",
    "PredictFunMarketStream", "PredictFunUserStream", "LimitlessMarketStream", "LimitlessUserStream",
)
_SERVER_NAMES = ("create_app", "create_trading_app", "VenueRegistry", "ControlStore", "Principal")


def __getattr__(name: str):
    """Stream classes and the HTTP server, resolved on first use.

    Both are part of the package, but neither is imported by `import synpath`:
    a stream module pulls in a venue's WebSocket client, and the server pulls
    in FastAPI, and a notebook reading quotes needs neither.
    """
    if name in _STREAM_CLASSES:
        from . import ws
        return getattr(ws, name)
    if name in _SERVER_NAMES:
        from . import server
        return getattr(server, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "Kalshi", "Polymarket", "PolymarketUS", "Opinion", "Hyperliquid", "PredictFun", "Limitless", "Exchange", "exchange", "exchanges",
    "Capability", "HttpClient", "RateLimiter",
    "Market", "Event", "Outcome", "Quote", "MarketStats", "OrderBook",
    "OrderLevel", "Trade", "Candle", "FeeSchedule", "Series", "Page",
    "HistoryMetadata", "HistoryCoverage", "HistoricalTrade", "HistoricalOrderBook",
    "HistoricalBookChange", "HistoricalBookSegment", "OrderBookAtResponse",
    "OrderBookRangeResponse", "TradesRangeResponse",
    "match_market", "match_event", "ids",
    "fetch_order_book_at", "fetch_order_book_range", "fetch_trades_range",
    "MarketMatch", "EventMatch", "MarketLink",
    "Bucket", "BucketMember", "BucketOrderReport", "BucketPosition",
    "MarketStatus", "BookModel", "PriceSource",
    "SynpathError", "NetworkError", "ExchangeError", "ExchangeNotAvailable",
    "RequestTimeout", "RateLimitExceeded", "BadRequest", "BadSymbol",
    "MarketNotFound", "NotSupported", "AuthenticationError",
    # order entry
    "TradingExchange", "KalshiTrading", "PolymarketTrading", "PolymarketUSTrading", "PolymarketUSExchangeTrading",
    "OpinionTrading", "HyperliquidTrading", "PredictFunTrading", "LimitlessTrading",
    "Account", "Balance", "EditRequest", "FeeEstimate", "Fill", "HeldBy", "Liquidity",
    "Order", "OrderRequest", "OrderStatus", "OrderType", "Position", "PositionSide",
    "Precision", "Settlement", "SettlementState", "Side", "TimeInForce",
    "CredentialsMissing", "DuplicateClientOrderId", "InsufficientFunds", "InvalidOrder",
    "MarketHalted", "OrderNotFound", "OrderRejected", "PermissionDenied",
    "RateBudgetExceeded", "RiskRejected",
    "load_credentials", "require", "Client",
    # live streams
    "Stream", "StreamStats", "StreamEvent", "BookEvent", "BookLevel", "QuoteEvent", "TradeEvent", "OrderEvent",
    "FillEvent", "PositionEvent", "BalanceEvent", "MarketStatusEvent", "VenueEvent", "StreamStatusEvent",
    "LocalBook", *_STREAM_CLASSES,
    # HTTP server
    *_SERVER_NAMES,
    "__version__",
]
