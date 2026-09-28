"""One client for every venue, routed by the venue in each Synpath id.

```python
import synpath

client = synpath.Client()                       # market data needs no credentials
market = client.fetch_market("kalshi:KXFEDDECISION-26SEP-C25")
book = client.fetch_order_book("polymarket:2252244", side="no")

client = synpath.Client(synpath.load_credentials())   # KALSHI_*, POLYMARKET_* from .env
async with client:
    order = await client.create_order(OrderRequest(
        market_id=market.id, side="buy", type="limit", price="0.05", amount=5,
    ))
    await client.cancel_order(order.id, market_id=order.market_id)
```

```python
client = synpath.Client(server="http://127.0.0.1:8000")   # your own `synpath serve`
order = await client.create_order(OrderRequest(..., type="stop_market", stop_price="0.40"))
```

With `server=`, every trading call goes to that server's engine instead of to
the venues, which is how the engine's order types (stops, icebergs, TWAP,
orders on a bucket) are reached from Python. Market data stays direct. The
server holds the venue credentials; this side holds only the server's access token,
found in `~/.synpath/servers.json` for a server on this machine, or given as
`access_token=` / `SYNPATH_ACCESS_TOKEN` for one across the network.

Every id in this library starts with its venue (`kalshi:...`, `polymarket:...`),
so a call that names a market knows where to go. The per-venue adapters
(`synpath.Kalshi`, `synpath.KalshiTrading`, ...) are what this routes to,
built on first use and shared after; a caller that wants one venue only can
keep using them directly.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, Mapping

from . import ids
from .base import Exchange
from .bucket import Bucket, BucketMember, BucketOrderReport, BucketPosition
from .errors import BadRequest
from .trading.base import TradingExchange
from .trading.credentials import Credentials, require
from .trading.types import (
    Balance, EditRequest, FeeEstimate, Fill, Order, OrderRequest, Position, Settlement, Side,
)
from .types import BookSide, Candle, FeeSchedule, Market, OrderBook, Page, Trade

__all__ = ["Client"]


class Client:
    """Reads on any venue, and order entry on the venues you hold credentials for."""

    def __init__(
        self,
        credentials: Mapping[str, Credentials | None] | None = None,
        *,
        exchanges: Mapping[str, Exchange] | None = None,
        trading: Mapping[str, TradingExchange] | None = None,
        server: str | None = None,
        access_token: str | None = None,
        server_http: Any = None,
    ):
        """`credentials` is what `synpath.load_credentials()` returns. `exchanges`
        and `trading` let a caller hand in adapters it already built (a paper
        venue, a test double); anything not given is built on first use.
        `server` is the root URL of your own `synpath serve`; with it, trading
        goes through that server's engine. `server_http` injects an httpx
        client for tests."""
        self.credentials: dict[str, Credentials | None] = dict(credentials or {})
        self._exchanges: dict[str, Exchange] = dict(exchanges or {})
        self._trading: dict[str, TradingExchange] = dict(trading or {})
        self.server = server
        self._remote: Any = None
        if server:
            from .remote import RemoteTrading
            self._remote = RemoteTrading(server, access_token, http_client=server_http)

    # -- adapters -------------------------------------------------------------

    def exchange(self, venue: str) -> Exchange:
        """The read adapter for a venue, built once."""
        if venue not in self._exchanges:
            from . import exchange
            self._exchanges[venue] = exchange(venue)
        return self._exchanges[venue]

    def trading(self, venue: str) -> TradingExchange:
        """The trading adapter for a venue, built once from its credentials.
        `CredentialsMissing` names the variables to set when there are none."""
        if venue not in self._trading:
            creds = require(venue, self.credentials)
            self._trading[venue] = _TRADING[venue](creds)  # type: ignore[index]
        return self._trading[venue]

    def _venue(self, market_id: str) -> str:
        return ids.venue_of(market_id)

    # -- market data (sync, like the read adapters) ---------------------------

    def fetch_market(self, market_id: str) -> Market:
        return self.exchange(self._venue(market_id)).fetch_market(market_id)

    def fetch_markets_by_ids(self, market_ids: list[str]) -> list[Market]:
        """Many markets, across venues, in the order asked for."""
        found: dict[str, Market] = {}
        for venue, batch in _by_venue(market_ids).items():
            for market in self.exchange(venue).fetch_markets_by_ids(batch):
                found[market.id] = market
        return [found[m] for m in (ids.qualify(self._venue(m), ids.split(m)[1]) for m in market_ids) if m in found]

    def fetch_markets(
        self, *, venue: str | None = None, query: str | None = None, limit: int | None = None,
        cursor: str | None = None, status: str = "open", sort: str | None = None,
    ) -> Page[Market]:
        """One venue's page, or without `venue` the first page of every
        venue concatenated (no cursor across venues)."""
        if venue is not None:
            return self.exchange(venue).fetch_markets(query=query, limit=limit, cursor=cursor, status=status, sort=sort)
        if cursor is not None:
            raise BadRequest("a cursor belongs to one venue; pass venue= to continue paging")
        markets: list[Market] = []
        for name in ids.VENUES:
            markets.extend(self.exchange(name).fetch_markets(query=query, limit=limit, status=status, sort=sort))
        return Page(markets, next_cursor=None)

    def fetch_order_book(self, market_id: str, *, side: BookSide = "yes", depth: int | None = None) -> OrderBook:
        return self.exchange(self._venue(market_id)).fetch_order_book(market_id, side=side, depth=depth)

    def fetch_order_books(
        self, market_ids: list[str], *, side: BookSide = "yes", depth: int | None = None,
    ) -> dict[str, OrderBook]:
        books: dict[str, OrderBook] = {}
        for venue, batch in _by_venue(market_ids).items():
            books.update(self.exchange(venue).fetch_order_books(batch, side=side, depth=depth))
        return books

    def fetch_trades(
        self, market_id: str, *, since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Trade]:
        return self.exchange(self._venue(market_id)).fetch_trades(market_id, since=since, limit=limit, cursor=cursor)

    def fetch_ohlcv(
        self, market_id: str, *, timeframe: str = "1h", since: int | None = None,
        until: int | None = None, limit: int | None = None,
    ) -> list[Candle]:
        return self.exchange(self._venue(market_id)).fetch_ohlcv(
            market_id, timeframe=timeframe, since=since, until=until, limit=limit,
        )

    def fetch_fee_schedule(self, market_id: str) -> FeeSchedule:
        return self.exchange(self._venue(market_id)).fetch_fee_schedule(market_id)

    # -- order entry (async, like the trading adapters) -----------------------

    async def create_order(self, request: OrderRequest) -> Order:
        if self._remote:
            return await self._remote.create_order(request)
        return await self.trading(self._venue(request.market_id)).create_order(request)

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders, each sent to its own venue, results in the order given."""
        if self._remote:
            return await self._remote.create_orders(requests)
        results: list[Order | Exception | None] = [None] * len(requests)
        groups: dict[str, list[int]] = {}
        for i, request in enumerate(requests):
            try:
                groups.setdefault(self._venue(request.market_id), []).append(i)
            except BadRequest as exc:
                results[i] = exc
        for venue, indexes in groups.items():
            answers = await self.trading(venue).create_orders([requests[i] for i in indexes])
            for i, answer in zip(indexes, answers):
                results[i] = answer
        return results  # type: ignore[return-value]

    async def create_bucket(self, *, book: str, name: str, members: list[BucketMember]) -> Bucket:
        """Define a bucket on your server; place orders on it by `bucket.market_id`.

        A bucket lives in the server's journal, beside the engine that routes
        its orders, so it needs `Client(server=...)`."""
        return await self._server().create_bucket(book=book, name=name, members=members)

    def _server(self) -> Any:
        if not self._remote:
            raise BadRequest("buckets are kept by your synpath serve: create the client with server=")
        return self._remote

    async def fetch_buckets(self, *, book: str | None = None, status: str = "active") -> list[Bucket]:
        """Your server's buckets: `active` (default), `archived` or `all`."""
        return await self._server().fetch_buckets(book=book, status=status)

    async def fetch_bucket(self, bucket_id: str) -> Bucket:
        """One bucket, by `<id>` or by its market id `bucket:<id>`."""
        return await self._server().fetch_bucket(bucket_id)

    async def archive_bucket(self, bucket_id: str) -> Bucket:
        """Retire a bucket: no new orders on it. Orders already on it keep working."""
        return await self._server().archive_bucket(bucket_id)

    async def fetch_bucket_position(self, bucket_id: str, *, book: str | None = None) -> BucketPosition:
        """The position across a bucket's members, netted in bucket terms."""
        return await self._server().fetch_bucket_position(bucket_id, book=book)

    async def fetch_bucket_orders(self, bucket_id: str) -> list[BucketOrderReport]:
        """Every order placed on a bucket, live or finished, with where it filled."""
        return await self._server().fetch_bucket_orders(bucket_id)

    async def fetch_bucket_order(self, bucket_id: str, order_id: str) -> BucketOrderReport:
        """One order on a bucket: filled, weighted average, and the split by venue."""
        return await self._server().fetch_bucket_order(bucket_id, order_id)

    async def cancel_order(self, order_id: str, *, market_id: str | None = None, venue: str | None = None) -> Order:
        """Cancel by id. The venue comes from `market_id` or `venue`; an order
        id alone does not say where it lives, except to a server, which knows."""
        if self._remote:
            return await self._remote.cancel_order(order_id, market_id=market_id, venue=venue)
        return await self.trading(_venue_of(market_id, venue)).cancel_order(order_id, market_id=market_id)

    async def cancel_all_orders(self, *, market_id: str | None = None, venue: str | None = None) -> int | None:
        """One market's, one venue's, or every venue's resting orders."""
        if self._remote:
            return await self._remote.cancel_all_orders(market_id=market_id, venue=venue)
        if market_id is None and venue is None:
            total: int | None = 0
            for name in list(self._trading):
                count = await self._trading[name].cancel_all_orders()
                total = None if count is None or total is None else total + count
            return total
        return await self.trading(_venue_of(market_id, venue)).cancel_all_orders(market_id=market_id)

    async def edit_order(self, request: EditRequest, *, venue: str | None = None, current: Order | None = None) -> Order:
        if self._remote:
            return await self._remote.edit_order(request, venue=venue, current=current)
        if venue is None:
            raise BadRequest("edit_order needs venue= when trading the venues directly")
        return await self.trading(venue).edit_order(request, current=current)

    async def fetch_order(self, order_id: str, *, market_id: str | None = None, venue: str | None = None) -> Order:
        if self._remote:
            return await self._remote.fetch_order(order_id, market_id=market_id, venue=venue)
        return await self.trading(_venue_of(market_id, venue)).fetch_order(order_id)

    async def fetch_open_orders(self, *, market_id: str | None = None, venue: str | None = None) -> list[Order]:
        """One market's, one venue's, or every connected venue's open orders."""
        if self._remote:
            return await self._remote.fetch_open_orders(market_id=market_id, venue=venue)
        if market_id is None and venue is None:
            orders: list[Order] = []
            for name in list(self._trading):
                orders.extend(await self._trading[name].fetch_open_orders())
            return orders
        return await self.trading(_venue_of(market_id, venue)).fetch_open_orders(market_id=market_id)

    async def fetch_my_trades(
        self, *, market_id: str | None = None, venue: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        if self._remote:
            return await self._remote.fetch_my_trades(market_id=market_id, venue=venue, since=since,
                                                      limit=limit, cursor=cursor)
        return await self.trading(_venue_of(market_id, venue)).fetch_my_trades(
            market_id=market_id, since=since, limit=limit, cursor=cursor,
        )

    async def fetch_positions(self, *, market_id: str | None = None, venue: str | None = None) -> list[Position]:
        """One market's, one venue's, or every connected venue's positions."""
        if self._remote:
            return await self._remote.fetch_positions(market_id=market_id, venue=venue)
        if market_id is None and venue is None:
            positions: list[Position] = []
            for name in list(self._trading):
                positions.extend(await self._trading[name].fetch_positions())
            return positions
        return await self.trading(_venue_of(market_id, venue)).fetch_positions(market_id=market_id)

    async def fetch_balance(self, venue: str) -> Balance:
        if self._remote:
            return await self._remote.fetch_balance(venue)
        return await self.trading(venue).fetch_balance()

    async def fetch_settlements(
        self, *, market_id: str | None = None, venue: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        if self._remote and not self.credentials:
            return await self._remote.fetch_settlements()
        return await self.trading(_venue_of(market_id, venue)).fetch_settlements(
            market_id=market_id, since=since, limit=limit, cursor=cursor,
        )

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        if self._remote and not self.credentials:
            return await self._remote.fetch_fee_estimate(market_id, side, price, amount)
        return await self.trading(self._venue(market_id)).fetch_fee_estimate(market_id, side, price, amount)

    # -- housekeeping ---------------------------------------------------------

    async def close(self) -> None:
        if self._remote is not None:
            await self._remote.close()
        for adapter in self._trading.values():
            await adapter.close()
        for exchange in self._exchanges.values():
            exchange.close()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        via = f" server={self.server}" if self.server else ""
        return f"<synpath.Client reads={sorted(self._exchanges)} trades={sorted(self._trading)}{via}>"


def _by_venue(market_ids: Iterable[str]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for market_id in market_ids:
        groups.setdefault(ids.venue_of(market_id), []).append(market_id)
    return groups


def _venue_of(market_id: str | None, venue: str | None) -> str:
    if market_id is not None:
        found = ids.venue_of(market_id)
        if venue is not None and venue != found:
            raise BadRequest(f"{market_id!r} belongs to {found}, not {venue}")
        return found
    if venue is None:
        raise BadRequest("pass market_id= or venue= so the call can be routed")
    return venue


def _kalshi(creds: Credentials) -> TradingExchange:
    from .trading.kalshi import KalshiTrading
    return KalshiTrading(creds)  # type: ignore[arg-type]


def _polymarket(creds: Credentials) -> TradingExchange:
    from .trading.polymarket import PolymarketTrading
    return PolymarketTrading(creds)  # type: ignore[arg-type]


def _polymarket_us(creds: Credentials) -> TradingExchange:
    from .trading.polymarket_us import PolymarketUSTrading
    return PolymarketUSTrading(creds)  # type: ignore[arg-type]


_TRADING = {"kalshi": _kalshi, "polymarket": _polymarket, "polymarket_us": _polymarket_us}
