"""Paper trading: a venue that fills orders the way a real book would.

A backtest that fills every order at the touch teaches a strategy to be
wrong. This one is deliberately pessimistic in the two places that matter:

**Taking costs the spread and walks the book.** A marketable order eats
levels in order and pays the average of what it ate, not the touch. If the
book is thinner than the order, the rest rests or is cancelled, exactly as
the venue's time-in-force says.

**Resting means queuing.** An order joining a price level is behind
everything already there. It fills only after the trades printed at that
price have consumed the size ahead of it, and if the level trades away
without reaching it, it does not fill. Size added at the same price later
sits behind, and size cancelled ahead moves it up only when the venue tells
us so, which is why the queue is an estimate and says so.

Fees come from the venue's own schedule, passed in as a callable, so a paper
run's profit and loss uses the same formula the live venue would charge.

It implements `TradingExchange`, so the engine, the risk rules and the
journal cannot tell it from a real adapter: the same code path runs in paper
and in production, which is the only way a paper run proves anything.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterable, Sequence

from ..base import Capability
from ..trading.base import TradingExchange
from ..trading.errors import InvalidOrder, OrderNotFound
from ..trading.types import (
    Account, Balance, EditRequest, Fill, Liquidity, Order, OrderRequest, OrderStatus, OrderType, Position, PositionSide,
    Side, TimeInForce,
)

ZERO = Decimal("0")
ONE = Decimal("1")

Level = tuple[Decimal, Decimal]
FeeModel = Callable[[str, Decimal, Decimal, Liquidity], Decimal]


def no_fees(market_id: str, price: Decimal, amount: Decimal, liquidity: Liquidity) -> Decimal:
    return ZERO


def quadratic_fee(rate: Decimal = Decimal("0.07"), *, exponent: Decimal = ONE, maker_rate: Decimal | None = ZERO) -> FeeModel:
    """The prediction-market fee shape: `rate * contracts * (p(1-p))^exponent`.

    Kalshi charges it on takers at 0.07 and rounds up to the cent; Polymarket
    uses the same shape with its own rate and exponent. `maker_rate` of zero
    is both venues today.
    """
    def fee(market_id: str, price: Decimal, amount: Decimal, liquidity: Liquidity) -> Decimal:
        applied = rate if liquidity != Liquidity.MAKER else (maker_rate if maker_rate is not None else rate)
        if not applied:
            return ZERO
        edge = price * (ONE - price)
        if exponent != ONE:
            edge = Decimal(str(float(edge) ** float(exponent)))
        raw = applied * amount * edge
        cents = (raw * 100).to_integral_value(rounding="ROUND_CEILING")
        return cents / 100
    return fee


@dataclass(frozen=True, slots=True)
class PriceLevel:
    """One level, shaped like the streaming layer's, so code that reads a
    live book reads a simulated one without noticing."""

    price: Decimal
    size: Decimal


@dataclass(slots=True)
class BookState:
    """One instrument's book, as the simulator sees it."""

    bids: list[Level] = field(default_factory=list)
    asks: list[Level] = field(default_factory=list)

    def levels(self, depth: int | None = None) -> tuple[tuple[PriceLevel, ...], tuple[PriceLevel, ...]]:
        """Bids best first, asks best first, as `synpath.ws.LocalBook` gives them."""
        bids = [PriceLevel(p, s) for p, s in self.bids[: depth or len(self.bids)]]
        asks = [PriceLevel(p, s) for p, s in self.asks[: depth or len(self.asks)]]
        return tuple(bids), tuple(asks)

    @property
    def ready(self) -> bool:
        return bool(self.bids or self.asks)

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0][0] if self.asks else None

    def size_at(self, side: Side, price: Decimal) -> Decimal:
        levels = self.bids if side == Side.BUY else self.asks
        for level_price, size in levels:
            if level_price == price:
                return size
        return ZERO


@dataclass(slots=True)
class Resting:
    """A resting paper order and where it stands in the queue."""

    order: Order
    queue_ahead: Decimal
    """Contracts that must trade at this price before this order fills."""
    request: OrderRequest


class PaperVenue(TradingExchange):
    """A venue that exists only in this process.

    ```python
    paper = PaperVenue(venue="kalshi", fees=quadratic_fee())
    paper.set_book("KXX:yes", bids=[(D("0.41"), D("500"))], asks=[(D("0.43"), D("300"))])
    order = await paper.create_order(request)      # rests behind 500 contracts
    paper.on_trade("KXX:yes", price=D("0.41"), amount=D("520"))   # 20 of them are ours
    ```

    Feed it books and trades from `synpath.ws` and a strategy runs against
    the real tape without sending anything.
    """

    id = "paper"
    name = "Paper trading"
    has: dict[str, Capability] = {
        "create_order": True, "create_orders": True, "cancel_order": True, "cancel_orders": True,
        "cancel_all_orders": True, "edit_order": True, "fetch_order": True, "fetch_open_orders": True,
        "fetch_orders": True, "fetch_my_trades": True, "fetch_positions": True, "fetch_balance": True,
        "fetch_settlements": False, "fetch_queue_position": True, "fetch_fee_estimate": True,
        "rfq": False, "split_merge": False,
        "watch_orders": False, "watch_my_trades": False, "watch_positions": False, "watch_balance": False,
    }

    def __init__(
        self,
        *,
        venue: str = "paper",
        cash: Decimal = Decimal("10000"),
        fees: FeeModel = no_fees,
        account: Account | None = None,
        clock: Callable[[], float] = time.time,
        face_value: Decimal = ONE,
    ):
        self.venue = venue
        self.account = account or Account(venue=venue, name="paper")
        self.fees = fees
        self.clock = clock
        self.face_value = face_value
        self.cash = cash
        self.start_cash = cash
        self.books: dict[str, BookState] = {}
        self.resting: dict[str, Resting] = {}
        self.orders: dict[str, Order] = {}
        self.fills: list[Fill] = []
        self.positions_held: dict[str, Decimal] = {}
        self.locked = ZERO
        self._n = 0
        self.pending: list[Fill] = []
        """Fills made but not yet delivered. A real venue reports a fill on a
        stream, after the call that caused it has returned, and code that
        reacts to fills must work that way here too."""
        self.listeners: list[Callable[[Fill], Any]] = []
        self.order_listeners: list[Callable[[Order], Any]] = []
        self.pending_orders: list[Order] = []

    def subscribe(self, listener: Callable[[Fill], Any]) -> Callable[[Fill], Any]:
        """Receive fills as they are delivered, as a user stream would send them."""
        self.listeners.append(listener)
        return listener

    def subscribe_orders(self, listener: Callable[[Order], Any]) -> Callable[[Order], Any]:
        """Receive order updates, as a user stream would send them."""
        self.order_listeners.append(listener)
        return listener

    async def deliver(self) -> list[Fill]:
        """Hand over everything that has happened since the last call: fills
        first, then the order updates they caused, which is the order a venue's
        own stream reports them in."""
        made, self.pending = self.pending, []
        for fill in made:
            for listener in list(self.listeners):
                result = listener(fill)
                if hasattr(result, "__await__"):
                    await result
        updates, self.pending_orders = self.pending_orders, []
        for order in updates:
            latest = self.orders.get(order.id, order)
            for listener in list(self.order_listeners):
                result = listener(latest)
                if hasattr(result, "__await__"):
                    await result
        return made

    # -- feeding it -----------------------------------------------------------

    def set_book(self, market_id: str, *, bids: Sequence[Level] = (), asks: Sequence[Level] = ()) -> None:
        self.books[market_id] = BookState(
            bids=sorted(((Decimal(p), Decimal(s)) for p, s in bids), key=lambda l: -l[0]),
            asks=sorted(((Decimal(p), Decimal(s)) for p, s in asks), key=lambda l: l[0]),
        )

    def from_local_book(self, market_id: str, book: Any, depth: int | None = None) -> None:
        """Take a `synpath.ws` `LocalBook` as it stands."""
        bids, asks = book.levels(depth)
        self.set_book(market_id, bids=[(l.price, l.size) for l in bids], asks=[(l.price, l.size) for l in asks])

    def on_trade(self, market_id: str, *, price: Decimal, amount: Decimal, taker_side: Side | None = None) -> list[Fill]:
        """A print on the tape. Consumes queue ahead and fills what it reaches."""
        made: list[Fill] = []
        for order_id, rest in list(self.resting.items()):
            order = rest.order
            if order.market_id != market_id or order.price != price:
                continue
            if taker_side is not None and taker_side == order.side:
                # A taker on our own side lifts the other side of the book.
                continue
            consumed = min(rest.queue_ahead, amount)
            rest.queue_ahead -= consumed
            left = amount - consumed
            if left <= 0:
                continue
            fillable = min(left, order.remaining or ZERO)
            if fillable > 0:
                made.append(self._fill(rest, price=price, amount=fillable, liquidity=Liquidity.MAKER))
            amount = left - fillable
            if amount <= 0:
                break
        return made

    # -- the venue interface --------------------------------------------------

    async def create_order(self, request: OrderRequest) -> Order:
        if request.type not in (OrderType.LIMIT, OrderType.MARKET):
            raise InvalidOrder(f"paper trading holds {request.type.value} orders in the engine, not the venue")
        book = self.books.get(request.market_id, BookState())
        self._n += 1
        order_id = f"paper-{self._n}"
        price = request.price
        order = Order(
            id=order_id, client_order_id=request.client_order_id, venue=self.venue, account=request.account or self.account,
            market_id=request.market_id, side=request.side,
            type=request.type, time_in_force=request.time_in_force, status=OrderStatus.OPEN, price=price,
            amount=request.amount, filled=ZERO, remaining=request.amount, book=request.book, trader=request.trader,
            created_at=int(self.clock() * 1000), tags=dict(request.tags), expires_at=request.expires_at,
            post_only=request.post_only, reduce_only=request.reduce_only,
        )
        self.orders[order_id] = order
        rest = Resting(order=order, queue_ahead=book.size_at(request.side, price) if price is not None else ZERO, request=request)
        self.resting[order_id] = rest

        if not request.post_only:
            self._cross(rest, book)
        order = self.orders[order_id]
        if order.remaining and order.remaining > 0:
            if request.time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
                if request.time_in_force == TimeInForce.FOK and order.filled > 0 and order.remaining > 0:
                    # Fill or kill: what filled should not have. Undo it.
                    self._unfill(order_id)
                self._close(order_id, OrderStatus.CANCELED)
            elif request.post_only and self._would_cross(order, book):
                self._close(order_id, OrderStatus.CANCELED)
        return self.orders[order_id]

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        out: list[Order | Exception] = []
        for request in requests:
            try:
                out.append(await self.create_order(request))
            except Exception as exc:
                out.append(exc)
        return out

    async def cancel_order(self, order_id: str, *, market_id: str | None = None, current: Order | None = None) -> Order:
        if order_id not in self.orders:
            raise OrderNotFound(f"paper: no order {order_id}")
        if self.orders[order_id].is_terminal:
            return self.orders[order_id]
        return self._close(order_id, OrderStatus.CANCELED)

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        out: list[Order | Exception] = []
        for order_id in order_ids:
            try:
                out.append(await self.cancel_order(order_id))
            except Exception as exc:
                out.append(exc)
        return out

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int:
        count = 0
        for order_id, rest in list(self.resting.items()):
            if market_id and rest.order.market_id != market_id:
                continue
            self._close(order_id, OrderStatus.CANCELED)
            count += 1
        return count

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        rest = self.resting.get(request.order_id)
        if rest is None:
            raise OrderNotFound(f"paper: no resting order {request.order_id}")
        order = rest.order
        new_price = request.price if request.price is not None else order.price
        new_amount = request.amount if request.amount is not None else order.amount
        shrinking = new_price == order.price and new_amount < order.amount
        updated = order.model_copy(update={
            "price": new_price, "amount": new_amount,
            "remaining": max(ZERO, new_amount - order.filled),
            "queue_priority_preserved": shrinking,
        })
        self.orders[order.id] = updated
        rest.order = updated
        if not shrinking:
            # A price change goes to the back of the new level's queue.
            book = self.books.get(order.market_id, BookState())
            rest.queue_ahead = book.size_at(order.side, new_price) if new_price is not None else ZERO
        return updated

    async def fetch_order(self, order_id: str) -> Order:
        if order_id not in self.orders:
            raise OrderNotFound(f"paper: no order {order_id}")
        return self.orders[order_id]

    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        return [r.order for r in self.resting.values() if not market_id or r.order.market_id == market_id]

    async def fetch_orders(self, *, market_id: str | None = None, status: Any = None, since: int | None = None,
                           until: int | None = None, limit: int | None = None, cursor: str | None = None) -> list[Order]:
        orders = list(self.orders.values())
        if market_id:
            orders = [o for o in orders if o.market_id == market_id]
        if since:
            orders = [o for o in orders if (o.created_at or 0) >= since]
        return orders[: limit or len(orders)]

    async def fetch_my_trades(self, *, market_id: str | None = None, since: int | None = None, until: int | None = None,
                              limit: int | None = None, cursor: str | None = None) -> list[Fill]:
        fills = self.fills
        if market_id:
            fills = [f for f in fills if f.market_id == market_id]
        if since:
            fills = [f for f in fills if f.timestamp >= since]
        return fills[: limit or len(fills)]

    async def fetch_queue_position(self, order_id: str) -> Decimal:
        rest = self.resting.get(order_id)
        if rest is None:
            raise OrderNotFound(f"paper: no resting order {order_id}")
        return rest.queue_ahead

    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        out = []
        for held_market, contracts in self.positions_held.items():
            if contracts == 0:
                continue
            if market_id and held_market != market_id:
                continue
            out.append(Position(
                venue=self.venue, account=self.account, market_id=held_market,
                side=PositionSide.LONG if contracts > 0 else PositionSide.SHORT, contracts=abs(contracts),
                timestamp=int(self.clock() * 1000),
            ))
        return out

    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        return Balance(
            venue=self.venue, account=account or self.account, currency="USD", total=self.cash,
            available=self.cash - self.locked, locked=self.locked, timestamp=int(self.clock() * 1000),
        )

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> Any:
        from ..trading.types import FeeEstimate

        return FeeEstimate(
            venue=self.venue, market_id=market_id, side=side, price=price, amount=amount,
            taker_fee=self.fees(market_id, price, amount, Liquidity.TAKER),
            maker_fee=self.fees(market_id, price, amount, Liquidity.MAKER),
        )

    async def close(self) -> None:
        return None

    # -- the simulation itself ------------------------------------------------

    def _would_cross(self, order: Order, book: BookState) -> bool:
        if order.price is None:
            return True
        if order.side == Side.BUY:
            return book.best_ask is not None and order.price >= book.best_ask
        return book.best_bid is not None and order.price <= book.best_bid

    def _cross(self, rest: Resting, book: BookState) -> None:
        """Take whatever of the book this order can reach, level by level."""
        order = rest.order
        levels = book.asks if order.side == Side.BUY else book.bids
        limit = order.price
        remaining = order.remaining or ZERO
        taken: list[Level] = []
        for price, size in list(levels):
            if remaining <= 0:
                break
            if limit is not None:
                if order.side == Side.BUY and price > limit:
                    break
                if order.side == Side.SELL and price < limit:
                    break
            amount = min(size, remaining)
            taken.append((price, amount))
            remaining -= amount
        for price, amount in taken:
            self._fill(rest, price=price, amount=amount, liquidity=Liquidity.TAKER)
            self._consume(book, order.side, price, amount)
        if taken:
            # Whatever is left joins the queue at its own price, behind the rest.
            rest.queue_ahead = book.size_at(order.side, order.price) if order.price is not None else ZERO

    @staticmethod
    def _consume(book: BookState, side: Side, price: Decimal, amount: Decimal) -> None:
        levels = book.asks if side == Side.BUY else book.bids
        for index, (level_price, size) in enumerate(levels):
            if level_price == price:
                left = size - amount
                if left > 0:
                    levels[index] = (level_price, left)
                else:
                    levels.pop(index)
                return

    def _fill(self, rest: Resting, *, price: Decimal, amount: Decimal, liquidity: Liquidity) -> Fill:
        order = rest.order
        fee = self.fees(order.market_id, price, amount, liquidity)
        stamp = int(self.clock() * 1000)
        fill = Fill(
            id=f"pf-{uuid.uuid4().hex[:12]}", order_id=order.id, client_order_id=order.client_order_id,
            venue=self.venue, account=order.account or self.account,
            market_id=order.market_id, side=order.side, price=price, amount=amount, fee=fee, fee_currency="USD",
            liquidity=liquidity, timestamp=stamp,
        )
        self.fills.append(fill)
        self.pending.append(fill)
        filled = order.filled + amount
        average = ((order.average_price or ZERO) * order.filled + price * amount) / filled if filled else ZERO
        updated = order.model_copy(update={
            "filled": filled, "remaining": order.amount - filled, "average_price": average,
            "last_fill_price": price, "last_fill_amount": amount, "fee": (order.fee or ZERO) + fee,
            "status": OrderStatus.CLOSED if order.amount - filled <= 0 else OrderStatus.OPEN,
            "updated_at": stamp,
        })
        self.orders[order.id] = updated
        rest.order = updated
        self._changed(updated)
        signed = amount if order.side == Side.BUY else -amount
        self.positions_held[order.market_id] = self.positions_held.get(order.market_id, ZERO) + signed
        self.cash -= (price * amount if order.side == Side.BUY else -price * amount) + fee
        if updated.remaining is not None and updated.remaining <= 0:
            self.resting.pop(order.id, None)
        return fill

    def _unfill(self, order_id: str) -> None:
        """Undo the fills of a fill-or-kill that could not complete."""
        order = self.orders[order_id]
        mine = [f for f in self.fills if f.order_id == order_id]
        for fill in mine:
            signed = fill.amount if fill.side == Side.BUY else -fill.amount
            self.positions_held[fill.market_id] = self.positions_held.get(fill.market_id, ZERO) - signed
            self.cash += (fill.price * fill.amount if fill.side == Side.BUY else -fill.price * fill.amount) + (fill.fee or ZERO)
        self.fills = [f for f in self.fills if f.order_id != order_id]
        self.pending = [f for f in self.pending if f.order_id != order_id]
        self.orders[order_id] = order.model_copy(update={
            "filled": ZERO, "remaining": order.amount, "average_price": None, "fee": ZERO,
        })
        if order_id in self.resting:
            self.resting[order_id].order = self.orders[order_id]

    def _close(self, order_id: str, status: OrderStatus) -> Order:
        order = self.orders[order_id]
        closed = order.model_copy(update={"status": status, "updated_at": int(self.clock() * 1000)})
        self.orders[order_id] = closed
        self.resting.pop(order_id, None)
        self._changed(closed)
        return closed

    def _changed(self, order: Order) -> None:
        """Queue an order update for delivery, newest state per order."""
        self.pending_orders = [o for o in self.pending_orders if o.id != order.id]
        self.pending_orders.append(order)

    # -- reporting ------------------------------------------------------------

    def settle(self, market_id: str, *, yes_wins: bool) -> Decimal:
        """Resolve a market: pay the holders and flatten the position. The
        position is signed on the YES leg: a long is paid the face value if
        YES wins, a short (NO held) if it does not."""
        contracts = self.positions_held.get(market_id, ZERO)
        if contracts == 0:
            return ZERO
        if contracts > 0:
            proceeds = (self.face_value if yes_wins else ZERO) * contracts
        else:
            proceeds = (ZERO if yes_wins else self.face_value) * -contracts
        self.cash += proceeds
        self.positions_held[market_id] = ZERO
        return proceeds

    @property
    def equity(self) -> Decimal:
        """Cash plus what the positions would fetch at the touch."""
        total = self.cash
        for market_id, contracts in self.positions_held.items():
            if contracts == 0:
                continue
            book = self.books.get(market_id, BookState())
            price = book.best_bid if contracts > 0 else book.best_ask
            if price is not None:
                total += price * contracts
        return total
