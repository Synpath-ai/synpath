"""What every venue's order entry offers, in one async interface.

A trading adapter answers the trading keys in `has` the same way a read
adapter answers the read keys: `True` for what the venue itself holds and
does, `False` for what it does not, filled in completely at class creation
so `has[key]` never raises. It claims nothing the venue does not do; a stop
or an iceberg is the execution engine's capability, reported by the engine.

Every method is a coroutine. The engine that will drive these holds
WebSockets on the same event loop, and a blocking call inside it would stall
every socket for one venue round trip -- the exact moment a stop must fire.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from ..base import Capability, complete_capabilities
from ..errors import NotSupported
from ..types import Page
from .types import (
    Side,
    Account, Balance, EditRequest, FeeEstimate, Fill, Order, OrderRequest, Position, Settlement,
)


class TradingExchange(ABC):
    id: str
    name: str
    has: dict[str, Capability] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        complete_capabilities(cls)

    # -- orders ---------------------------------------------------------------

    @abstractmethod
    async def create_order(self, request: OrderRequest) -> Order:
        """Place one order the venue holds: a limit, or a market where the
        venue has them. Anything else belongs to the engine."""

    async def create_orders(self, requests: list[OrderRequest]) -> list[Order | Exception]:
        """Many orders in one call, one result per request in the order asked
        for. A request the venue refused comes back as its exception rather
        than failing the batch: partial success is normal."""
        raise NotSupported(f"{self.id}: create_orders")

    @abstractmethod
    async def cancel_order(self, order_id: str, *, market_id: str | None = None) -> Order:
        """Cancel one order. Returns the order as the venue reports it after
        the cancel, with `filled` carrying whatever matched first."""

    async def cancel_orders(self, order_ids: list[str], *, market_id: str | None = None) -> list[Order | Exception]:
        raise NotSupported(f"{self.id}: cancel_orders")

    async def cancel_all_orders(self, *, market_id: str | None = None) -> int | None:
        """Cancel every resting order, or every one on a market. Returns how
        many the venue reports cancelling, or `None` where it acknowledges
        without a count."""
        raise NotSupported(f"{self.id}: cancel_all_orders")

    async def edit_order(self, request: EditRequest, *, current: Order | None = None) -> Order:
        """Change a resting order in place where the venue allows it, or by
        cancel and replace where it does not. `Order.queue_priority_preserved`
        says which happened."""
        raise NotSupported(f"{self.id}: edit_order")

    @abstractmethod
    async def fetch_order(self, order_id: str) -> Order:
        """One order by the venue's id."""

    @abstractmethod
    async def fetch_open_orders(self, *, market_id: str | None = None) -> list[Order]:
        """Every resting order, or every one on a market."""

    async def fetch_orders(
        self, *, status: str | None = None, market_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Order]:
        raise NotSupported(f"{self.id}: fetch_orders")

    async def fetch_my_trades(
        self, *, market_id: str | None = None, order_id: str | None = None,
        since: int | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Fill]:
        raise NotSupported(f"{self.id}: fetch_my_trades")

    async def fetch_queue_position(self, order_id: str) -> Decimal:
        """Contracts ahead of this order at its price level."""
        raise NotSupported(f"{self.id}: fetch_queue_position")

    # -- account --------------------------------------------------------------

    @abstractmethod
    async def fetch_balance(self, *, account: Account | None = None) -> Balance:
        """This account's balance at this venue. Never pooled with another."""

    @abstractmethod
    async def fetch_positions(self, *, market_id: str | None = None, event_id: str | None = None) -> list[Position]:
        """Open positions, in this venue's own netting model."""

    async def fetch_settlements(
        self, *, market_id: str | None = None, since: int | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Settlement]:
        raise NotSupported(f"{self.id}: fetch_settlements")

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, amount: Decimal) -> FeeEstimate:
        """What an order on this market would cost, in the YES price."""
        raise NotSupported(f"{self.id}: fetch_fee_estimate")

    # -- housekeeping ---------------------------------------------------------

    async def close(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.id}>"
