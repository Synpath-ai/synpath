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
from typing import Any, Callable

from .._deprecated import renamed_kwargs
from ..base import Capability, complete_capabilities
from ..errors import NotSupported
from ..types import Page
from .types import (
    Side,
    Account, Balance, EditRequest, FeeEstimate, Fill, Order, OrderRequest, Position, Precision, Settlement,
    AcceptQuote, Action, Combo, ComboLeg, MakerQuote, Outcome, QuoteStatus, Rfq, RfqQuote, RfqStatus,
)


class TradingExchange(ABC):
    id: str
    name: str
    has: dict[str, Capability] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        complete_capabilities(cls)
        # TODO(0.9): remove. Every adapter's own `fetch_fee_estimate` takes
        # the old `amount=` for one release.
        own = cls.__dict__.get("fetch_fee_estimate")
        if own is not None and not getattr(own, "__wrapped__", None):
            cls.fetch_fee_estimate = renamed_kwargs(amount="quantity")(own)  # type: ignore[method-assign]

    # -- orders ---------------------------------------------------------------

    @abstractmethod
    async def create_order(self, request: OrderRequest) -> Order:
        """Place one order the venue holds: a limit, or a market where the
        venue has them. Anything else belongs to the engine."""

    async def create_order_with_precision(self, request: OrderRequest, precision: Precision) -> Order:
        """Use supplied reference rules where supported; other adapters load their own."""
        return await self.create_order(request)

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

    async def fetch_fee_estimate(self, market_id: str, side: Side, price: Decimal, quantity: Decimal) -> FeeEstimate:
        """What an order on this market would cost, in the YES price."""
        raise NotSupported(f"{self.id}: fetch_fee_estimate")

    # -- combos (docs/rfq-combo.md) -------------------------------------------
    #
    # Answered where `has["combos"]` is True. A combo is an instrument with
    # its own market id; its position, P&L and settlement live there.

    async def create_combo(self, legs: list[ComboLeg]) -> Combo:
        """The venue's combo for these legs, created if it does not exist.
        Where the venue keeps one canonical combo per set of legs (Kalshi,
        Polymarket US) this is idempotent: the same legs return the same id."""
        raise NotSupported(f"{self.id}: create_combo")

    async def fetch_combo(self, combo_id: str) -> Combo:
        """One combo by its own market id, with its legs."""
        raise NotSupported(f"{self.id}: fetch_combo")

    async def lookup_combo(self, legs: list[ComboLeg]) -> Combo | None:
        """The existing combo for these legs without creating one, or `None`.
        Only where the venue has a read-only lookup by legs."""
        raise NotSupported(f"{self.id}: lookup_combo")

    # -- RFQ, requester (taker) side ------------------------------------------
    #
    # Answered where `has["rfq_taker"]` is True. Every price is a YES price.

    async def create_rfq(
        self, instrument_id: str, *, quantity: Decimal | None = None, amount: Decimal | None = None,
        outcome: Outcome | None = None, action: Action | None = None, rest_remainder: bool = False,
        combo: Combo | None = None,
    ) -> Rfq:
        """Ask for quotes on a market or a combo (`instrument_id` is either's
        market id), sized by contracts (`quantity`) or money (`amount`),
        exactly one. `outcome`/`action` only where the venue asks one side."""
        raise NotSupported(f"{self.id}: create_rfq")

    async def fetch_rfq(self, rfq_id: str) -> Rfq:
        raise NotSupported(f"{self.id}: fetch_rfq")

    async def fetch_rfqs(
        self, *, status: RfqStatus | str | None = None, market_id: str | None = None,
        limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """This account's own RFQs."""
        raise NotSupported(f"{self.id}: fetch_rfqs")

    async def cancel_rfq(self, rfq_id: str) -> None:
        """Close one of this account's open RFQs. Read it back with `fetch_rfq`."""
        raise NotSupported(f"{self.id}: cancel_rfq")

    async def fetch_quotes(self, rfq_id: str, *, status: QuoteStatus | str | None = None) -> list[RfqQuote]:
        """The quotes visible on one RFQ: every maker's on our own RFQ, our
        own on someone else's."""
        raise NotSupported(f"{self.id}: fetch_quotes")

    async def fetch_quote(self, quote_id: str, *, rfq_id: str | None = None) -> RfqQuote:
        raise NotSupported(f"{self.id}: fetch_quote")

    async def accept_quote(self, accept: AcceptQuote) -> RfqQuote:
        """Take one side of a quote. Returns at once with the quote in its new
        state (`accepted`); the maker's confirm and the orders come after.
        Refused before sending when the quote is worse than `worst_price`."""
        raise NotSupported(f"{self.id}: accept_quote")

    # -- RFQ, quoter (maker) side ---------------------------------------------
    #
    # Answered where `has["rfq_quoter"]` is True.

    async def fetch_incoming_rfqs(
        self, *, market_id: str | None = None, limit: int | None = None, cursor: str | None = None,
    ) -> Page[Rfq]:
        """Open RFQs from other accounts that this account may quote."""
        raise NotSupported(f"{self.id}: fetch_incoming_rfqs")

    async def send_quote(self, quote: MakerQuote) -> RfqQuote:
        raise NotSupported(f"{self.id}: send_quote")

    async def replace_quote(self, quote_id: str, quote: MakerQuote) -> RfqQuote:
        """Swap a live quote for new prices. The result may carry a new id."""
        raise NotSupported(f"{self.id}: replace_quote")

    async def cancel_quote(self, quote_id: str, *, rfq_id: str | None = None) -> None:
        """Withdraw a quote, or decline an accepted one during the last look."""
        raise NotSupported(f"{self.id}: cancel_quote")

    async def confirm_quote(self, quote: RfqQuote) -> RfqQuote:
        """Confirm an accepted quote (the maker's last look). Commits capital."""
        raise NotSupported(f"{self.id}: confirm_quote")

    async def fetch_my_quotes(
        self, *, rfq_id: str | None = None, status: QuoteStatus | str | None = None,
    ) -> list[RfqQuote]:
        """Quotes this account made."""
        raise NotSupported(f"{self.id}: fetch_my_quotes")

    # -- RFQ streams and restarts (optional hooks) ----------------------------

    def subscribe_incoming_rfqs(self, listener: Callable[[Rfq], Any]) -> Callable[[], None] | None:
        """Call `listener(rfq)` (sync or async) for each incoming RFQ as the
        venue pushes it, and return the unsubscribe. `None` (the default)
        means the venue has no such stream here, so the engine polls
        `fetch_incoming_rfqs`. Registering starts nothing; see
        `start_incoming_rfqs`."""
        return None

    async def start_incoming_rfqs(self) -> None:
        """Start the stream that feeds `subscribe_incoming_rfqs` listeners,
        once (a no-op where there is none, or where it already runs)."""
        return None

    def restore_rfqs(self, rfqs: list[Rfq], quotes: list[RfqQuote] | None = None) -> None:
        """After a restart, re-seed what the venue cannot list back: the
        RFQs the engine created and still holds open, and the quotes whose
        accept (`mine=False`) or confirm (`mine=True`) was in doubt, each
        with the status the engine last knew. A venue that lists its own
        RFQs and quotes ignores this (the default)."""
        return None

    # -- housekeeping ---------------------------------------------------------

    async def close(self) -> None:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.id}>"
