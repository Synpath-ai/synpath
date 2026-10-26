"""Azuro's feed WebSocket: conditions' odds and states as quotes, books and status changes.

One endpoint per network, `wss://streams.onchainfeed.org/v1/streams/feed`
(`dev-streams` for the testnets), speaking plain JSON: the client sends
`{"event": "SubscribeConditions", "data": {"conditionIds": [...], "environment": ...}}`
(and `UnsubscribeConditions`), and the feed answers with a `ConditionUpdated`
message whenever a condition's odds or state change, carrying every outcome's
`currentOdds`. The environment names the chain's pool, as on REST.

**Quotes.** Each update is the pool's quote on every market of the condition,
read as `Azuro` reads it: buying YES at `1 / odds` of the YES outcome and, on a
two-outcome market, selling at one minus the price of the other.

**Books.** A pool has no book; `watch_order_book` keeps the quote as a one-level
book whose sizes are the pool's limits on each outcome, read over REST
(`Azuro.fetch_order_book`) when the market is first watched and after every
reconnect. Updates move the prices; the sizes stay as read.

**Status.** A condition stopping, resuming, resolving or being cancelled
arrives on the same subscription as its odds.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

from .. import ids
from ..azuro import chain_of, outcome_odds, outcome_order, parse_market_id, quote_of, winner_of
from ..base import Capability
from .base import BookEvent, Event, LocalBook, MarketStatusEvent, QuoteEvent, Stream

VENUE = "azuro"

STATES = {"Active": "open", "Stopped": "paused", "Resolved": "settled", "Canceled": "settled"}
"""A condition's state as a market state."""


def _dec(value: float | None) -> Decimal | None:
    return None if value is None else Decimal(str(value))


class AzuroMarketStream(Stream):
    """Odds, one-level books and status changes for Azuro markets. No key needed.

    ```python
    async with AzuroMarketStream(chain="polygon") as stream:
        await stream.watch_ticker(["azuro:600610060000000000943019940000000000001802657415"])
        async for event in stream:
            ...
    ```
    """

    venue = VENUE
    name = "market"
    has: dict[str, Capability] = {
        # The quote as one level a side, sized by the pool's limits.
        "watch_order_book": "partial",
        "watch_ticker": True,
        # Bets reach the tape through the subgraph, not this feed.
        "watch_trades": False,
        "watch_market_status": True,
    }

    def __init__(self, *, chain: Any = "polygon", catalog: Any = None, url: str | None = None, **kwargs: Any):
        self.chain = chain_of(chain)
        super().__init__(url or f"{self.chain.socket_url}/feed", **kwargs)
        self.catalog = catalog
        """An `Azuro` for the books' limits; made when first needed."""
        self.markets: dict[str, tuple[str, str | None]] = {}
        """Native market id -> (condition id, outcome id or `None`), for every market watched."""
        self.books: dict[str, LocalBook] = {}
        self.sizes: dict[str, dict[str, Decimal | None]] = {}
        self.tickers: set[str] = set()
        self.statuses: set[str] = set()
        self.conditions: dict[str, dict[str, Any]] = {}
        """Condition id -> its last state and outcomes, as the feed sent them."""
        self.subscribed: set[str] = set()

    # -- intents --------------------------------------------------------------

    def _add(self, market_ids: list[str]) -> list[str]:
        natives = []
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            self.markets[native] = parse_market_id(native)
            natives.append(native)
        return natives

    async def _subscribe(self) -> None:
        wanted = sorted({c for c, _ in self.markets.values()} - self.subscribed)
        if wanted and await self.send({"event": "SubscribeConditions",
                                       "data": {"conditionIds": wanted, "environment": self.chain.environment}}):
            self.subscribed.update(wanted)

    async def watch_ticker(self, market_ids: list[str]) -> None:
        """The pool's quote on every change of odds."""
        self.tickers.update(self._add(market_ids))
        await self._subscribe()

    async def watch_market_status(self, market_ids: list[str]) -> None:
        """The condition stopping, resuming, resolving or being cancelled."""
        self.statuses.update(self._add(market_ids))
        await self._subscribe()

    async def watch_order_book(self, market_ids: list[str]) -> None:
        """The quote as a one-level book, sized by the pool's limits (read
        over REST now), its prices moved by every update."""
        natives = self._add(market_ids)
        for native in natives:
            self.books.setdefault(native, LocalBook())
        await self._subscribe()
        await self._seed(natives)

    async def unwatch(self, market_ids: list[str]) -> None:
        for market_id in market_ids:
            native = ids.native(VENUE, market_id)
            self.markets.pop(native, None)
            self.books.pop(native, None)
            self.sizes.pop(native, None)
            self.tickers.discard(native)
            self.statuses.discard(native)
        still = {c for c, _ in self.markets.values()}
        dropped = sorted(self.subscribed - still)
        if dropped:
            self.subscribed.difference_update(dropped)
            await self.send({"event": "UnsubscribeConditions",
                             "data": {"conditionIds": dropped, "environment": self.chain.environment}})

    def book(self, market_id: str, side: str = "yes") -> LocalBook | None:
        book = self.books.get(ids.native(VENUE, market_id))
        if book is None:
            return None
        return book if side != "no" else book.mirrored()

    # -- connection -----------------------------------------------------------

    async def on_connect(self) -> None:
        self.subscribed.clear()
        await self._subscribe()
        if self.books:
            self._later(self._seed(list(self.books)))

    def on_disconnect(self) -> None:
        self.subscribed.clear()
        for book in self.books.values():
            book.invalidate()

    def _later(self, coroutine: Any) -> None:
        try:
            asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:  # pragma: no cover - no loop
            coroutine.close()

    def _catalog(self) -> Any:
        if self.catalog is None:
            from ..azuro import Azuro

            self.catalog = Azuro(chain=self.chain)
        return self.catalog

    async def _seed(self, natives: list[str]) -> None:
        """Each book's limits and quote from REST, applied as a snapshot."""
        for native in natives:
            try:
                book = await asyncio.to_thread(self._catalog().fetch_order_book, native)
            except Exception as exc:
                self.status("error", f"could not read the pool's limits for {native}: {type(exc).__name__}: {exc}",
                            key=ids.qualify(VENUE, native))
                continue
            local = self.books.get(native)
            if local is None:
                continue
            ask = book.asks[0] if book.asks else None
            bid = book.bids[0] if book.bids else None
            self.sizes[native] = {"ask": _dec(ask.size) if ask else None, "bid": _dec(bid.size) if bid else None}
            recovering = not local.ready and local.timestamp is not None
            local.replace([(_dec(bid.price), _dec(bid.size))] if bid else [],  # type: ignore[list-item]
                          [(_dec(ask.price), _dec(ask.size))] if ask else [])  # type: ignore[list-item]
            local.timestamp = 0
            if recovering:
                self.status("resynced", f"book {ids.qualify(VENUE, native)}", key=ids.qualify(VENUE, native))
            self.emit(self._book_event(native, local, info={"seeded": True, "limits": book.info.get("limits")}))

    # -- messages -------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        event = message.get("event")
        data = message.get("data")
        if event == "SubscribedToConditions":
            self.status("subscribed", "conditions")
            return []
        if event == "ConditionUpdated" and isinstance(data, dict):
            return self.on_condition(data)
        return []

    def on_condition(self, data: dict[str, Any]) -> list[Event]:
        condition_id = str(data.get("conditionId") or data.get("id") or "")
        if not condition_id:
            return []
        previous = self.conditions.get(condition_id) or {}
        merged = {**previous, **{k: v for k, v in data.items() if k != "outcomes"}, "conditionId": condition_id}
        outcomes = {str(o.get("outcomeId")): dict(o) for o in previous.get("outcomes") or []}
        for row in data.get("outcomes") or []:
            if isinstance(row, dict) and row.get("outcomeId") is not None:
                key = str(row["outcomeId"])
                outcomes[key] = {**outcomes.get(key, {}), **row, "outcomeId": key}
        merged["outcomes"] = list(outcomes.values())
        self.conditions[condition_id] = merged
        events: list[Event] = []
        changed = previous.get("state") != merged.get("state") and previous
        for native, (cid, outcome_id) in self.markets.items():
            if cid != condition_id:
                continue
            events += self._market_events(native, merged, outcome_id, state_changed=bool(changed) or not previous)
        return events

    def _sides(self, condition: dict[str, Any], outcome_id: str | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        ordered = outcome_order(condition)
        if outcome_id is None:
            return (ordered[0], ordered[1]) if len(ordered) == 2 else (None, None)
        return next((o for o in ordered if str(o["outcomeId"]) == outcome_id), None), None

    def _market_events(self, native: str, condition: dict[str, Any], outcome_id: str | None, *,
                       state_changed: bool) -> list[Event]:
        yes, no = self._sides(condition, outcome_id)
        if yes is None:
            return []
        market_id = ids.qualify(VENUE, native)
        yes_quote, _ = quote_of(condition, yes, no)
        events: list[Event] = []
        info = {"condition_id": condition["conditionId"], "state": condition.get("state"),
                "odds": {"yes": outcome_odds(yes), "no": outcome_odds(no) if no else None}}
        if native in self.tickers:
            events.append(QuoteEvent(venue=VENUE, market_id=market_id, side="yes", bid=_dec(yes_quote.bid),
                                     ask=_dec(yes_quote.ask), info=info))
        book = self.books.get(native)
        sizes = self.sizes.get(native)
        if book is not None and sizes is not None:
            ask, bid = _dec(yes_quote.ask), _dec(yes_quote.bid)
            book.replace([(bid, sizes["bid"])] if bid is not None and sizes["bid"] else [],
                         [(ask, sizes["ask"])] if ask is not None and sizes["ask"] else [])
            events.append(self._book_event(native, book, info=info))
        if native in self.statuses and state_changed and condition.get("state") in STATES:
            no_id = str(no["outcomeId"]) if no else None
            winner = winner_of(condition, str(yes["outcomeId"]), no_id)
            events.append(MarketStatusEvent(
                venue=VENUE, market_id=market_id, state=STATES[str(condition["state"])],  # type: ignore[arg-type]
                native=str(condition["state"]), result=winner, info=info,
            ))
        return events

    def _book_event(self, native: str, book: Any, *, info: dict[str, Any]) -> BookEvent:
        bids, asks = book.levels()
        return BookEvent(
            venue=VENUE, market_id=ids.qualify(VENUE, native), side="yes", kind="snapshot", bids=bids, asks=asks,
            best_bid=book.best_bid, best_ask=book.best_ask, info=info,
        )


__all__ = ["AzuroMarketStream"]
