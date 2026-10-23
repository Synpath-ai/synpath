"""RFQ and combo routes under `/trading` (docs/rfq-combo.md §6, phase P5).

Thin over the engine's desk (`engine.rfq`, `synpath/engine/rfq.py`): every
write goes through the `Engine` methods, so risk, the journal, the halt and
recovery are the engine's, exactly as for `POST /orders`. What this layer
adds is who may ask and on which account, and the status codes.

**An accept or a confirm answers `202` by default.** The venue answers an
accept with the quote `accepted`; the maker's last look and the execution
come later, as `quote.*` events on `/ws/events`. With `wait: true` the route
follows the quote for up to `timeout_s` and answers `200` when it is final
(`executed`, `canceled`, `expired` or `failed`), `202` when it is not yet. A
send whose outcome is not known is `202 submission_in_doubt` with the intent
key in `details.client_order_id`, as for orders.

**The venue of an id.** RFQ and quote ids are the venue's own, not Synpath
ids, so a route finds the venue from `venue` (query or body), the account
named, a market id (`venue:native`), what the engine already holds about the
id, or the only venue that has the capability; otherwise it is a `400`. The
grant is checked on that venue's account before anything is sent.
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Annotated, Any, Awaitable, Callable, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel, Field, model_validator

from .. import ids
from ..engine.engine import Engine
from ..errors import SynpathError
from ..trading.types import (
    AcceptQuote, Account, Action, Combo, ComboLeg, ComboMarket, MakerQuote, Outcome, QuoteStatus, Rfq, RfqQuote, RfqStatus,
)
from .errors import http_error
from .models import ErrorBody, PageResponse
from .store import Principal
from .trading import RESPONSES, Caller, EngineDep, _tag_venue, account_key, require, visible

RFQ_RESPONSES: dict[int | str, dict[str, Any]] = {
    **RESPONSES,
    404: {"model": ErrorBody, "description": "No such RFQ, quote or combo, or no such venue"},
    409: {"model": ErrorBody, "description": "Refused by a pre-trade risk rule (`details.rule`) or the halt"},
    501: {"model": ErrorBody, "description": "The venue has no such call"},
}



# ---------------------------------------------------------------------------
# Wire shapes
# ---------------------------------------------------------------------------

class RfqBody(BaseModel):
    """An RFQ to create: one market or one combo, sized by contracts or money."""

    market_id: str = Field(description="The market, or the combo's own market id (`venue:native`)")
    quantity: Decimal | None = Field(default=None, gt=0, description="Contracts. Exactly one of this and `amount`")
    amount: Decimal | None = Field(default=None, gt=0, description="Money, in the venue's collateral")
    outcome: Outcome | None = Field(default=None, description="Only where the venue asks one side")
    action: Action | None = None
    rest_remainder: bool = False
    combo: Combo | None = Field(default=None, description="The combo's legs, where known; defaults to the combo the "
                                                          "engine saved under `market_id`")
    venue: str | None = None
    account: Account | None = None
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _one_size(self) -> "RfqBody":
        if (self.quantity is None) == (self.amount is None):
            raise ValueError("size an RFQ by quantity (contracts) or amount (money), exactly one")
        return self


class RfqView(Rfq):
    """An RFQ, with the strategy book and trader the engine holds for it (ours only)."""

    book: str | None = None
    trader: str | None = None


class AcceptBody(AcceptQuote):
    """`AcceptQuote`, with the quote id from the path, plus how long to follow it."""

    quote_id: str | None = Field(default=None, description="Taken from the path; ignored here")
    venue: str | None = None
    wait: bool = Field(default=False, description="Follow the quote until it is final, up to `timeout_s`")
    timeout_s: float = Field(default=30.0, gt=0, le=120)


class ConfirmBody(BaseModel):
    """The maker's last look on one of our accepted quotes."""

    rfq_id: str | None = None
    venue: str | None = None
    wait: bool = False
    timeout_s: float = Field(default=30.0, gt=0, le=120)


class MakerQuoteBody(MakerQuote):
    """`MakerQuote`, plus the venue it is for."""

    venue: str | None = None


class ComboBody(BaseModel):
    """The legs of a combo. The same legs always give the same combo."""

    legs: list[ComboLeg] = Field(min_length=2)
    venue: str | None = None


class CreateComboBody(ComboBody):
    """`ComboBody`, plus where to build the combo."""

    collection_id: str | None = Field(
        default=None, description="Kalshi: the multivariate event collection to build the combo in "
        "(`collection_ticker`, e.g. `KXMVECROSSCATEGORY-R`; see `GET /combos/collections`). Without it the one "
        "open collection that takes every leg's event is used, and several are a `400` naming them in "
        "`details.collections`. Refused (`400`) on a venue without collections (Polymarket, Polymarket US).")


class QuoteResult(BaseModel):
    """What an accept or a confirm did, as far as it is known yet."""

    quote: RfqQuote
    client_order_id: str = Field(description="The intent key: retry under it, follow events by it")
    venue: str
    final: bool = Field(description="The quote is `executed`, `canceled`, `expired` or `failed`")
    intent_state: str | None = Field(default=None, description="`sending` until the quote is final, then `sent` "
                                                               "(traded) or `failed`/`rejected` (nothing traded)")
    order_id: str | None = Field(default=None, description="The order the execution made, once adopted")


ACCEPT_RESPONSES: dict[int | str, dict[str, Any]] = {
    **RFQ_RESPONSES,
    200: {"model": QuoteResult, "description": "`wait` was true and the quote is final"},
    202: {"description": "Sent; the result arrives as `quote.*` events. Also the error envelope "
                         "`submission_in_doubt` (with `details.client_order_id`) when the venue gave no answer"},
}


class Canceled(BaseModel):
    id: str
    venue: str
    kind: Literal["rfq", "quote"]
    status: Literal["canceled"] = "canceled"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def rfq_venue(engine: Engine, venue: str | None = None, *, account: Account | None = None,
              market_id: str | None = None, rfq_id: str | None = None, quote_id: str | None = None,
              capability: str | None = None) -> str:
    """The venue a request is for (module docstring)."""
    if venue:
        if venue not in engine.adapters:
            raise http_error(404, "unknown_venue", f"this server does not trade {venue}", venue=venue)
        return venue
    if account is not None:
        return account.venue
    desk = engine.rfq
    if rfq_id:
        for key in (*desk.rfqs, *desk.incoming):
            if key.endswith(f":{rfq_id}"):
                return key.split(":", 1)[0]
    if quote_id:
        for key in desk.maker_quotes:
            if key.endswith(f":{quote_id}"):
                return key.split(":", 1)[0]
        for key in (*desk.following, *desk._last_quote):
            if key.endswith(f":quote:{quote_id}"):
                return key.split(":", 1)[0]
    if market_id:
        found, _ = ids.split(market_id)
        if found is not None and found in engine.adapters:
            return found
    capable = [v for v, a in engine.adapters.items() if capability is None or a.has.get(capability)]
    if len(capable) == 1:
        return capable[0]
    if len(engine.adapters) == 1:
        return next(iter(engine.adapters))
    raise HTTPException(status_code=400, detail="name the venue: pass `venue`, name an account, or use a Synpath "
                                                "id (venue:native)")


def venues_for(engine: Engine, principal: Principal, venue: str | None, capability: str) -> list[str]:
    """The venues a list route reads: the one named (403 if the key may not
    view it), else every venue with the capability the key may view."""
    if venue:
        rfq_venue(engine, venue)
        if not visible(principal, account_key(None, engine, venue)):
            raise HTTPException(status_code=403, detail=f"this key may not view {account_key(None, engine, venue)}")
        return [venue]
    return [v for v, a in engine.adapters.items()
            if a.has.get(capability) and visible(principal, account_key(None, engine, v))]


def require_view(principal: Principal, engine: Engine, venue: str) -> None:
    account = account_key(None, engine, venue)
    if not visible(principal, account):
        raise HTTPException(status_code=403, detail=f"this key may not view {account}")


async def tagged(venue: str, awaitable: Any) -> Any:
    try:
        return await awaitable
    except SynpathError as exc:
        _tag_venue(exc, venue)
        raise


def rfq_view(rfq: Rfq, held: dict[str, Any] | None) -> RfqView:
    return RfqView.model_validate({**rfq.model_dump(), "book": (held or {}).get("book"),
                                   "trader": (held or {}).get("trader")})


async def quote_result(engine: Engine, quote: RfqQuote, key: str, venue: str) -> QuoteResult:
    intent = await engine.journal.intent(key)
    return QuoteResult(quote=quote, client_order_id=key, venue=venue, final=quote.is_terminal,
                       intent_state=intent.state if intent else None,
                       order_id=(intent.order_id if intent else None) or None)


COMBO_LIST_TTL_S = 15.0
"""How long a combo list read (eligible markets, collections) is served from
memory: the terminal polls them, and they change on the scale of minutes."""
COMBO_LIST_CACHE_SIZE = 256
COMBO_BOARD_TTL_S = 30.0
"""How long a venue's combo board is served from memory: it reads several
pages of the venue's list (and, on Kalshi, every collection event's markets)."""
PREVIEW_TTL_S = 10.0
"""How long a group preview is reused: the slip asks again on every pick."""


class TtlCache:
    """A small in-process cache of venue reads, by key, for `ttl_s` seconds.
    Only answers are kept; an error is never cached."""

    def __init__(self, ttl_s: float = COMBO_LIST_TTL_S, size: int = COMBO_LIST_CACHE_SIZE,
                 clock: Callable[[], float] = time.monotonic):
        self.ttl_s, self.size, self.clock = ttl_s, size, clock
        self.rows: dict[Any, tuple[float, Any]] = {}

    async def get(self, key: Any, read: Callable[[], Awaitable[Any]]) -> Any:
        now = self.clock()
        held = self.rows.get(key)
        if held is not None and held[0] > now:
            return held[1]
        value = await read()
        if len(self.rows) >= self.size:
            for stale in [k for k, (until, _) in self.rows.items() if until <= now] or [next(iter(self.rows))]:
                self.rows.pop(stale, None)
        self.rows[key] = (now + self.ttl_s, value)
        return value


from ..engine.rfq_group import lists_combo_markets  # noqa: E402  (shared with the engine's groups)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def add_rfq_routes(router: APIRouter) -> None:
    """RFQs (taker and quoter sides), quotes and combos."""

    combo_lists = TtlCache()

    # -- taker: RFQs ----------------------------------------------------------

    @router.post("/rfqs", tags=["rfq"], summary="Ask for quotes", responses=RFQ_RESPONSES, status_code=201)
    async def create_rfq(principal: Caller, engine: EngineDep, body: RfqBody) -> RfqView:
        """On a market or a combo (its own market id), by contracts or money.
        Journaled; refused (`409`, `details.rule`) while halted, on a paused
        book, a restricted market or over the order size limits. Quotes
        arrive as `quote.received` events and on `GET /rfqs/{id}/quotes`."""
        venue = rfq_venue(engine, body.venue, account=body.account, market_id=body.market_id, capability="rfq_taker")
        require(principal, "trade", account_key(body.account, engine, venue))
        combo = body.combo or await engine.rfq.combo(body.market_id)
        rfq = await tagged(venue, engine.create_rfq(
            body.market_id, quantity=body.quantity, amount=body.amount, outcome=body.outcome, action=body.action,
            rest_remainder=body.rest_remainder, combo=combo, venue=venue, account=body.account, book=body.book,
            trader=body.trader, tags=body.tags))
        return rfq_view(rfq, {"book": body.book, "trader": body.trader})

    @router.get("/rfqs", tags=["rfq"], summary="Our RFQs", responses=RFQ_RESPONSES)
    async def list_rfqs(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="Only this venue")] = None,
        status: Annotated[RfqStatus | None, Query(description="Only RFQs in this state")] = None,
        market_id: Annotated[str | None, Query(description="Only this market or combo")] = None,
        source: Annotated[Literal["venue", "engine"], Query(
            description="`venue` (the default) reads each venue's list; `engine` returns the open RFQs the engine "
                        "follows, without a venue call")] = "venue",
        limit: Annotated[int | None, Query(ge=1, le=500)] = None,
    ) -> PageResponse[RfqView]:
        rows: list[RfqView] = []
        for name in venues_for(engine, principal, venue, "rfq_taker"):
            held = {k.split(":", 1)[1]: v for k, v in engine.rfq.rfqs.items() if k.startswith(f"{name}:")}
            if source == "engine":
                found = [Rfq.model_validate(h["rfq"]) for h in held.values()]
                found = [r for r in found if (status is None or r.status == status)
                         and (market_id is None or r.instrument_id == market_id)]
            else:
                found = list(await tagged(name, engine.adapters[name].fetch_rfqs(
                    status=status, market_id=market_id, limit=limit)))
            rows.extend(rfq_view(r, held.get(r.id)) for r in found)
        rows = rows[:limit] if limit else rows
        return PageResponse[RfqView](data=rows, next_cursor=None, count=len(rows))

    @router.get("/rfqs/incoming", tags=["rfq"], summary="Others' open RFQs, for us to quote",
                responses=RFQ_RESPONSES)
    async def incoming_rfqs(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="Only this venue")] = None,
        market_id: Annotated[str | None, Query(description="Only this market or combo")] = None,
        source: Annotated[Literal["venue", "engine"], Query(
            description="`venue` (the default) reads each venue; `engine` returns what the engine's incoming feed "
                        "last saw (the live `rfq.incoming` events), without a venue call")] = "venue",
        limit: Annotated[int | None, Query(ge=1, le=500)] = None,
    ) -> PageResponse[Rfq]:
        rows: list[Rfq] = []
        for name in venues_for(engine, principal, venue, "rfq_quoter"):
            if source == "engine":
                rows.extend(r for k, r in engine.rfq.incoming.items() if k.startswith(f"{name}:")
                            and r.status == RfqStatus.OPEN and (market_id is None or r.instrument_id == market_id))
            else:
                rows.extend(await tagged(name, engine.adapters[name].fetch_incoming_rfqs(market_id=market_id,
                                                                                         limit=limit)))
        rows = rows[:limit] if limit else rows
        return PageResponse[Rfq](data=rows, next_cursor=None, count=len(rows))

    @router.get("/rfqs/{rfq_id}", tags=["rfq"], summary="One RFQ", responses=RFQ_RESPONSES)
    async def get_rfq(principal: Caller, engine: EngineDep, rfq_id: Annotated[str, Path()],
                      venue: Annotated[str | None, Query()] = None) -> RfqView:
        name = rfq_venue(engine, venue, rfq_id=rfq_id, capability="rfq_taker")
        require_view(principal, engine, name)
        rfq = await tagged(name, engine.adapters[name].fetch_rfq(rfq_id))
        return rfq_view(rfq, engine.rfq.rfqs.get(f"{name}:{rfq_id}"))

    @router.delete("/rfqs/{rfq_id}", tags=["rfq"], summary="Cancel one of our RFQs", responses=RFQ_RESPONSES)
    async def cancel_rfq(principal: Caller, engine: EngineDep, rfq_id: Annotated[str, Path()],
                         venue: Annotated[str | None, Query()] = None) -> Canceled:
        """Its open quotes end `canceled` (`quote.canceled`, `rfq.closed`).
        Journaled; never refused by risk, so it works while halted."""
        name = rfq_venue(engine, venue, rfq_id=rfq_id, capability="rfq_taker")
        require(principal, "trade", account_key(None, engine, name))
        await tagged(name, engine.cancel_rfq(rfq_id, venue=name))
        return Canceled(id=rfq_id, venue=name, kind="rfq")

    @router.get("/rfqs/{rfq_id}/quotes", tags=["rfq"], summary="Quotes on one of our RFQs", responses=RFQ_RESPONSES)
    async def rfq_quotes(principal: Caller, engine: EngineDep, rfq_id: Annotated[str, Path()],
                         venue: Annotated[str | None, Query()] = None,
                         status: Annotated[QuoteStatus | None, Query()] = None) -> PageResponse[RfqQuote]:
        name = rfq_venue(engine, venue, rfq_id=rfq_id, capability="rfq_taker")
        require_view(principal, engine, name)
        rows = await tagged(name, engine.adapters[name].fetch_quotes(rfq_id, status=status))
        return PageResponse[RfqQuote](data=rows, next_cursor=None, count=len(rows))

    @router.post("/quotes/{quote_id}/accept", tags=["rfq"], summary="Take one side of a quote",
                 responses=ACCEPT_RESPONSES, status_code=202)
    async def accept_quote(principal: Caller, engine: EngineDep, response: Response,
                           quote_id: Annotated[str, Path()], body: AcceptBody) -> QuoteResult:
        """An order: the quote is re-read, its price checked against
        `worst_price` (`409`, rule `worst_price`), an `amount` sized into
        contracts, and every order risk rule run before the venue is called.
        A retry under the same `client_order_id` never sends twice."""
        name = rfq_venue(engine, body.venue, account=body.account, rfq_id=body.rfq_id, quote_id=quote_id,
                         capability="rfq_taker")
        require(principal, "trade", account_key(body.account, engine, name))
        accept = AcceptQuote.model_validate({**body.model_dump(exclude={"venue", "wait", "timeout_s"}),
                                             "quote_id": quote_id})
        quote = await tagged(name, engine.accept_quote(accept, venue=name, wait=body.wait, timeout_s=body.timeout_s))
        result = await quote_result(engine, quote, accept.client_order_id or f"rfq-accept:{name}:{quote_id}", name)
        if body.wait and result.final:
            response.status_code = 200
        return result

    # -- quoter ---------------------------------------------------------------

    @router.get("/quotes", tags=["rfq"], summary="Our quotes (the quoter side)", responses=RFQ_RESPONSES)
    async def my_quotes(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="Only this venue")] = None,
        rfq_id: Annotated[str | None, Query(description="Only quotes on this RFQ")] = None,
        status: Annotated[QuoteStatus | None, Query()] = None,
    ) -> PageResponse[RfqQuote]:
        """As each venue lists them, with the expiry the engine holds (the
        venue may not take one) filled in."""
        rows: list[RfqQuote] = []
        for name in venues_for(engine, principal, venue, "rfq_quoter"):
            for quote in await tagged(name, engine.adapters[name].fetch_my_quotes(rfq_id=rfq_id, status=status)):
                held = engine.rfq.maker_quotes.get(f"{name}:{quote.id}")
                if held and held.get("expires_at") and quote.expires_at is None:
                    quote = quote.model_copy(update={"expires_at": held["expires_at"]})
                rows.append(quote)
        return PageResponse[RfqQuote](data=rows, next_cursor=None, count=len(rows))

    @router.post("/quotes", tags=["rfq"], summary="Quote an incoming RFQ", responses=RFQ_RESPONSES, status_code=201)
    async def send_quote(principal: Caller, engine: EngineDep, body: MakerQuoteBody) -> RfqQuote:
        """Journaled; refused while halted, on a paused book, a restricted
        market or over the order size limits. `expires_at` is held by the
        engine, which cancels the quote when it passes."""
        name = rfq_venue(engine, body.venue, account=body.account, rfq_id=body.rfq_id, capability="rfq_quoter")
        require(principal, "trade", account_key(body.account, engine, name))
        quote = MakerQuote.model_validate(body.model_dump(exclude={"venue"}))
        return await tagged(name, engine.send_quote(quote, venue=name))

    @router.put("/quotes/{quote_id}", tags=["rfq"], summary="Replace one of our quotes", responses=RFQ_RESPONSES)
    async def replace_quote(principal: Caller, engine: EngineDep, quote_id: Annotated[str, Path()],
                            body: MakerQuoteBody) -> RfqQuote:
        """The new quote may carry a new id (`quote.replaced` names both)."""
        name = rfq_venue(engine, body.venue, account=body.account, quote_id=quote_id, rfq_id=body.rfq_id,
                         capability="rfq_quoter")
        require(principal, "trade", account_key(body.account, engine, name))
        quote = MakerQuote.model_validate(body.model_dump(exclude={"venue"}))
        return await tagged(name, engine.replace_quote(quote_id, quote, venue=name))

    @router.delete("/quotes/{quote_id}", tags=["rfq"], summary="Withdraw one of our quotes", responses=RFQ_RESPONSES)
    async def cancel_quote(principal: Caller, engine: EngineDep, quote_id: Annotated[str, Path()],
                           rfq_id: Annotated[str | None, Query()] = None,
                           venue: Annotated[str | None, Query()] = None) -> Canceled:
        """Also declines it during the last look. Never refused by risk."""
        name = rfq_venue(engine, venue, quote_id=quote_id, rfq_id=rfq_id, capability="rfq_quoter")
        require(principal, "trade", account_key(None, engine, name))
        await tagged(name, engine.cancel_quote(quote_id, rfq_id=rfq_id, venue=name))
        return Canceled(id=quote_id, venue=name, kind="quote")

    @router.post("/quotes/{quote_id}/confirm", tags=["rfq"], summary="Confirm an accepted quote of ours",
                 responses=ACCEPT_RESPONSES, status_code=202)
    async def confirm_quote(principal: Caller, engine: EngineDep, response: Response,
                            quote_id: Annotated[str, Path()], body: ConfirmBody | None = None) -> QuoteResult:
        """The maker's last look. Commits capital, so every order risk rule
        runs on the side and size that would trade; refused while halted."""
        body = body or ConfirmBody()
        name = rfq_venue(engine, body.venue, quote_id=quote_id, rfq_id=body.rfq_id, capability="rfq_quoter")
        require(principal, "trade", account_key(None, engine, name))
        quote = await tagged(name, engine.confirm_quote(quote_id, rfq_id=body.rfq_id, venue=name, wait=body.wait,
                                                        timeout_s=body.timeout_s))
        result = await quote_result(engine, quote, f"rfq-confirm:{name}:{quote_id}", name)
        if body.wait and result.final:
            response.status_code = 200
        return result

    # -- combos ---------------------------------------------------------------

    @router.post("/combos", tags=["rfq"], summary="Create or look up the combo for these legs",
                 responses=RFQ_RESPONSES)
    async def create_combo(principal: Caller, engine: EngineDep, body: CreateComboBody) -> Combo:
        """Idempotent: the same legs give the same combo (an existing one
        costs no venue quota). A signed write, so `trade`. The legs are kept
        by the engine (`combo.saved`) and shown on the combo's position.
        On Kalshi, `collection_id` names the collection to build it in."""
        name = rfq_venue(engine, body.venue, market_id=body.legs[0].market_id, capability="combos")
        require(principal, "trade", account_key(None, engine, name))
        return await tagged(name, engine.create_combo(body.legs, venue=name, collection_id=body.collection_id))

    @router.post("/combos/lookup", tags=["rfq"], summary="The combo for these legs, if it exists",
                 responses=RFQ_RESPONSES)
    async def lookup_combo(principal: Caller, engine: EngineDep, body: ComboBody) -> Combo | None:
        """A read: `null` when the venue has no such combo. `501` where the
        venue has no read-only lookup (Polymarket US: use `POST /combos`)."""
        name = rfq_venue(engine, body.venue, market_id=body.legs[0].market_id, capability="combos")
        require_view(principal, engine, name)
        return await tagged(name, engine.adapters[name].lookup_combo(body.legs))

    @router.get("/combos/collections", tags=["rfq"], summary="Where combos can be built (Kalshi collections)",
                responses=RFQ_RESPONSES)
    async def combo_collections(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="The venue; defaults to the only one with collections")] = None,
        status: Annotated[Literal["unopened", "open", "closed"] | None, Query()] = "open",
        event_id: Annotated[str | None, Query(description="Only collections that take this event")] = None,
        limit: Annotated[int | None, Query(ge=1, le=200)] = None,
        cursor: Annotated[str | None, Query()] = None,
    ) -> PageResponse[dict[str, Any]]:
        """The venue's own collection objects (Kalshi's multivariate event
        collections: `collection_ticker`, `associated_event_tickers` (always
        present: every event a leg may come from), `associated_events`,
        `size_min`, `size_max`...). `501` on a venue without collections.
        Served from memory for 15 s."""
        if venue is None:
            with_collections = [v for v, a in engine.adapters.items() if hasattr(a, "fetch_combo_collections")]
            venue = with_collections[0] if len(with_collections) == 1 else None
        name = rfq_venue(engine, venue, capability="combos")
        require_view(principal, engine, name)
        fetch = getattr(engine.adapters[name], "fetch_combo_collections", None)
        if fetch is None:
            from ..errors import NotSupported
            raise NotSupported(f"{name}: combo collections")
        page = await combo_lists.get(
            ("collections", name, status, event_id, limit, cursor),
            lambda: tagged(name, fetch(status=status, event_id=event_id, limit=limit, cursor=cursor)))
        rows = list(page)
        return PageResponse[dict[str, Any]](data=rows, next_cursor=page.next_cursor, count=len(rows))

    @router.get("/combos/markets", tags=["rfq"], summary="Markets a combo may take as legs (Polymarket, Polymarket US)",
                responses=RFQ_RESPONSES)
    async def combo_markets(
        principal: Caller,
        engine: EngineDep,
        venue: Annotated[str | None, Query(description="`polymarket` or `polymarket_us`; defaults to the only "
                                                       "venue with such a list")] = None,
        event_id: Annotated[str | None, Query(description="Polymarket US: only this event's markets "
                                                          "(`polymarket_us:<event slug>`). Polymarket: `400`")] = None,
        tag: Annotated[str | None, Query(description="Only this tag: Polymarket US's tag slug (`nfl`, `soccer`); "
                                                     "on Polymarket a catalogue tag, applied to each page")] = None,
        limit: Annotated[int | None, Query(ge=1, le=500, description="Polymarket: markets per page. Polymarket "
                                                                     "US: events read per page (default 50)")] = None,
        cursor: Annotated[str | None, Query()] = None,
    ) -> PageResponse[ComboMarket]:
        """The venue's own list of combo-eligible markets, as `ComboMarket`
        rows: `market_id` (the leg's), `title`, `event_id`/`event_title`
        where listed, `position_ids` (Polymarket: per outcome, the leg's
        `info.position_id`), `min_legs`/`max_legs`, the venue's `status` and
        `tags`, and the venue's row in `info`. A page can be short or empty
        with a `next_cursor`: the venues filter after paging. Served from
        memory for 15 s, so it can be polled. `501` on a venue without such
        a list (Kalshi: `GET /combos/collections`)."""
        if venue is None:
            listing = [v for v, a in engine.adapters.items() if lists_combo_markets(a)]
            venue = listing[0] if len(listing) == 1 else None
        name = rfq_venue(engine, venue, capability="combos")
        require_view(principal, engine, name)
        fetch = getattr(engine.adapters[name], "fetch_combo_eligible_markets", None)
        if fetch is None:
            from ..errors import NotSupported
            raise NotSupported(f"{name}: combo-eligible markets")
        page = await combo_lists.get(
            ("markets", name, event_id, tag, limit, cursor),
            lambda: tagged(name, fetch(event_id=event_id, tag=tag, limit=limit, cursor=cursor)))
        rows = list(page)
        return PageResponse[ComboMarket](data=rows, next_cursor=page.next_cursor, count=len(rows))

    boards = TtlCache(ttl_s=COMBO_BOARD_TTL_S, size=16)

    async def whole_board(request: Request, engine: Engine, name: str, pages: int) -> ComboBoard:
        """serve's warm board (never waiting on a build: `status: building`
        until the first ends); without a warmer (a bare trading app, or
        `[combo_board] warm = false`), built on request and kept 30 s."""
        warmer = getattr(request.app.state, "combo_boards", None)
        if warmer is not None and warmer.warms(name):
            return warmer.board(name)
        reader = catalogue_reader(request, name)
        return await boards.get(("board", name, pages),
                                lambda: tagged(name, board_for(name, engine.adapters[name], reader, pages=pages)))

    @router.get("/combos/board/categories", tags=["rfq"], summary="A combo board's categories, with counts",
                responses=RFQ_RESPONSES)
    async def combo_board_categories(
        principal: Caller,
        engine: EngineDep,
        request: Request,
        venue: Annotated[str, Query(description="A venue with `combos`: `kalshi`, `polymarket`, `polymarket_us`")],
    ) -> ComboBoardCategories:
        """The rail of a combo builder: the board's categories (leagues,
        kinds) with their event counts, the markets read and the events on
        the board, its `status` (`building` until serve's first build of it
        ends), `built_at` and `complete`. A few hundred bytes; read the
        events a category at a time from `GET /combos/board?category=`."""
        name = rfq_venue(engine, venue, capability="combos")
        require_view(principal, engine, name)
        return categories_view(await whole_board(request, engine, name, BOARD_PAGES))

    @router.get("/combos/board", tags=["rfq"], summary="A venue's combo-eligible markets as games and questions",
                responses=RFQ_RESPONSES)
    async def combo_board(
        principal: Caller,
        engine: EngineDep,
        request: Request,
        venue: Annotated[str, Query(description="A venue with `combos`: `kalshi`, `polymarket`, `polymarket_us`")],
        pages: Annotated[int, Query(ge=1, le=20, description="Without serve's warmer only: pages of the venue's "
                                                             "list to read")] = BOARD_PAGES,
        category: Annotated[str | None, Query(max_length=40, description="One category's events (`nfl`, "
                                                                         "`politics`; `all`: every one), a page "
                                                                         "at a time")] = None,
        cursor: Annotated[str | None, Query(max_length=20, description="The page after the one that gave it")] = None,
        limit: Annotated[int | None, Query(ge=1, le=MAX_PAGE_LIMIT, description="Events per page (default "
                                                                                 f"{PAGE_LIMIT})")] = None,
        q: Annotated[str | None, Query(max_length=100, description="Only events whose title, category or "
                                                                    "participants hold every word")] = None,
        sort: Annotated[BoardSort, Query(description="`board` (start time), `volume` (largest first) within each "
                                                     "category, or `start` (games ahead first, soonest first)")] = "board",
        competition: Annotated[str | None, Query(max_length=120, description="Only this league's events, as a "
                                                                             "category's `competitions` name it")] = None,
    ) -> ComboBoard:
        """What a combo builder draws (`synpath/engine/combo_board.py`):
        games (participants, and moneyline / spread / total picks where the
        venue lists those types), YES / NO questions for everything else,
        and categories (leagues, kinds) with counts. Each pick is a leg:
        `market_id` and `outcome`, with the catalogue's prices. Kalshi's
        board is its open collections' events (their markets read from the
        server's catalogue); Polymarket's and Polymarket US's, their own
        eligible lists.

        serve keeps every combo venue's board warm (built at start-up from
        the venue's whole list, rebuilt each minute; docs/server.md), so
        this never waits on a venue: until the first build ends it answers
        `status: "building"` with no events. With `category`, `cursor`,
        `limit`, `q` or `sort` it answers one page (`category`, `total`,
        `next_cursor`) with the whole board's categories; without any, the
        whole board, as before."""
        name = rfq_venue(engine, venue, capability="combos")
        require_view(principal, engine, name)
        board = await whole_board(request, engine, name, pages)
        if category is None and cursor is None and limit is None and not q and sort == "board" and not competition:
            return board
        try:
            return board_page(board, category=category, cursor=cursor, limit=limit or PAGE_LIMIT, query=q, sort=sort,
                              competition=competition)
        except BadRequest as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None

    @router.get("/combos/{market_id}", tags=["rfq"], summary="One combo and its legs", responses=RFQ_RESPONSES)
    async def get_combo(principal: Caller, engine: EngineDep, market_id: Annotated[str, Path()],
                        venue: Annotated[str | None, Query()] = None) -> Combo:
        """From the engine's saved legs, else the venue."""
        name = rfq_venue(engine, venue, market_id=market_id, capability="combos")
        require_view(principal, engine, name)
        combo = await engine.rfq.combo(market_id)
        if combo is not None:
            return combo
        return await tagged(name, engine.adapters[name].fetch_combo(market_id))

    add_rfq_group_routes(router)


# ---------------------------------------------------------------------------
# Cross-venue RFQ groups (docs/rfq-combo.md, "Cross-venue RFQ")
# ---------------------------------------------------------------------------

from ..engine.combo_board import (  # noqa: E402
    BOARD_PAGES, MAX_PAGE_LIMIT, PAGE_LIMIT, BoardSort, ComboBoard, ComboBoardCategories, board_for, board_page,
    categories_view,
)
from ..errors import BadRequest  # noqa: E402
from ..engine.rfq_group import GroupPreview, RfqGroup  # noqa: E402


def catalogue_reader(request: Request, venue: str) -> Any | None:
    """The server's read adapter for a venue (serve's market-data registry),
    or None where this app has none (a bare `create_trading_app`)."""
    registry = getattr(request.app.state, "registry", None)
    if registry is None:
        return None
    try:
        return registry.get(venue)
    except Exception:
        return None


async def preview_prices(request: Request, preview: GroupPreview) -> None:
    """Each offered venue's leg prices from the catalogue (YES ask for a YES
    leg, 1 - YES bid for a NO leg) and their product, best effort."""
    import asyncio

    for venue in preview.offered:
        if not venue.legs:
            continue
        reader = catalogue_reader(request, venue.venue)
        found: dict[str, Any] = {}
        if reader is not None:
            try:
                found = {m.id: m for m in await asyncio.to_thread(reader.fetch_markets_by_ids,
                                                                   [leg.market_id for leg in venue.legs])}
            except Exception:
                found = {}
        prices: list[Decimal | None] = []
        for leg in venue.legs:
            market = found.get(leg.market_id)
            quote = market.yes.quote if market is not None else None
            raw = (quote.ask if leg.outcome == "yes" else (1 - quote.bid if quote.bid is not None else None)) \
                if quote is not None else None
            price = Decimal(str(raw)).quantize(Decimal("0.0001")) if raw is not None and 0 < raw < 1 else None
            prices.append(price)
        venue.leg_prices = prices
        if prices and all(p is not None for p in prices):
            combined = Decimal(1)
            for price in prices:
                combined *= price  # type: ignore[operator]
            venue.combined_price = combined.quantize(Decimal("0.000001"))


class RfqGroupBody(BaseModel):
    """One RFQ on every venue that lists the instrument, ranked as one."""

    instrument_id: str | None = Field(
        default=None, description="A market (`venue:native`), a bucket (`bucket:<id>`), or a combo's own market id "
                                  "the engine holds the legs of. Exactly one of this and `legs`")
    legs: list[ComboLeg] | None = Field(
        default=None, min_length=2, description="A combo's legs: each a market on any venue (or a bucket) and the "
                                                "outcome it needs; mapped onto each combo venue by matching")
    quantity: Decimal | None = Field(default=None, gt=0, description="Contracts. Exactly one of this and `amount`")
    amount: Decimal | None = Field(default=None, gt=0, description="Money: quotes rank by the contracts it buys "
                                                                   "after each venue's taker fee")
    outcome: Outcome = Field(default="yes", description="The outcome the requester trades, in the instrument's terms")
    action: Action = "buy"
    cross_venue: bool = Field(default=False, description="A plain market: also ask every other RFQ venue's "
                                                         "matched market. Buckets and combos always fan out")
    venues: list[str] | None = Field(default=None, description="Only these venues")
    exclude_venues: list[str] = Field(default_factory=list, description="Not these venues")
    book: str | None = None
    trader: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _consistent(self) -> "RfqGroupBody":
        if (self.quantity is None) == (self.amount is None):
            raise ValueError("size an RFQ group by quantity (contracts) or amount (money), exactly one")
        if (self.instrument_id is None) == (self.legs is None):
            raise ValueError("give instrument_id or legs, exactly one")
        return self


class RfqGroupPreviewBody(BaseModel):
    """`POST /rfq-groups` without creating anything: the size is optional."""

    instrument_id: str | None = Field(default=None, description="A market, a bucket or a combo's own id. Exactly "
                                                                "one of this and `legs`")
    legs: list[ComboLeg] | None = Field(default=None, min_length=2)
    quantity: Decimal | None = Field(default=None, gt=0)
    amount: Decimal | None = Field(default=None, gt=0, description="Unsized, a buy by amount is planned")
    outcome: Outcome = "yes"
    action: Action = "buy"
    cross_venue: bool = False
    venues: list[str] | None = None
    exclude_venues: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _consistent(self) -> "RfqGroupPreviewBody":
        if self.quantity is not None and self.amount is not None:
            raise ValueError("size a preview by quantity or amount, at most one")
        if (self.instrument_id is None) == (self.legs is None):
            raise ValueError("give instrument_id or legs, exactly one")
        return self


class GroupAcceptBody(BaseModel):
    quote_id: str = Field(description="A quote from the group's ranked list")
    venue: str | None = Field(default=None, description="The quote's venue. Required when the group spans more "
                                                        "than one venue: quote ids are the venues' own, so two "
                                                        "venues may return the same one (`400 venue_required`)")
    worst_price: Decimal | None = Field(default=None, gt=0, lt=1, description="The worst YES price accepted, in the "
                                                                              "group's terms (a flipped member's is "
                                                                              "turned over)")
    wait: bool = Field(default=False, description="Follow the accept until the quote is final, up to `timeout_s`")
    timeout_s: float = Field(default=30.0, gt=0, le=120)
    tags: dict[str, str] = Field(default_factory=dict)


class GroupAcceptResult(BaseModel):
    """What an accept on a group did, as far as it is known yet."""

    group: RfqGroup
    quote: RfqQuote = Field(description="The member quote, as its venue answered")
    venue: str
    client_order_id: str = Field(description="The member accept's intent key")
    claim_key: str = Field(description="The group's claim intent: one live claim per group")
    final: bool
    intent_state: str | None = None
    order_id: str | None = None


GROUP_ACCEPT_RESPONSES: dict[int | str, dict[str, Any]] = {
    **RFQ_RESPONSES,
    200: {"model": GroupAcceptResult, "description": "`wait` was true and the quote is final"},
    202: {"description": "Claimed, the other venues cancelled, the accept sent; the rest arrives as `rfq_group.*` "
                         "and `quote.*` events. Also `submission_in_doubt` when the venue gave no answer"},
    409: {"model": ErrorBody, "description": "A risk rule or the halt (`details.rule`), or the group already has an "
                                             "accept in flight (`rfq_group_accept_in_flight`) or done "
                                             "(`rfq_group_accepted`)"},
}


def group_accounts(engine: Engine, group: RfqGroup) -> list[str]:
    return [account_key(None, engine, venue) for venue in sorted(group.venues())]


def require_group(principal: Principal, engine: Engine, group: RfqGroup, permission: str) -> None:
    """A group is seen by a key that may see every venue it asks, and changed
    (accepted, cancelled) by one that may trade every one: an accept cancels
    the others."""
    for account in group_accounts(engine, group):
        if permission == "view":
            if not visible(principal, account):
                raise HTTPException(status_code=403, detail=f"this key may not view {account}")
        else:
            require(principal, "trade", account)


def add_rfq_group_routes(router: APIRouter) -> None:
    @router.post("/rfq-groups", tags=["rfq"], summary="Ask every venue that lists it", responses=RFQ_RESPONSES,
                 status_code=201)
    async def create_rfq_group(principal: Caller, engine: EngineDep, body: RfqGroupBody) -> RfqGroup:
        """One RFQ per venue: a bucket's members, a combo's legs mapped onto
        each combo venue, or a market and (with `cross_venue`) its matches.
        Venues this key may not trade are skipped (`forbidden`), like those
        missing a leg or refusing one, each with its reason in `skipped`.
        `400 no_venue` (with `details.skipped`) when none is left. Quotes
        arrive as `rfq_group.quote` and `rfq_group.updated` events."""
        capable = [v for v, a in engine.adapters.items() if a.has.get("rfq_taker")]
        for name in [*(body.venues or []), *body.exclude_venues]:
            if name not in engine.adapters:
                raise http_error(404, "unknown_venue", f"this server does not trade {name}", venue=name)
        refused = {v: f"this key may not trade {account_key(None, engine, v)}" for v in capable
                   if not principal.may("trade", account_key(None, engine, v))}
        for name in body.venues or []:
            if name in refused:
                raise HTTPException(status_code=403, detail=refused[name])
        if capable and len(refused) == len(capable):
            raise HTTPException(status_code=403, detail="this key may not trade on any RFQ venue")
        from ..errors import BadRequest
        try:
            return await engine.create_rfq_group(
                body.instrument_id, legs=body.legs, quantity=body.quantity, amount=body.amount,
                outcome=body.outcome, action=body.action, cross_venue=body.cross_venue, venues=body.venues,
                exclude_venues=body.exclude_venues, refused_venues=refused, book=body.book, trader=body.trader,
                tags=body.tags)
        except BadRequest as exc:
            skipped = getattr(exc, "skipped", None)
            if skipped is not None:
                raise http_error(400, "no_venue", str(exc), skipped=skipped) from None
            raise

    previews = TtlCache(ttl_s=PREVIEW_TTL_S, size=128)

    @router.post("/rfq-groups/preview", tags=["rfq"], summary="Which venues a group would ask, without creating it",
                 responses=RFQ_RESPONSES)
    async def preview_rfq_group(principal: Caller, engine: EngineDep, request: Request,
                                body: RfqGroupPreviewBody) -> GroupPreview:
        """A dry run of `POST /rfq-groups`: each venue's mapped market or
        legs (matching, eligibility, Kalshi's collection) in `offered`, with
        the legs' catalogue prices and their product (an estimate, not a
        quote), or the reason it would be skipped. Nothing is created,
        journaled or sent to a venue's RFQ desk; the matching service and
        the venues' eligibility lists are read. Served from memory for 10 s."""
        capable = [v for v, a in engine.adapters.items() if a.has.get("rfq_taker")]
        for name in [*(body.venues or []), *body.exclude_venues]:
            if name not in engine.adapters:
                raise http_error(404, "unknown_venue", f"this server does not trade {name}", venue=name)
        refused = {v: f"this key may not trade {account_key(None, engine, v)}" for v in capable
                   if not principal.may("trade", account_key(None, engine, v))}

        async def read() -> GroupPreview:
            preview = await engine.preview_rfq_group(
                body.instrument_id, legs=body.legs, quantity=body.quantity, amount=body.amount,
                outcome=body.outcome, action=body.action, cross_venue=body.cross_venue, venues=body.venues,
                exclude_venues=body.exclude_venues, refused_venues=refused)
            await preview_prices(request, preview)
            return preview

        key = ("preview", body.model_dump_json(), tuple(sorted(refused)))
        return (await previews.get(key, read)).model_copy(deep=True)

    @router.get("/rfq-groups", tags=["rfq"], summary="Cross-venue RFQ groups", responses=RFQ_RESPONSES)
    async def list_rfq_groups(
        principal: Caller,
        engine: EngineDep,
        status: Annotated[str | None, Query(description="Only groups in this state")] = None,
        limit: Annotated[int | None, Query(ge=1, le=500)] = None,
    ) -> PageResponse[RfqGroup]:
        """The groups the engine holds (open ones, and those finished since
        it started), newest first, that this key may see every venue of."""
        rows = [g for g in engine.rfq_groups.list(status=status)
                if all(visible(principal, a) for a in group_accounts(engine, g))]
        rows = rows[:limit] if limit else rows
        return PageResponse[RfqGroup](data=rows, next_cursor=None, count=len(rows))

    @router.get("/rfq-groups/{group_id}", tags=["rfq"], summary="One group: members, skipped venues, ranked quotes",
                responses=RFQ_RESPONSES)
    async def get_rfq_group(principal: Caller, engine: EngineDep, group_id: Annotated[str, Path()],
                            refresh: Annotated[bool, Query(description="Read every open member's quotes from its "
                                                                       "venue first")] = False) -> RfqGroup:
        group = await engine.rfq_group(group_id)
        require_group(principal, engine, group, "view")
        if refresh and not group.final:
            group = await engine.rfq_groups.refresh(group_id)
        return group

    @router.post("/rfq-groups/{group_id}/accept", tags=["rfq"], summary="Take one quote; cancel the other venues",
                 responses=GROUP_ACCEPT_RESPONSES, status_code=202)
    async def accept_rfq_group(principal: Caller, engine: EngineDep, response: Response,
                               group_id: Annotated[str, Path()], body: GroupAcceptBody) -> GroupAcceptResult:
        """Checked like any accept (risk, `worst_price`) before anything is
        written; then the group is claimed in the journal, every other
        member RFQ is cancelled (Polymarket global's are left to expire), and
        the quote is accepted. One accept per group: a second is `409`. The
        quote is named by `{quote_id, venue}`; `venue` may be left out only
        on a one-venue group (`400 venue_required`)."""
        group = await engine.rfq_group(group_id)
        require_group(principal, engine, group, "trade")
        from ..errors import BadRequest
        try:
            done = await engine.accept_group_quote(group_id, body.quote_id, venue=body.venue,
                                                   worst_price=body.worst_price, wait=body.wait,
                                                   timeout_s=body.timeout_s, tags=body.tags)
        except BadRequest as exc:
            reason = getattr(exc, "reason", None)
            if reason in ("venue_required", "venue_not_in_group"):
                raise http_error(400, reason, str(exc), venues=sorted(group.venues())) from None
            raise
        intent = await engine.journal.intent(done.client_order_id)
        result = GroupAcceptResult(group=done.group, quote=done.quote, venue=done.venue,
                                   client_order_id=done.client_order_id, claim_key=done.claim_key,
                                   final=done.quote.is_terminal, intent_state=intent.state if intent else None,
                                   order_id=(intent.order_id if intent else None) or None)
        if body.wait and result.final:
            response.status_code = 200
        return result

    @router.delete("/rfq-groups/{group_id}", tags=["rfq"], summary="Cancel every venue's RFQ in a group",
                   responses=RFQ_RESPONSES)
    async def cancel_rfq_group(principal: Caller, engine: EngineDep, group_id: Annotated[str, Path()]) -> RfqGroup:
        """Never refused by risk, so it works while halted. `409` while an
        accept is in flight (the others are cancelled already)."""
        group = await engine.rfq_group(group_id)
        require_group(principal, engine, group, "trade")
        return await engine.cancel_rfq_group(group_id)
