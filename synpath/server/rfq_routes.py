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

from decimal import Decimal
from typing import Annotated, Any, Literal

from fastapi import APIRouter, HTTPException, Path, Query, Response
from pydantic import BaseModel, Field, model_validator

from .. import ids
from ..engine.engine import Engine
from ..errors import SynpathError
from ..trading.types import (
    AcceptQuote, Account, Action, Combo, ComboLeg, MakerQuote, Outcome, QuoteStatus, Rfq, RfqQuote, RfqStatus,
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


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def add_rfq_routes(router: APIRouter) -> None:
    """RFQs (taker and quoter sides), quotes and combos."""

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
    async def create_combo(principal: Caller, engine: EngineDep, body: ComboBody) -> Combo:
        """Idempotent: the same legs give the same combo (an existing one
        costs no venue quota). A signed write, so `trade`. The legs are kept
        by the engine (`combo.saved`) and shown on the combo's position."""
        name = rfq_venue(engine, body.venue, market_id=body.legs[0].market_id, capability="combos")
        require(principal, "trade", account_key(None, engine, name))
        return await tagged(name, engine.create_combo(body.legs, venue=name))

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
        collections: `collection_ticker`, `associated_events`, `size_min`,
        `size_max`...). `501` on a venue without collections."""
        if venue is None:
            with_collections = [v for v, a in engine.adapters.items() if hasattr(a, "fetch_combo_collections")]
            venue = with_collections[0] if len(with_collections) == 1 else None
        name = rfq_venue(engine, venue, capability="combos")
        require_view(principal, engine, name)
        fetch = getattr(engine.adapters[name], "fetch_combo_collections", None)
        if fetch is None:
            from ..errors import NotSupported
            raise NotSupported(f"{name}: combo collections")
        page = await tagged(name, fetch(status=status, event_id=event_id, limit=limit, cursor=cursor))
        rows = list(page)
        return PageResponse[dict[str, Any]](data=rows, next_cursor=page.next_cursor, count=len(rows))

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
