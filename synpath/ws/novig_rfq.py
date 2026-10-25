"""Novig's parlay RFQ sockets: the maker WebSocket a pricer answers rounds
on, and the public tape of every RFQ execution.

**The maker WebSocket** (`wss://api.novig.com/rfq/ws`, QA
`wss://api-qa.novig.us/rfq/ws`) takes the market-maker bearer token on the
upgrade and refuses an account with no pricer registration (`403`). Frames
are JSON `{event, data}`; Novig adds `timestamp` to its own. After every
connect the stream subscribes to both channels: `rfq` (`rfq_created`,
`rfq_closed`, `rfq_executed`, to every pricer) and `quotes` (`quote_created`,
`quote_accepted`, `quote_executed`, `quote_unfilled`, `quote_dropped`, to
this pricer alone). Without `quotes` no last look arrives and every win is
rejected.

A round opens with `rfq_created`: the stream hands it to the pricer and
sends `create_quote` with the answer, or `decline`, before `expires_at`. A
win arrives as `quote_accepted`, answered with `confirm` within 1 second
under Novig's id for the quote. Every message sent gets one ack or one
`error`; an error never closes the connection. A `FORBIDDEN` error naming a
confirm-rate lock records the lock, and the pricer stops quoting until it
ends.

Novig neither replays nor backfills: a reconnect starts at the live edge,
so it is marked `reconcile_required`; read the RFQs won from
`NovigRFQ.reconcile_executions`. Novig pings every 15 s and the WebSocket
library answers.

Every frame is emitted as a `VenueEvent` named by its `event`, with `data`
as the payload, so a caller sees the rounds, its quotes' fates and the acks.

**The execution feed** (`/rfq/executions/feed`) carries every RFQ execution
on Novig, naming no pricer, each with an `offset` that grows by one; a jump
is a gap (`StreamStatusEvent(state="gap", reconcile_required=True)`), filled
from `NovigRFQ.reconcile_executions`.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..trading import novig_rfq as rfq_api
from ..trading.credentials import NovigRFQCredentials
from ..trading.novig_rfq import LastLook, NovigPricer, ParlayRFQ, lock_of_message, ms_of
from .base import Event, Stream, VenueEvent, now_ms

log = logging.getLogger("synpath.ws.novig_rfq")

VENUE = "novig"
CHANNELS = ("rfq", "quotes")

FINAL_STATUSES = {
    401: "the bearer token is missing or invalid",
    403: "not a market-maker token, or no pricer registered (POST /rfq/pricer first)",
    404: "the RFQ socket is off in this environment",
}
"""Upgrade refusals Novig says not to retry."""


def _status_of(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None) or getattr(exc, "status_code", None)
    return int(code) if code else None


class _Bearer(Stream):
    """A socket authenticated by the market-maker bearer token on the upgrade."""

    venue = VENUE

    def __init__(self, url: str, *, token: Any, **kwargs: Any):
        self.token = token
        self._bearer: str | None = None
        super().__init__(url, **kwargs)

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._bearer}"} if self._bearer else {}

    async def _open(self) -> Any:
        try:
            self._bearer = await self.token.get()
        except Exception as exc:
            self._fail(f"no bearer token: {exc}")
            raise
        try:
            return await super()._open()
        except Exception as exc:
            code = _status_of(exc)
            if code in FINAL_STATUSES:
                if code == 401:
                    self.token.invalidate()
                self._fail(f"upgrade refused ({code}): {FINAL_STATUSES[code]}")
            raise

    def _fail(self, detail: str) -> None:
        self._closing = True
        self.status("failed", detail)
        from .base import _CLOSED

        self._queue.put_nowait(_CLOSED)

    def _later(self, coroutine: Any) -> asyncio.Task | None:
        try:
            return asyncio.get_running_loop().create_task(coroutine)
        except RuntimeError:
            coroutine.close()
            return None

    def _event(self, message: dict[str, Any]) -> VenueEvent:
        data = message.get("data")
        return VenueEvent(venue=VENUE, name=str(message.get("event") or ""),
                          payload=data if isinstance(data, dict) else {"data": data},
                          timestamp=ms_of(message.get("timestamp")))


class NovigPricerStream(_Bearer):
    """The maker WebSocket, answering every round with a `NovigPricer`."""

    name = "rfq"
    private = True

    def __init__(self, pricer: NovigPricer, *, url: str | None = None, **kwargs: Any):
        self.pricer = pricer
        env = pricer.env
        if env not in rfq_api.WS_URLS:
            raise ValueError(f"{VENUE}: the RFQ env is production or qa, got {env!r}")
        self._pricing: dict[str, asyncio.Task] = {}
        self._confirms: dict[str, asyncio.Task] = {}
        self._confirmed: dict[str, bool] = {}
        super().__init__(url or rfq_api.WS_URLS[env], token=pricer.rfq.token, **kwargs)

    async def on_connect(self) -> None:
        for channel in CHANNELS:
            await self.send({"event": "subscribe", "data": channel})

    def on_disconnect(self) -> None:
        # A round being priced cannot be answered on the next connection.
        for task in self._pricing.values():
            task.cancel()
        self._pricing.clear()

    async def close(self) -> None:
        for task in [*self._pricing.values(), *self._confirms.values()]:
            task.cancel()
        self._pricing.clear()
        self._confirms.clear()
        await super().close()

    # -- frames ---------------------------------------------------------------

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict):
            return []
        name = message.get("event")
        data = message.get("data") if isinstance(message.get("data"), dict) else {}
        if name == "error":
            self._error(data)
            return []
        if name in ("subscribed", "unsubscribed"):
            self.status("subscribed" if name == "subscribed" else "error",
                        f"{name} {data.get('channel', '')}".strip())
            return []
        if name == "rfq_created":
            rfq = ParlayRFQ.from_ws(data, received_at=now_ms())
            task = self._later(self._answer(rfq))
            if task is not None:
                self._pricing[rfq.rfq_id] = task
        elif name == "rfq_closed":
            task = self._pricing.pop(str(data.get("rfq_id")), None)
            if task is not None and not task.done():
                task.cancel()
        elif name == "quote_accepted":
            look = LastLook.from_ws(data, received_at=now_ms())
            task = self._later(self._confirm(look))
            if task is not None:
                self._confirms[look.rfq_id] = task
        return [self._event(message)]

    def _error(self, data: dict[str, Any]) -> None:
        code, text, rfq_id = str(data.get("code") or ""), str(data.get("message") or ""), data.get("rfq_id")
        if code == "FORBIDDEN":
            lock = lock_of_message(text)
            if lock is not None:
                self.pricer.confirm_rate.locked_out(until=lock.until, source=lock.source)
        elif code == "NO_CONFIRM_WINDOW" and rfq_id and self._confirmed.get(str(rfq_id)):
            # The confirm arrived after the window: Novig counts a miss.
            self.pricer.confirm_rate.amend(str(rfq_id), False)
        detail = f"novig refused: {code} {text}".strip()
        self.status("error", detail + (f" (rfq {rfq_id})" if rfq_id else ""), key=str(rfq_id) if rfq_id else None)

    # -- answering ------------------------------------------------------------

    async def _answer(self, rfq: ParlayRFQ) -> None:
        try:
            quote = await self.pricer.quote(rfq)
            if quote is not None:
                await self.send({"event": "create_quote", "data": quote.ws_data(rfq.rfq_id)})
            elif not self.pricer.confirm_rate.locked():
                # A decline says the pricer is up; silence looks like one that is down.
                await self.send({"event": "decline", "data": {"rfq_id": rfq.rfq_id}})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.status("error", f"answering rfq {rfq.rfq_id}: {type(exc).__name__}: {exc}", key=rfq.rfq_id)
        finally:
            self._pricing.pop(rfq.rfq_id, None)

    async def _confirm(self, look: LastLook) -> None:
        try:
            decision = await self.pricer.last_look(look)
            self._confirmed[look.rfq_id] = decision
            if len(self._confirmed) > 1000:
                self._confirmed.pop(next(iter(self._confirmed)))
            sent = await self.send({"event": "confirm", "data": {
                "rfq_id": look.rfq_id, "quote_id": look.quote_id, "confirmed": decision,
            }})
            if not sent and decision:
                # The connection dropped before the answer: Novig rejects the trade.
                self.pricer.confirm_rate.amend(look.rfq_id, False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.status("error", f"confirming rfq {look.rfq_id}: {type(exc).__name__}: {exc}", key=look.rfq_id)
        finally:
            self._confirms.pop(look.rfq_id, None)


class NovigExecutionFeed(_Bearer):
    """Every RFQ execution on Novig as it happens (`rfq_executed`), naming no
    pricer. Needs a bearer token, not a pricer registration."""

    name = "rfq_executions"

    def __init__(self, credentials: NovigRFQCredentials | None = None, *, env: str | None = None,
                 url: str | None = None, rfq: rfq_api.NovigRFQ | None = None, **kwargs: Any):
        rfq = rfq or rfq_api.NovigRFQ(credentials, env=env)
        self.rfq = rfq
        self.last_offset: int | None = None
        super().__init__(url or rfq_api.FEED_URLS[rfq.env], token=rfq.token, **kwargs)

    def handle(self, message: Any) -> list[Event]:
        if not isinstance(message, dict) or message.get("event") != "rfq_executed":
            return []
        data = message.get("data") if isinstance(message.get("data"), dict) else {}
        offset = data.get("offset")
        if isinstance(offset, int):
            if self.last_offset is not None and offset > self.last_offset + 1:
                self.status("gap", f"offsets {self.last_offset + 1} to {offset - 1} missed; "
                                   "reconcile from NovigRFQ.reconcile_executions", reconcile=True)
            if self.last_offset is None or offset > self.last_offset:
                self.last_offset = offset
        return [self._event(message)]
