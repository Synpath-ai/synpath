"""SX Bet parlays as incoming RFQs (P8 of docs/rfq-combo.md), offline.

The request follows the venue's documented `parlay_markets:global` example
(tests/samples/sx_bet_parlay_request.json); the stream runs over a fake
socket speaking Centrifugo's JSON protocol; REST is mocked with respx.
Covered: the request as an `Rfq` with a `Combo`, the stream feeding the
adapter, a quote as signed orders that expire with the parlay, the re-sign
to the venue's named end, cancel and replace, reading a quote back from its
orders, the void-leg readings, error mapping, and the quoter flow through
`engine.rfq`.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from decimal import Decimal as D

import httpx
import pytest
import respx

from synpath.errors import NotSupported
from synpath.trading.credentials import SxBetCredentials
from synpath.trading.errors import InvalidOrder, OrderRejected, RfqNotFound
from synpath.trading.sx_bet import (
    PARLAY_EXPIRY_MARGIN_S, SxBetTrading, order_ids_of, order_of_row, parlay_result, quote_id_of, quote_of_orders,
    rfq_of_parlay,
)
from synpath.trading.types import ComboLeg, MakerQuote, OrderStatus, QuoteStatus, RfqCloseReason, RfqStatus, Side
from synpath.ws.base import VenueEvent
from synpath.ws.sx_bet import PARLAY_CHANNEL, SxBetParlayStream

from conftest import load

API = "https://api.sx.bet"
KEY = "0x" + "11" * 32
REQUEST = load("sx_bet_parlay_request.json")
PARENT = REQUEST["marketHash"]
COMBO_ID = f"sx_bet:{PARENT}"
LEG_ONE = f"sx_bet:{REQUEST['legs'][0]['marketHash']}"
LEG_TWO = f"sx_bet:{REQUEST['legs'][1]['marketHash']}"
META = {
    "chainId": 4162,
    "domain": {"name": "OBv3 Escrow", "version": "1", "chainId": 4162,
               "verifyingContract": "0x890482680C3a0116aBB003B17e7D694D13a6c1eB"},
    "activeAsset": {"symbol": "USDC", "baseToken": "0x6629Ce1Cf35Cc1329ebB4F63202F3f197b3F050B", "decimals": 6},
    "oddsLadderStepSize": 125,
    "limits": {"orderSizeMinimumBaseUnits": "1000000", "maxCreateOrders": 10, "maxCancelOrders": 100},
}
BID_ID, ASK_ID = "0x" + "b1" * 32, "0x" + "a1" * 32


def run(coro):
    return asyncio.run(coro)


def adapter(**kw) -> SxBetTrading:
    return SxBetTrading(SxBetCredentials(private_key=KEY, api_key="api-key"), catalog=_Catalog(), **kw)


class _Catalog:
    def close(self):
        pass


def submitted(req: httpx.Request) -> httpx.Response:
    """Every order placed and resting; the bid and the ask get fixed ids."""
    orders = json.loads(req.content)["orders"]
    return httpx.Response(200, json={"status": "success", "data": {"orders": [
        {"orderId": BID_ID if o["isMakerBettingOutcomeOne"] else ASK_ID, "status": "SUBMITTED", "commandId": "c",
         "outcome": {"state": "RESTED", "remainingAmount": o["totalBetSize"]}} for o in orders]}})


def order_row(order_id: str, *, outcome_one: bool, status: str = "ACTIVE", reason: str | None = None,
              remaining: str = "10000000", client: str | None = None) -> dict:
    return {
        "id": order_id, "marketHash": PARENT, "isBettingOutcomeOne": outcome_one,
        "percentageOdds": str(20 * 10 ** 18) if outcome_one else str(70 * 10 ** 18), "totalBetSize": "10000000",
        "remainingSize": remaining, "expiry": "2026-10-21T12:01:00.000Z", "status": status, "inactiveReason": reason,
        "clientOrderId": client, "createdAt": "2026-10-21T12:00:01.000Z", "updatedAt": "2026-10-21T12:00:01.000Z",
    }


def mocked(mock: respx.MockRouter, *, post=submitted) -> dict:
    mock.get("/metadata/obv3").respond(json={"status": "success", "data": META})
    return {"post": mock.post("/orders-v3").mock(side_effect=post)}


def quote(**kw) -> MakerQuote:
    return MakerQuote(**{"rfq_id": PARENT, "bid": D("0.20"), "ask": D("0.30"), **kw})


# -- the request ----------------------------------------------------------------------


class TestRequest:
    def test_a_request_is_an_incoming_two_sided_rfq_on_a_combo(self):
        rfq = rfq_of_parlay(REQUEST, received_at=1_000_000)
        assert (rfq.id, rfq.venue, rfq.combo_id, rfq.market_id) == (PARENT, "sx_bet", COMBO_ID, None)
        assert rfq.mine is False and rfq.status == RfqStatus.OPEN
        assert (rfq.outcome, rfq.action, rfq.quantity) == (None, None, None), "the bettor's side is not said"
        assert rfq.amount == D("50"), "requestSize is the stake asked for, in USDC base units"
        assert rfq.expires_at == 1_060_000 and rfq.info["expiry_exact"] is False
        assert rfq.info["requestorId"] == REQUEST["requestorId"] and "legs" not in rfq.info
        combo = rfq.combo
        assert combo.market_id == COMBO_ID and combo.venue == "sx_bet" and combo.status == "open"
        assert [(l.market_id, l.outcome) for l in combo.legs] == [(LEG_ONE, "no"), (LEG_TWO, "yes")]
        assert "VOID" in combo.info["void_leg_rule"]

    def test_a_request_without_a_size_still_maps(self):
        rfq = rfq_of_parlay({k: v for k, v in REQUEST.items() if k != "requestSize"}, received_at=0)
        assert rfq.amount is None and rfq.quantity is None

    def test_a_request_that_is_not_a_combo_is_dropped(self):
        sx = adapter()
        assert sx.on_parlay({**REQUEST, "legs": REQUEST["legs"][:1]}) is None
        assert sx.on_parlay({**REQUEST, "legs": [REQUEST["legs"][0]] * 2}) is None
        assert sx.parlays == {}


# -- the stream ---------------------------------------------------------------------------


class FakeSocket:
    """A Centrifugo server: records what is sent, hands back what a test pushes."""

    def __init__(self):
        self.sent: list = []
        self.inbox: asyncio.Queue = asyncio.Queue()

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        return await self.inbox.get()

    async def close(self):
        pass


def fake_stream() -> tuple[SxBetParlayStream, FakeSocket]:
    socket = FakeSocket()

    async def connect(url, **kw):
        return socket

    return SxBetParlayStream(token="tok", connect=connect), socket


def push(data) -> str:
    return json.dumps({"push": {"channel": PARLAY_CHANNEL, "pub": {"data": data}}})


async def settle(until=lambda: True, steps: int = 50):
    for _ in range(steps):
        if until():
            return
        await asyncio.sleep(0.005)


async def handshake(socket: FakeSocket):
    """Answer the connect, then the subscription, as the server does."""
    await settle(lambda: any("connect" in c for c in socket.sent))
    connect = next(c for c in socket.sent if "connect" in c)
    assert connect["connect"]["token"] == "tok"
    await socket.inbox.put(json.dumps({"id": connect["id"], "connect": {"client": "c", "pong": True}}))
    await settle(lambda: any("subscribe" in c for c in socket.sent))
    subscribe = next(c for c in socket.sent if "subscribe" in c)
    assert subscribe["subscribe"]["channel"] == PARLAY_CHANNEL
    await socket.inbox.put(json.dumps({"id": subscribe["id"], "subscribe": {}}))


class TestStream:
    def test_the_channel_emits_the_request_as_an_rfq(self):
        stream, _ = fake_stream()
        [event] = stream.on_publication(PARLAY_CHANNEL, REQUEST)
        assert isinstance(event, VenueEvent) and event.name == "parlay_request"
        assert event.payload["row"] == REQUEST and event.payload["rfq"].combo_id == COMBO_ID
        assert stream.on_publication("markets:global", REQUEST) == []

    def test_requests_reach_the_adapter_and_its_listeners(self):
        heard = []

        async def go():
            stream, socket = fake_stream()
            async with adapter(parlay_stream=stream) as sx:
                sx.subscribe_incoming_rfqs(heard.append)
                assert list(await sx.fetch_incoming_rfqs()) == [], "the first call starts the stream"
                await handshake(socket)
                await socket.inbox.put(push(REQUEST))
                await socket.inbox.put(push(REQUEST))             # the same request again
                await settle(lambda: PARENT in sx.parlays)
                incoming = await sx.fetch_incoming_rfqs()
                by_leg = await sx.fetch_incoming_rfqs(market_id=LEG_TWO)
                elsewhere = await sx.fetch_incoming_rfqs(market_id="sx_bet:0x" + "00" * 32)
                combo = await sx.fetch_combo(COMBO_ID)
                found = await sx.lookup_combo([ComboLeg(market_id=LEG_TWO, outcome="yes"),
                                               ComboLeg(market_id=LEG_ONE, outcome="no")])
                missing = await sx.lookup_combo([ComboLeg(market_id=LEG_TWO, outcome="no"),
                                                 ComboLeg(market_id=LEG_ONE, outcome="no")])
                await stream.close()
                return incoming, by_leg, elsewhere, combo, found, missing

        incoming, by_leg, elsewhere, combo, found, missing = run(go())
        assert [r.id for r in incoming] == [PARENT] and [r.id for r in by_leg] == [PARENT] and list(elsewhere) == []
        assert len(heard) == 1 and heard[0].id == PARENT
        assert combo.market_id == COMBO_ID and found == combo and missing is None

    def test_an_async_listener_is_scheduled(self):
        heard = []

        async def listener(rfq):
            heard.append(rfq.id)

        async def go():
            sx = adapter()
            sx.subscribe_incoming_rfqs(listener)
            sx.on_parlay(REQUEST)
            await settle(lambda: heard)

        run(go())
        assert heard == [PARENT]


# -- quoting --------------------------------------------------------------------------------


class TestQuote:
    def test_a_quote_is_a_bid_and_an_ask_order_expiring_with_the_parlay(self):
        now = 1_800_000_000.0

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                routes = mocked(mock)
                sx = adapter()
                sx.clock = lambda: now
                sx.on_parlay(REQUEST)
                result = await sx.send_quote(quote(client_order_id="q-1"))
                body = json.loads(routes["post"].calls[0].request.content)
                await sx.close()
                return result, body

        result, body = run(go())
        bid, ask = body["orders"]
        expiry = int(now) + 60 - PARLAY_EXPIRY_MARGIN_S
        assert body["waitForOutcome"] is True
        assert (bid["isMakerBettingOutcomeOne"], bid["percentageOdds"]) == (True, str(20 * 10 ** 18))
        assert (ask["isMakerBettingOutcomeOne"], ask["percentageOdds"]) == (False, str(70 * 10 ** 18))
        assert bid["marketHash"] == PARENT and ask["marketHash"] == PARENT
        assert bid["timeInForce"] == ask["timeInForce"] == "GTC"
        assert bid["expiry"] == ask["expiry"] == expiry
        assert (bid["clientOrderId"], ask["clientOrderId"]) == ("q-1:bid", "q-1:ask")
        # No quantity: each side covers the 50 USDC asked for at the bettor's price.
        assert bid["totalBetSize"] == str(int(D("62.5") * D("0.2") * 10 ** 6))      # bettor pays 0.8 a contract
        assert ask["totalBetSize"] == str(int(D("166.666666") * D("0.7") * 10 ** 6))  # bettor pays 0.3
        assert result.id == quote_id_of(BID_ID, ASK_ID) and order_ids_of(result.id) == (BID_ID, ASK_ID)
        assert (result.bid, result.ask, result.status, result.mine) == (D("0.2"), D("0.3"), QuoteStatus.OPEN, True)
        assert (result.rfq_id, result.market_id, result.expires_at) == (PARENT, COMBO_ID, expiry * 1000)
        assert result.info["time_in_force"] == "GTC" and result.info["refused"] == {}

    def test_a_one_sided_quote_and_its_own_expiry(self):
        now = 1_800_000_000.0

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                routes = mocked(mock)
                sx = adapter()
                sx.clock = lambda: now
                sx.on_parlay(REQUEST)
                result = await sx.send_quote(quote(bid=None, quantity=D("10"), post_only=True,
                                                   expires_at=int(now + 20) * 1000))
                return result, json.loads(routes["post"].calls[0].request.content)

        result, body = run(go())
        [ask] = body["orders"]
        assert ask["expiry"] == int(now) + 20 and ask["postOnly"] is True and ask["totalBetSize"] == "7000000"
        assert result.id == f"~{ASK_ID}" and result.bid is None and result.ask_quantity == D("10")

    def test_an_order_past_the_parlay_is_resigned_to_the_end_the_venue_names(self):
        now = 1_800_000_000.0
        exact = int(now) + 41
        answers = iter([
            httpx.Response(400, json={"message": "PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY", "marketHash": PARENT,
                                      "parlayExpiry": exact}),
        ])

        def post(req):
            return next(answers, None) or submitted(req)

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                routes = mocked(mock, post=post)
                sx = adapter()
                sx.clock = lambda: now
                sx.on_parlay(REQUEST)
                result = await sx.send_quote(quote())
                expiries = [json.loads(c.request.content)["orders"][0]["expiry"] for c in routes["post"].calls]
                return result, expiries, sx.parlays[PARENT]

        result, expiries, rfq = run(go())
        assert expiries == [int(now) + 60 - PARLAY_EXPIRY_MARGIN_S, exact]
        assert rfq.expires_at == exact * 1000 and rfq.info["expiry_exact"] is True
        assert result.expires_at == exact * 1000

    def test_an_expired_parlay_is_refused_before_anything_is_sent(self):
        clock = [1_800_000_000.0]

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                routes = mocked(mock)
                sx = adapter(parlay_stream=fake_stream()[0])
                sx.clock = lambda: clock[0]
                sx.on_parlay(REQUEST)
                clock[0] += 61
                with pytest.raises(OrderRejected) as refused:
                    await sx.send_quote(quote())
                incoming = await sx.fetch_incoming_rfqs()
                closed = await sx.fetch_rfq(PARENT)
                await sx.close()
                return refused.value, incoming, closed, routes["post"].call_count

        refused, incoming, closed, posts = run(go())
        assert refused.reason == "parlay_expired" and posts == 0
        assert list(incoming) == []
        assert (closed.status, closed.closed_reason) == (RfqStatus.CLOSED, RfqCloseReason.EXPIRED)

    def test_a_quote_on_a_request_never_heard_is_not_found(self):
        async def go():
            with pytest.raises(RfqNotFound, match="parlay_markets:global"):
                await adapter().send_quote(quote())

        run(go())

    @pytest.mark.parametrize("body,error,reason", [
        ({"message": "INVALID_MARKET", "marketHash": PARENT}, OrderRejected, "parlay_expired"),
        ({"message": "PARLAY_LEG_INACTIVE", "parentMarketHash": PARENT, "legMarketHash": "0x22",
          "legStatus": "INACTIVE"}, OrderRejected, "parlay_leg_inactive"),
        ({"message": "TOTAL_BET_SIZE_TOO_LOW", "marketHash": PARENT, "minimum": "1"}, InvalidOrder, None),
        ({"message": "PARLAY_ORDER_EXPIRY_BEYOND_PARLAY_EXPIRY"}, InvalidOrder, None),
    ])
    def test_refusals_are_typed(self, body, error, reason):
        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock, post=lambda req: httpx.Response(400, json=body))
                sx = adapter()
                sx.on_parlay(REQUEST)
                with pytest.raises(error) as raised:
                    await sx.send_quote(quote())
                return raised.value

        raised = run(go())
        if reason:
            assert raised.reason == reason

    def test_one_side_refused_leaves_a_one_sided_quote(self):
        def post(req):
            return httpx.Response(200, json={"status": "success", "data": {"orders": [
                {"orderId": BID_ID, "status": "SUBMITTED", "outcome": {"state": "RESTED"}},
                {"orderId": ASK_ID, "status": "FAILED"}]}})

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock, post=post)
                sx = adapter()
                sx.on_parlay(REQUEST)
                return await sx.send_quote(quote())

        result = run(go())
        assert result.id == f"{BID_ID}~" and result.ask is None and "ask" in result.info["refused"]

    def test_there_is_no_last_look_and_no_combo_create(self):
        async def go():
            sx = adapter()
            sx.on_parlay(REQUEST)
            with pytest.raises(NotSupported, match="no last look"):
                await sx.confirm_quote(None)
            with pytest.raises(NotSupported, match="bettor's request"):
                await sx.create_combo([ComboLeg(market_id=LEG_ONE, outcome="yes"),
                                       ComboLeg(market_id=LEG_TWO, outcome="yes")])
            with pytest.raises(NotSupported):
                await sx.create_rfq(COMBO_ID, amount=D("5"))

        run(go())


# -- cancel, replace, read back -------------------------------------------------------------


class TestLifecycle:
    def test_cancel_cancels_both_orders_and_replace_sends_anew(self):
        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                routes = mocked(mock)
                delete = mock.delete("/orders-v3").respond(json={"status": "success", "data": {}})
                mock.get(f"/orders-v3/{BID_ID}").respond(json={"status": "success", "data": order_row(
                    BID_ID, outcome_one=True, status="INACTIVE", reason="CANCELLED")})
                mock.get(f"/orders-v3/{ASK_ID}").respond(json={"status": "success", "data": order_row(
                    ASK_ID, outcome_one=False, status="INACTIVE", reason="CANCELLED")})
                sx = adapter()
                sx.on_parlay(REQUEST)
                first = await sx.send_quote(quote())
                await sx.cancel_quote(first.id)
                cancelled = json.loads(delete.calls[0].request.content)
                again = await sx.replace_quote(first.id, quote(bid=D("0.21")))
                return cancelled, again, routes["post"].call_count, delete.call_count

        cancelled, again, posts, deletes = run(go())
        assert cancelled == {"orders": [{"orderId": BID_ID}, {"orderId": ASK_ID}]}
        assert posts == 2 and deletes == 2 and again.bid == D("0.21")

    def test_cancel_of_a_quote_the_venue_does_not_know(self):
        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock)
                mock.delete("/orders-v3").respond(json={"status": "success", "data": {}})
                mock.get(url__regex=r"/orders-v3/0x.*").respond(404, json={"message": "not found"})
                with pytest.raises(RfqNotFound):
                    await adapter().cancel_quote(quote_id_of(BID_ID, ASK_ID))

        run(go())

    @pytest.mark.parametrize("bid,ask,status", [
        (dict(status="ACTIVE"), dict(status="ACTIVE"), QuoteStatus.OPEN),
        (dict(status="ACTIVE", remaining="5000000"), dict(status="INACTIVE", reason="EXPIRED"), QuoteStatus.OPEN),
        (dict(status="INACTIVE", reason="FILLED", remaining="0"), dict(status="INACTIVE", reason="EXPIRED"),
         QuoteStatus.EXECUTED),
        (dict(status="INACTIVE", reason="EXPIRED"), dict(status="INACTIVE", reason="EXPIRED"), QuoteStatus.EXPIRED),
        (dict(status="INACTIVE", reason="CANCELLED"), dict(status="INACTIVE", reason="EXPIRED"), QuoteStatus.CANCELED),
    ])
    def test_a_quote_reads_back_from_its_orders(self, bid, ask, status):
        b = order_of_row(order_row(BID_ID, outcome_one=True, **bid))
        a = order_of_row(order_row(ASK_ID, outcome_one=False, **ask))
        result = quote_of_orders(b, a)
        assert result.status == status and result.rfq_id == PARENT and result.market_id == COMBO_ID
        assert (result.bid, result.ask) == (D("0.2"), D("0.3"))
        if status == QuoteStatus.EXECUTED:
            assert result.maker_order_id == BID_ID

    def test_fetch_quote_and_my_quotes_find_orders_sent_in_doubt(self):
        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock)
                mock.get(f"/orders-v3/{BID_ID}").respond(json={"status": "success", "data": order_row(
                    BID_ID, outcome_one=True, status="INACTIVE", reason="FILLED", remaining="0")})
                mock.get(f"/orders-v3/{ASK_ID}").respond(404, json={"message": "not found"})
                mock.get("/orders-v3").respond(json={"status": "success", "data": {"orders": [
                    order_row("0x" + "c1" * 32, outcome_one=True, client="rfq-quote:sx_bet:k:bid"),
                    order_row("0x" + "c2" * 32, outcome_one=False, client="rfq-quote:sx_bet:k:ask"),
                ]}})
                sx = adapter()
                one = await sx.fetch_quote(quote_id_of(BID_ID, ASK_ID))
                mine = await sx.fetch_my_quotes(rfq_id=PARENT, status=QuoteStatus.OPEN)
                return one, mine

        one, [found] = run(go())
        assert one.id == quote_id_of(BID_ID, ASK_ID) and one.status == QuoteStatus.EXECUTED and one.ask is None
        assert found.id == quote_id_of("0x" + "c1" * 32, "0x" + "c2" * 32) and found.rfq_id == PARENT


# -- void legs ------------------------------------------------------------------------------


class TestVoidLegs:
    @pytest.mark.parametrize("legs,one,two", [
        # (developer docs, user guide, worst) for the parlay WIN side, then the LOSE side.
        (["won", "won"], ("won", "won", "won"), ("lost", "lost", "lost")),
        (["won", "lost"], ("lost", "lost", "lost"), ("won", "won", "won")),
        (["won", "open"], ("open", "open", "open"), ("open", "open", "open")),
        (["won", "void"], ("refund", "lost", "lost"), ("refund", "lost", "lost")),
        (["lost", "void"], ("refund", "lost", "lost"), ("refund", "lost", "lost")),
        (["open", "void"], ("refund", "lost", "lost"), ("refund", "lost", "lost")),
    ])
    def test_both_readings_and_the_worse_one(self, legs, one, two):
        for outcome_one, expected in ((True, one), (False, two)):
            result = parlay_result(legs, outcome_one=outcome_one)
            assert (result["developer_docs"], result["user_guide"], result["worst"]) == expected, (legs, outcome_one)

    def test_unknown_results_are_refused(self):
        with pytest.raises(ValueError):
            parlay_result(["won", "push?"])


# -- through the engine ---------------------------------------------------------------------


class TestEngine:
    def test_the_quoter_flow_through_engine_rfq(self, tmp_path):
        from synpath.engine import Engine, EngineConfig, RiskConfig

        filled = {"done": False}

        def order(order_id, outcome_one):
            def answer(req):
                if filled["done"]:
                    reason = "FILLED" if outcome_one else "EXPIRED"
                    return httpx.Response(200, json={"status": "success", "data": order_row(
                        order_id, outcome_one=outcome_one, status="INACTIVE", reason=reason,
                        remaining="0" if outcome_one else "10000000")})
                return httpx.Response(200, json={"status": "success", "data": order_row(order_id, outcome_one=outcome_one)})
            return answer

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock)
                mock.get(f"/orders-v3/{BID_ID}").mock(side_effect=order(BID_ID, True))
                mock.get(f"/orders-v3/{ASK_ID}").mock(side_effect=order(ASK_ID, False))
                stream, socket = fake_stream()
                sx = adapter(parlay_stream=stream)
                engine = Engine({"sx_bet": sx}, EngineConfig(
                    journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0, rfq_poll_interval_s=0.01,
                    rfq_incoming_interval_s=0,
                ), risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
                await engine.start()
                assert engine.rfq.pushed == {"sx_bet"}, "subscribed to the stream, not polled"
                sub = engine.bus.subscribe("rfq", "quote")
                heard = []
                engine.rfq.on_incoming_rfq(lambda rfq: heard.append(rfq.id))
                await engine.rfq.tick()                          # starts the parlay stream
                await handshake(socket)
                await socket.inbox.put(push(REQUEST))
                await settle(lambda: heard == [PARENT])          # pushed: no tick in between
                assert heard == [PARENT]
                assert f"sx_bet:{PARENT}" in engine.rfq.incoming
                sent = await engine.send_quote(MakerQuote(rfq_id=PARENT, bid=D("0.20"), ask=D("0.30"), book="mm"))
                held = dict(engine.rfq.maker_quotes)
                with pytest.raises(OrderRejected, match="only an accepted quote"):
                    await engine.confirm_quote(sent.id)         # never accepted: no last look on SX
                filled["done"] = True                            # the bettor took the bid
                await engine.rfq.tick()
                out = []
                while not sub.queue.empty():
                    out.append(sub.queue.get_nowait().kind)
                left = dict(engine.rfq.maker_quotes)
                await engine.stop()
                await sx.close()
                return sent, held, out, left

        sent, held, kinds, left = run(go())
        assert sent.id == quote_id_of(BID_ID, ASK_ID) and sent.mine
        assert held[f"sx_bet:{sent.id}"]["market_id"] == COMBO_ID
        assert kinds[:2] == ["rfq.incoming", "quote.sent"] and "quote.executed" in kinds
        assert left == {}

    def test_halt_cancels_a_live_parlay_quote(self, tmp_path):
        from synpath.engine import Engine, EngineConfig, RiskConfig

        async def go():
            with respx.mock(base_url=API, assert_all_called=False) as mock:
                mocked(mock)
                delete = mock.delete("/orders-v3").respond(json={"status": "success", "data": {}})
                mock.delete("/orders-v3/all").respond(json={"status": "success", "data": {}})
                mock.get("/orders-v3").respond(json={"status": "success", "data": {"orders": []}})
                for order_id, one in ((BID_ID, True), (ASK_ID, False)):
                    mock.get(f"/orders-v3/{order_id}").respond(json={"status": "success", "data": order_row(
                        order_id, outcome_one=one, status="INACTIVE", reason="CANCELLED")})
                sx = adapter(parlay_stream=fake_stream()[0])
                sx.on_parlay(REQUEST)
                engine = Engine({"sx_bet": sx}, EngineConfig(
                    journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0, rfq_incoming_interval_s=None,
                ), risk=RiskConfig(price_collar=None, duplicate_window_ms=0, closing_soon_s=None))
                await engine.start()
                sent = await engine.send_quote(MakerQuote(rfq_id=PARENT, bid=D("0.20"), ask=D("0.30")))
                result = await engine.halt("operator")
                left = dict(engine.rfq.maker_quotes)
                await engine.stop()
                await sx.close()
                return sent, result, left, [json.loads(c.request.content) for c in delete.calls]

        sent, result, left, deletes = run(go())
        assert result["rfq"]["quotes"]["sx_bet"] == 1 and left == {}
        assert {"orders": [{"orderId": BID_ID}, {"orderId": ASK_ID}]} in deletes
