"""The Azuro feed stream: subscriptions, quotes, one-level books and status
changes, through a fake socket. The frames follow the feed's documented
`ConditionUpdated` shape and the venue's SDK (`@azuro-org/sdk` 8.2.0), which
read `id`, `state` and each outcome's `outcomeId` and `currentOdds`; the feed's
socket refuses this test machine's region, so none was recorded."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

from synpath.types import OrderBook, OrderLevel
from synpath.ws.azuro import AzuroMarketStream
from synpath.ws.base import BookEvent, MarketStatusEvent, QuoteEvent, StreamStatusEvent

CORNERS = "600610060000000000943019940000000000001802657415"
THREE_WAY = "600610060000000000943019940000000000001406934671"


def update(condition_id, state="Active", **odds):
    return json.dumps({"event": "ConditionUpdated", "data": {
        "id": condition_id, "conditionId": condition_id, "state": state, "gameId": "1006000000000094301994",
        "outcomes": [{"outcomeId": int(k[1:]), "currentOdds": v, "state": "Active"} for k, v in odds.items()],
    }})


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


class FakeSocket:
    def __init__(self):
        self.sent: list[dict] = []

    async def send(self, text):
        self.sent.append(json.loads(text))


class Catalog:
    def __init__(self):
        self.read: list[str] = []

    def fetch_order_book(self, native):
        self.read.append(native)
        return OrderBook(market_id=f"azuro:{native}", venue="azuro", bids=[OrderLevel(price=0.39393939, size=16.5)],
                         asks=[OrderLevel(price=0.49019608, size=150.0)], info={"limits": {}})


def run(stream, coroutine=None, *frames):
    async def go():
        stream._ws = FakeSocket()
        if coroutine is not None:
            await coroutine(stream)
        for raw in frames:
            stream._dispatch(raw)
            await asyncio.sleep(0)
        for _ in range(3):
            await asyncio.sleep(0)
        return stream._ws.sent

    return asyncio.run(go())


class TestSubscriptions:
    def test_it_subscribes_each_condition_once_on_the_chains_environment(self):
        stream = AzuroMarketStream(chain="base")
        assert stream.url == "wss://streams.onchainfeed.org/v1/streams/feed"

        async def go(s):
            await s.watch_ticker([f"azuro:{CORNERS}", f"azuro:{THREE_WAY}-1", f"azuro:{THREE_WAY}-2"])
            await s.watch_market_status([f"azuro:{CORNERS}"])

        sent = run(stream, go)
        assert sent == [{"event": "SubscribeConditions",
                         "data": {"conditionIds": sorted([CORNERS, THREE_WAY]), "environment": "BaseWETH"}}]

    def test_a_testnet_uses_the_dev_feed(self):
        assert AzuroMarketStream(chain="polygon_amoy").url.startswith("wss://dev-streams.onchainfeed.org/")

    def test_a_reconnect_subscribes_everything_again(self):
        stream = AzuroMarketStream()

        async def go(s):
            await s.watch_ticker([f"azuro:{CORNERS}"])
            s.on_disconnect()
            await s.on_connect()

        sent = run(stream, go)
        assert len(sent) == 2 and sent[1]["data"]["conditionIds"] == [CORNERS]

    def test_unwatch_unsubscribes_a_condition_no_market_still_needs(self):
        stream = AzuroMarketStream()

        async def go(s):
            await s.watch_ticker([f"azuro:{THREE_WAY}-1", f"azuro:{THREE_WAY}-2"])
            await s.unwatch([f"azuro:{THREE_WAY}-1"])
            await s.unwatch([f"azuro:{THREE_WAY}-2"])

        sent = run(stream, go)
        assert [m["event"] for m in sent] == ["SubscribeConditions", "UnsubscribeConditions"]
        assert sent[1]["data"] == {"conditionIds": [THREE_WAY], "environment": "PolygonUSDT"}


class TestUpdates:
    def test_an_update_is_the_pools_quote_on_yes(self):
        stream = AzuroMarketStream()
        run(stream, lambda s: s.watch_ticker([f"azuro:{CORNERS}"]), update(CORNERS, o1="2.04", o2="1.65"))
        [quote] = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        assert quote.market_id == f"azuro:{CORNERS}" and quote.ask == Decimal("0.49019608")
        assert quote.bid == Decimal(str(round(1 - 1 / 1.65, 8)))

    def test_an_update_naming_one_outcome_keeps_the_others_odds(self):
        stream = AzuroMarketStream()
        run(stream, lambda s: s.watch_ticker([f"azuro:{CORNERS}"]),
            update(CORNERS, o1="2.04", o2="1.65"), update(CORNERS, o1="2.5"))
        quotes = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        assert quotes[-1].ask == Decimal("0.4") and quotes[-1].bid == quotes[0].bid

    def test_one_outcome_of_a_larger_condition_has_only_an_ask(self):
        stream = AzuroMarketStream()
        run(stream, lambda s: s.watch_ticker([f"azuro:{THREE_WAY}-1"]), update(THREE_WAY, o1="4.95", o2="1.32", o3="6.83"))
        [quote] = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        assert quote.ask == Decimal(str(round(1 / 4.95, 8))) and quote.bid is None

    def test_other_conditions_are_ignored(self):
        stream = AzuroMarketStream()
        run(stream, lambda s: s.watch_ticker([f"azuro:{CORNERS}"]), update(THREE_WAY, o1="2"))
        assert not [e for e in drain(stream) if isinstance(e, QuoteEvent)]

    def test_a_stop_and_a_resolution_are_status_changes(self):
        stream = AzuroMarketStream()
        won = json.dumps({"event": "ConditionUpdated", "data": {"id": CORNERS, "state": "Resolved", "wonOutcomeIds": ["2"],
                                                                 "outcomes": []}})
        run(stream, lambda s: s.watch_market_status([f"azuro:{CORNERS}"]),
            update(CORNERS, o1="2.04", o2="1.65"), update(CORNERS, state="Stopped"), won)
        states = [(e.state, e.result) for e in drain(stream) if isinstance(e, MarketStatusEvent)]
        assert states == [("open", None), ("paused", None), ("settled", "no")]

    def test_a_stopped_condition_quotes_nothing(self):
        stream = AzuroMarketStream()
        run(stream, lambda s: s.watch_ticker([f"azuro:{CORNERS}"]), update(CORNERS, state="Stopped", o1="2.04", o2="1.65"))
        [quote] = [e for e in drain(stream) if isinstance(e, QuoteEvent)]
        assert quote.bid is None and quote.ask is None

    def test_garbage_is_ignored(self):
        stream = AzuroMarketStream()
        run(stream, None, "not json", json.dumps({"event": "Other"}), json.dumps([1, 2]))
        assert not [e for e in drain(stream) if not isinstance(e, StreamStatusEvent)]


class TestBooks:
    def test_a_book_is_seeded_with_the_pools_limits_then_moved_by_updates(self):
        catalog = Catalog()
        stream = AzuroMarketStream(catalog=catalog)
        run(stream, lambda s: s.watch_order_book([f"azuro:{CORNERS}"]), update(CORNERS, o1="2.5", o2="1.65"))
        books = [e for e in drain(stream) if isinstance(e, BookEvent)]
        assert catalog.read == [CORNERS] and books[0].info["seeded"]
        assert books[0].best_ask == Decimal("0.49019608") and books[0].asks[0].size == Decimal("150.0")
        assert books[-1].best_ask == Decimal("0.4") and books[-1].asks[0].size == Decimal("150.0")
        local = stream.book(f"azuro:{CORNERS}")
        assert local.ready and local.best_bid == books[-1].best_bid
        assert stream.book(f"azuro:{CORNERS}", "no").best_ask == 1 - local.best_bid

    def test_a_disconnect_invalidates_the_book_and_a_reconnect_reads_it_again(self):
        catalog = Catalog()
        stream = AzuroMarketStream(catalog=catalog)

        async def go(s):
            await s.watch_order_book([f"azuro:{CORNERS}"])
            s.on_disconnect()
            assert not s.book(f"azuro:{CORNERS}").ready
            await s.on_connect()
            for _ in range(5):
                await asyncio.sleep(0.01)

        run(stream, go)
        events = drain(stream)
        assert catalog.read == [CORNERS, CORNERS] and stream.book(f"azuro:{CORNERS}").ready
        assert any(isinstance(e, StreamStatusEvent) and e.state == "resynced" for e in events)

    def test_limits_that_cannot_be_read_say_so(self):
        class Refusing:
            def fetch_order_book(self, native):
                from synpath import ExchangeNotAvailable
                raise ExchangeNotAvailable("azuro: unavailable (503)")

        stream = AzuroMarketStream(catalog=Refusing())
        run(stream, lambda s: s.watch_order_book([f"azuro:{CORNERS}"]), update(CORNERS, o1="2.5", o2="1.65"))
        events = drain(stream)
        assert any(isinstance(e, StreamStatusEvent) and e.state == "error" for e in events)
        assert not [e for e in events if isinstance(e, BookEvent)] and not stream.book(f"azuro:{CORNERS}").ready

    def test_the_engine_gets_the_stream(self):
        from synpath.engine.feeds import default_streams
        from synpath.trading.credentials import AzuroCredentials

        creds = AzuroCredentials(private_key="0x" + "11" * 32, chain="base")
        streams = default_streams({"azuro": object()}, {"azuro": creds})
        assert isinstance(streams["azuro"].market, AzuroMarketStream) and streams["azuro"].private is None
        assert streams["azuro"].market.chain.name == "base"
