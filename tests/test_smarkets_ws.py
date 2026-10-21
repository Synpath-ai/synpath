"""The polled Smarkets market stream: what a poll reads, and the events the
difference between two polls becomes. Every request goes to an
`httpx.MockTransport` answering from the recorded samples."""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from synpath.ws.base import BookEvent, MarketStatusEvent, QuoteEvent, StreamStatusEvent
from synpath.ws.smarkets import SmarketsMarketStream, state_of, yes_levels

from test_smarkets import FULHAM, HALTED, QUOTES, SETTLED, Api, session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    for name in ("SMARKETS_USERNAME", "SMARKETS_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def stream(api: Api | None = None, **kwargs) -> tuple[SmarketsMarketStream, Api]:
    api = api or Api()
    client = httpx.AsyncClient(transport=httpx.MockTransport(api.handler))
    return SmarketsMarketStream(client=client, limiter=None, quotes_limiter=None, interval=0.01, **kwargs), api


def moved(quotes: dict, contract: str, **book) -> dict:
    return {**quotes, contract: {**quotes[contract], **book}}


def books(events):
    return [e for e in events if isinstance(e, BookEvent)]


class TestRecords:
    def test_levels_read_on_yes(self):
        assert yes_levels(QUOTES["456060183"]["bids"])[0] == (Decimal("0.5"), Decimal("11.6881"))
        assert yes_levels([{"price": 0, "quantity": 5}, {"price": 5000, "quantity": 0}]) == []

    @pytest.mark.parametrize("market,contract,expected", [
        ("open", "open", ("open", None)), ("halted", "halted", ("paused", None)), ("new", "new", ("created", None)),
        ("settled", "winner", ("settled", "yes")), ("settled", "loser", ("settled", "no")),
        ("voided", "open", ("settled", "void")), ("live", "reduced", ("settled", "void")),
    ])
    def test_state_of(self, market, contract, expected):
        assert state_of(market, contract) == expected


class TestPolling:
    async def test_nothing_watched_reads_nothing(self):
        s, api = stream()
        await s._open()
        assert (await s.poll())["op"] == "idle" and api.requests == []

    async def test_a_poll_reads_the_quotes_of_every_market_watched(self):
        s, api = stream()
        await s._open()
        await s.watch_order_book([FULHAM])
        message = await s.poll()
        assert api.paths() == ["/v3/markets/164973231/quotes/"]
        assert message["quotes"]["456060183"] == QUOTES["456060183"]
        assert api.requests[0].headers["User-Agent"].startswith("synpath/")
        assert "Authorization" not in api.requests[0].headers

    async def test_a_login_rides_on_the_poll(self):
        api = Api()
        s, _ = stream(api, session=session(api))
        await s._open()
        await s.watch_order_book([FULHAM])
        await s.poll()
        assert api.requests[-1].headers["Authorization"] == "Session-Token TOKEN-1" and s.delayed is False

    async def test_a_refusal_drops_the_connection(self):
        s, _ = stream(Api({r"/v3/markets/[\d,]+/quotes/": httpx.Response(429, json={"error_type": "RATE_LIMIT_EXCEEDED"})}))
        await s._open()
        await s.watch_order_book([FULHAM])
        with pytest.raises(ConnectionError, match="429"):
            await s.poll()


class TestBooks:
    def message(self, quotes, **extra):
        return {"op": "poll", "at": 1, "markets": ["164973231"], "quotes": quotes, **extra}

    async def test_a_snapshot_then_only_what_changed(self):
        s, _ = stream()
        await s.watch_order_book([FULHAM])
        first = books(s.handle(self.message(QUOTES)))
        assert [e.kind for e in first] == ["snapshot"]
        assert first[0].best_bid == Decimal("0.5") and first[0].best_ask == Decimal("0.5155")
        assert first[0].info["delayed"] is True
        assert s.handle(self.message(QUOTES)) == []
        changed = moved(QUOTES, "456060183", offers=[{"price": 5128, "quantity": 20000}])
        delta = books(s.handle(self.message(changed)))
        assert [e.kind for e in delta] == ["delta"]
        assert {(level.price, level.size) for level in delta[0].asks} == {
            (Decimal("0.5128"), Decimal("2")), (Decimal("0.5155"), Decimal("0")),
        }
        assert s.book(FULHAM).best_ask == Decimal("0.5128")
        assert s.book(FULHAM, "no").best_bid == Decimal("1") - Decimal("0.5128")

    async def test_a_whole_market_keeps_every_contract(self):
        s, api = stream()
        await s._open()
        await s.watch_order_book(["smarkets:164973231"])
        events = s.handle(await s.poll())
        assert "/v3/markets/164973231/contracts/" in api.paths()
        assert {e.market_id for e in books(events)} == {
            "smarkets:164973231-456060183", "smarkets:164973231-456060184", "smarkets:164973231-456060185",
        }

    async def test_a_ticker_fires_when_the_top_moves(self):
        s, _ = stream()
        await s.watch_ticker([FULHAM])
        first = s.handle(self.message(QUOTES))
        assert len(first) == 1 and isinstance(first[0], QuoteEvent) and first[0].bid == Decimal("0.5")
        assert s.handle(self.message(QUOTES)) == []
        deeper = moved(QUOTES, "456060183", bids=QUOTES["456060183"]["bids"][:1])
        assert s.handle(self.message(deeper)) == []          # only a lower level went
        assert len(s.handle(self.message(moved(QUOTES, "456060183", bids=[])))) == 1

    async def test_after_a_drop_the_next_poll_is_a_snapshot_and_resynced(self):
        s, _ = stream()
        await s.watch_order_book([FULHAM])
        s.handle(self.message(QUOTES))
        s.on_disconnect()
        assert not s.book(FULHAM).ready
        assert [e.kind for e in books(s.handle(self.message(QUOTES)))] == ["snapshot"]
        status = s._queue.get_nowait()
        assert isinstance(status, StreamStatusEvent) and status.state == "resynced"

    async def test_unwatch(self):
        s, _ = stream()
        await s.watch_order_book([FULHAM])
        await s.unwatch([FULHAM])
        assert s.wanted == set() and s.book(FULHAM) is None


class TestStatus:
    async def test_contract_states_are_read_every_few_polls_and_passed_on_change(self):
        api = Api()
        s, _ = stream(api, status_every=3)
        await s._open()
        market = HALTED["markets"][0]["id"]
        await s.watch_market_status([f"smarkets:{market}"])
        first = s.handle(await s.poll())
        statuses = [e for e in first if isinstance(e, MarketStatusEvent)]
        assert {e.state for e in statuses} == {"paused"} and len(statuses) == 2
        assert f"/v3/markets/{market}/" in api.paths()
        reads = len(api.requests)
        assert s.handle(await s.poll()) == [] and len(api.requests) == reads + 1   # quotes only
        s.handle(await s.poll())
        assert s.handle(await s.poll()) == []                                      # read again, unchanged

    async def test_settled_contracts_carry_their_result(self):
        s, _ = stream()
        await s._open()
        market = SETTLED["markets"][0]["id"]
        await s.watch_market_status([f"smarkets:{market}"])
        events = [e for e in s.handle(await s.poll()) if isinstance(e, MarketStatusEvent)]
        assert sorted(e.result for e in events) == ["no", "no", "yes"]
        assert all(e.state == "settled" for e in events)


class TestLoop:
    async def test_the_stream_runs_through_the_base_loop(self):
        s, _ = stream()
        await s.watch_order_book([FULHAM])
        seen = []

        async def read():
            async for event in s:
                seen.append(event)
                if any(isinstance(e, BookEvent) for e in seen):
                    return

        s.start()
        await asyncio.wait_for(read(), timeout=5)
        await s.close()
        assert isinstance(seen[0], StreamStatusEvent) and seen[0].state == "connected"
        assert any(isinstance(e, BookEvent) and e.market_id == FULHAM for e in seen)

    def test_capabilities_and_feeds(self):
        from synpath.engine.feeds import default_streams

        assert SmarketsMarketStream.has["watch_order_book"] == "partial"
        assert SmarketsMarketStream.has["watch_trades"] is False
        streams = default_streams({"smarkets": object()}, {})
        assert isinstance(streams["smarkets"].market, SmarketsMarketStream) and streams["smarkets"].private is None

    def test_the_interval_must_be_positive(self):
        with pytest.raises(ValueError):
            SmarketsMarketStream(interval=0)


def test_messages_are_json():
    json.dumps({"op": "poll", "quotes": QUOTES})
