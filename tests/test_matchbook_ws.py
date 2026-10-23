"""The polled Matchbook market stream: what a poll reads, and the events the
difference between two polls becomes. Every request goes to an
`httpx.MockTransport` answering from the recorded samples."""
from __future__ import annotations

import asyncio
import copy
import json
from decimal import Decimal

import httpx
import pytest

from synpath.ws.base import BookEvent, MarketStatusEvent, QuoteEvent, StreamStatusEvent
from synpath.ws.matchbook import MatchbookMarketStream, state_of, yes_levels

from test_matchbook import ARSENAL, ARSENAL_EVERTON, ARSENAL_ID, EVENTS, MATCH_ODDS, MATCH_ODDS_ID, RACE, WIN, Api

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch, tmp_path):
    for name in ("MATCHBOOK_USERNAME", "MATCHBOOK_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def stream(api: Api | None = None, **kwargs) -> tuple[MatchbookMarketStream, Api]:
    api = api or Api()
    client = httpx.AsyncClient(transport=httpx.MockTransport(api.handler))
    return MatchbookMarketStream(client=client, limiter=None, interval=0.01, **kwargs), api


def message(events: list[dict]) -> dict:
    return {"op": "poll", "at": 1, "events": events, "polled": [str(e["id"]) for e in events]}


def with_prices(event: dict, market_id, runner_id, prices: list[dict], **runner_fields) -> dict:
    """A copy of an event with one runner's prices (and fields) changed."""
    out = copy.deepcopy(event)
    for market in out["markets"]:
        if market["id"] == market_id:
            for runner in market["runners"]:
                if runner["id"] == runner_id:
                    runner["prices"] = prices
                    runner.update(runner_fields)
    return out


def books(events):
    return [e for e in events if isinstance(e, BookEvent)]


class TestRecords:
    def test_levels_read_on_yes(self):
        lay = yes_levels(ARSENAL["prices"], "lay")
        assert lay[0] == (Decimal("0.6666666667"), Decimal("1388.11838") * Decimal("1.5"))
        assert yes_levels([{"side": "back", "odds": 1, "available-amount": 5},
                           {"side": "back", "odds": 2, "available-amount": 0}], "back") == []

    @pytest.mark.parametrize("event,market,runner,expected", [
        ("open", "open", {"status": "open"}, ("open", None)),
        ("open", "suspended", {"status": "open"}, ("paused", None)),
        ("open", "closed", {"status": "closed"}, ("closed", None)),
        ("open", "open", {"status": "paid", "withdrawn": True}, ("settled", "void")),
        ("graded", "closed", {"status": "closed"}, ("settled", None)),
    ])
    def test_state_of(self, event, market, runner, expected):
        assert state_of(event, market, runner) == expected


class TestPolling:
    async def test_nothing_watched_reads_nothing(self):
        s, api = stream()
        await s._open()
        assert (await s.poll())["op"] == "idle" and api.requests == []

    async def test_a_poll_reads_every_event_watched_in_one_request(self):
        s, api = stream()
        await s._open()
        await s.watch_order_book([ARSENAL_ID, f"matchbook:{RACE['id']}-{WIN['id']}"])
        polled = await s.poll()
        assert api.paths() == ["/edge/rest/events"]
        params = api.requests[0].url.params
        assert set(params["ids"].split(",")) == {str(ARSENAL_EVERTON["id"]), str(RACE["id"])}
        assert params["include-prices"] == "true" and params["odds-type"] == "DECIMAL"
        # Only the markets watched ride in the message.
        arsenal = next(e for e in polled["events"] if e["id"] == ARSENAL_EVERTON["id"])
        assert [m["id"] for m in arsenal["markets"]] == [MATCH_ODDS["id"]]
        assert api.requests[0].headers["User-Agent"].startswith("synpath/")
        assert "session-token" not in api.requests[0].headers

    async def test_a_refusal_drops_the_connection(self):
        s, _ = stream(Api({r"/edge/rest/events": httpx.Response(429, json={})}))
        await s._open()
        await s.watch_order_book([ARSENAL_ID])
        with pytest.raises(ConnectionError, match="429"):
            await s.poll()


class TestBooks:
    async def test_a_snapshot_then_only_what_changed(self):
        s, _ = stream()
        await s.watch_order_book([ARSENAL_ID])
        first = books(s.handle(message([ARSENAL_EVERTON])))
        assert [e.kind for e in first] == ["snapshot"]
        assert first[0].best_bid == Decimal("0.6666666667") and first[0].best_ask == Decimal("0.6711409396")
        assert first[0].info["currency"] == "GBP"
        assert s.handle(message([ARSENAL_EVERTON])) == []
        moved = with_prices(ARSENAL_EVERTON, MATCH_ODDS["id"], ARSENAL["id"], [
            {"side": "back", "odds": 1.48, "decimal-odds": 1.48, "available-amount": 10, "currency": "GBP"},
            *[p for p in ARSENAL["prices"] if p["side"] == "lay"],
        ])
        delta = books(s.handle(message([moved])))
        assert [e.kind for e in delta] == ["delta"] and delta[0].bids == ()
        assert s.book(ARSENAL_ID).best_ask == Decimal("0.6756756757")
        assert s.book(ARSENAL_ID, "no").best_bid == Decimal("1") - Decimal("0.6756756757")

    async def test_a_whole_market_keeps_every_runner(self):
        s, _ = stream()
        await s._open()
        await s.watch_order_book([MATCH_ODDS_ID])
        events = s.handle(await s.poll())
        assert {e.market_id for e in books(events)} == {
            f"matchbook:{ARSENAL_EVERTON['id']}-{MATCH_ODDS['id']}-{r['id']}" for r in MATCH_ODDS["runners"]}

    async def test_a_ticker_fires_when_the_top_moves(self):
        s, _ = stream()
        await s.watch_ticker([ARSENAL_ID])
        first = s.handle(message([ARSENAL_EVERTON]))
        assert len(first) == 1 and isinstance(first[0], QuoteEvent) and first[0].bid == Decimal("0.6666666667")
        assert s.handle(message([ARSENAL_EVERTON])) == []
        empty = with_prices(ARSENAL_EVERTON, MATCH_ODDS["id"], ARSENAL["id"], [])
        assert s.handle(message([empty]))[0].bid is None

    async def test_a_runner_no_longer_listed_has_an_empty_book(self):
        s, _ = stream()
        await s.watch_order_book([ARSENAL_ID])
        s.handle(message([ARSENAL_EVERTON]))
        closed = {**ARSENAL_EVERTON, "status": "closed", "markets": []}
        delta = books(s.handle(message([closed])))
        assert delta and s.book(ARSENAL_ID).best_bid is None

    async def test_after_a_drop_the_next_poll_is_a_snapshot_and_resynced(self):
        s, _ = stream()
        await s.watch_order_book([ARSENAL_ID])
        s.handle(message([ARSENAL_EVERTON]))
        s.on_disconnect()
        assert not s.book(ARSENAL_ID).ready
        assert [e.kind for e in books(s.handle(message([ARSENAL_EVERTON])))] == ["snapshot"]
        status = s._queue.get_nowait()
        assert isinstance(status, StreamStatusEvent) and status.state == "resynced"

    async def test_unwatch(self):
        s, _ = stream()
        await s.watch_order_book([ARSENAL_ID])
        await s.unwatch([ARSENAL_ID])
        assert s.wanted == {} and s.book(ARSENAL_ID) is None


class TestStatus:
    async def test_status_changes_are_passed_on_change_only(self):
        s, _ = stream()
        race_market = f"matchbook:{RACE['id']}-{WIN['id']}"
        await s.watch_market_status([race_market])
        first = [e for e in s.handle(message([RACE])) if isinstance(e, MarketStatusEvent)]
        assert {e.state for e in first} == {"open", "settled"}
        withdrawn = next(e for e in first if e.state == "settled")
        assert withdrawn.result == "void" and withdrawn.info["withdrawn"] is True
        assert s.handle(message([RACE])) == []
        suspended = copy.deepcopy(RACE)
        suspended["markets"][0]["status"] = "suspended"
        paused = [e for e in s.handle(message([suspended])) if isinstance(e, MarketStatusEvent)]
        assert {e.state for e in paused} == {"paused"} and len(paused) == len(WIN["runners"]) - 1


class TestLoop:
    async def test_the_stream_runs_through_the_base_loop(self):
        s, _ = stream()
        await s.watch_order_book([ARSENAL_ID])
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
        assert any(isinstance(e, BookEvent) and e.market_id == ARSENAL_ID for e in seen)

    def test_capabilities_and_feeds(self):
        from synpath.engine.feeds import default_streams

        assert MatchbookMarketStream.has["watch_order_book"] == "partial"
        assert MatchbookMarketStream.has["watch_trades"] is False
        streams = default_streams({"matchbook": object()}, {})
        assert isinstance(streams["matchbook"].market, MatchbookMarketStream)
        assert streams["matchbook"].private is None and streams["matchbook"].market.session is None

    def test_the_interval_must_be_positive(self):
        with pytest.raises(ValueError):
            MatchbookMarketStream(interval=0)


def test_messages_are_json():
    json.dumps(message(EVENTS["events"]))
