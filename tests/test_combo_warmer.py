"""serve's warm combo boards and the combo skip reasons, offline.

* The warmer (`synpath/server/combo_warmer.py`): every combo venue's board
  built at start-up, through the venue's whole list (no page cap), rebuilt on
  its interval, the last good board kept when a rebuild fails, and its state.
* The board routes on it: `GET /combos/board/categories`, one category a page
  at a time on `GET /combos/board?category=&cursor=&limit=&q=&sort=`, the
  whole board as before, and `status: "building"` at once while the first
  build runs.
* The skip reasons of a cross-venue combo, on Yuki's real Polymarket combo of
  2026-10-22: "Will Al Fayha Saudi Club win on 2026-10-22?"
  (`polymarket:5441701`, matched on Polymarket US only) and "Will the U.S.
  invade Iran before 2027?" (`polymarket:665374`, matched nowhere), with
  hosted matching mocked from its real answers and Gamma's real rows for both
  (`samples/polymarket_combo_skip_legs_live.json`, no auth).
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.combo_board import board_of, market_labels, polymarket_markets, short_question
from synpath.engine.paper import PaperRfqVenue
from synpath.engine.rfq_group import hosted_matcher
from synpath.polymarket import normalize_market as gamma_market
from synpath.server.combo_warmer import ComboBoardWarmer
from synpath.server.store import ControlStore
from synpath.server.trading import create_trading_app
from synpath.trading.types import Account, ComboLeg, ComboMarket
from synpath.types import Page

from test_engine_rfq_group import RISK, CollectionVenue, ListedVenue

pytestmark = pytest.mark.anyio

SAMPLE = json.loads((Path(__file__).parent / "samples" / "polymarket_combo_skip_legs_live.json").read_text())
AL_FAYHA, IRAN = "polymarket:5441701", "polymarket:665374"
AL_FAYHA_US = "polymarket_us:atc-spl-haz-fay-2026-10-22-fay"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def no_saved_key(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNPATH_API_KEY", raising=False)
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "no-credentials.json"))


# -- a venue with a long eligible list ----------------------------------------------

def question(n: int, tag: str) -> ComboMarket:
    return ComboMarket(market_id=f"polymarket:{n}", venue="polymarket", title=f"Will thing {n} happen?", tags=[tag],
                       info={"volume": str(n), "outcome_prices": ["0.4", "0.6"]})


class PagedVenue(PaperRfqVenue):
    """A Polymarket-like list of `pages` pages; each build may be held or fail."""

    def __init__(self, pages: int, per_page: int = 10, **kw: Any):
        super().__init__(venue="polymarket", **kw)
        self.has = {**self.has, "combos": True}
        tags = ("nfl", "politics", "crypto")
        self.rows = [question(n, tags[n % 3]) for n in range(pages * per_page)]
        self.per_page = per_page
        self.reads = 0
        self.fail = False
        self.hold: asyncio.Event | None = None
        self.block_at: int | None = None
        """Hold the read of this page (1-based) until `hold` is set."""

    async def fetch_combo_eligible_markets(self, *, event_id=None, tag=None, limit=None, cursor=None) -> Page:
        if self.hold is not None and (self.block_at is None or self.reads + 1 == self.block_at):
            await self.hold.wait()
        if self.fail:
            raise ConnectionError("the catalogue is down")
        self.reads += 1
        start = int(cursor or 0)
        page = self.rows[start:start + self.per_page]
        more = start + self.per_page < len(self.rows)
        return Page(page, next_cursor=str(start + self.per_page) if more else None)


class FakeEngine:
    def __init__(self, adapters):
        self.adapters = adapters


class TestWarmer:
    async def test_builds_the_whole_list_at_start_up_with_no_page_cap(self):
        venue = PagedVenue(pages=12)                       # more than the 5 pages a board read on request takes
        warmer = ComboBoardWarmer(FakeEngine({"polymarket": venue}), pause_s=0)
        assert warmer.venues() == ["polymarket"]
        first = warmer.board("polymarket")                 # starts the build, does not wait for it
        assert first.status == "building" and first.events == []
        await warmer.ensure("polymarket")
        board = warmer.board("polymarket")
        assert board.status == "ready" and board.complete and board.count == 120 and venue.reads == 12
        assert board.built_at and warmer.state("polymarket")["status"] == "ready"

    async def test_refreshes_and_keeps_the_last_good_board_when_a_refresh_fails(self):
        venue = PagedVenue(pages=2)
        warmer = ComboBoardWarmer(FakeEngine({"polymarket": venue}), pause_s=0)
        await warmer.build("polymarket")
        built = warmer.board("polymarket").built_at
        venue.rows = venue.rows[:5]
        await warmer.build("polymarket")
        assert warmer.board("polymarket").count == 5, "a refresh replaces the board"
        venue.fail = True
        await warmer.build("polymarket")
        kept = warmer.board("polymarket")
        assert kept.count == 5 and kept.status == "ready" and "catalogue is down" in kept.error
        assert kept.built_at >= built and not warmer.state("polymarket")["refreshing"]

    async def test_the_loop_builds_at_once_then_on_its_interval_and_never_raises(self):
        venue = PagedVenue(pages=1)
        warmer = ComboBoardWarmer(FakeEngine({"polymarket": venue}), refresh_interval_s=0.01, pause_s=0)
        task = asyncio.create_task(warmer.loop())
        for _ in range(200):
            await asyncio.sleep(0.005)
            if venue.reads >= 3:
                break
        venue.fail = True
        await asyncio.sleep(0.05)
        assert not task.done(), "a failing build never stops serve's loop"
        task.cancel()
        await warmer.close()
        assert venue.reads >= 3 and warmer.board("polymarket").count == 10

    async def test_a_long_first_build_serves_its_busiest_pages_first(self):
        venue = PagedVenue(pages=12)
        venue.hold, venue.block_at = asyncio.Event(), 7
        warmer = ComboBoardWarmer(FakeEngine({"polymarket": venue}), pause_s=0)
        task = warmer.ensure("polymarket")
        for _ in range(200):
            await asyncio.sleep(0.005)
            if warmer.board("polymarket").status == "ready":
                break
        partial = warmer.board("polymarket")
        assert partial.status == "ready" and not partial.complete and partial.count == 50, "after the first 5 pages"
        venue.hold.set()
        await task
        whole = warmer.board("polymarket")
        assert whole.complete and whole.count == 120
        # A refresh never swaps the whole board for a partial one.
        venue.hold, venue.reads = asyncio.Event(), 0
        refresh = warmer.ensure("polymarket")
        for _ in range(50):
            await asyncio.sleep(0.005)
        assert warmer.board("polymarket").complete and warmer.state("polymarket")["refreshing"]
        venue.hold.set()
        await refresh

    async def test_a_venue_without_combo_lists_is_not_warmed(self):
        warmer = ComboBoardWarmer(FakeEngine({"limitless": PaperRfqVenue(venue="limitless")}))
        assert warmer.venues() == [] and not warmer.warms("limitless")


# -- the routes on a warm board ----------------------------------------------------------

@pytest.fixture
async def server(tmp_path: Path):
    from fastapi.testclient import TestClient

    venue = PagedVenue(pages=3)
    venue.account = Account(venue="polymarket", name="desk-a")
    engine = Engine({"polymarket": venue}, EngineConfig(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0,
                                                         rfq_incoming_interval_s=None),
                    risk=RiskConfig(**RISK), accounts={"polymarket": venue.account})
    await engine.start()
    store = await ControlStore(str(tmp_path / "control.db")).open()
    owner, issued = await store.bootstrap("owner")
    app = create_trading_app(engine, store)
    warmer = ComboBoardWarmer(engine, pause_s=0)
    app.state.combo_boards = warmer
    with TestClient(app) as client:
        yield {"client": client, "venue": venue, "warmer": warmer, "headers": {"Authorization": f"Bearer {issued.secret}"}}
    await store.close()
    await engine.stop()


def wait_ready(server) -> None:
    client = server["client"]
    for _ in range(200):
        if client.get("/combos/board/categories?venue=polymarket", headers=server["headers"]).json()["status"] == "ready":
            return
        import time
        time.sleep(0.01)
    raise AssertionError("the board never became ready")


class TestRoutes:
    async def test_building_answers_at_once_then_the_board_fills_in(self, server):
        client, venue = server["client"], server["venue"]
        venue.hold = asyncio.Event()
        categories = client.get("/combos/board/categories?venue=polymarket", headers=server["headers"])
        assert categories.status_code == 200 and categories.json()["status"] == "building"
        whole = client.get("/combos/board?venue=polymarket", headers=server["headers"]).json()
        assert whole["status"] == "building" and whole["events"] == []
        page = client.get("/combos/board?venue=polymarket&category=nfl", headers=server["headers"]).json()
        assert page["status"] == "building" and page["total"] == 0 and page["next_cursor"] is None
        client.portal.call(venue.hold.set)
        wait_ready(server)

    async def test_categories_then_one_category_a_page_at_a_time(self, server):
        client = server["client"]
        wait_ready(server)
        rail = client.get("/combos/board/categories?venue=polymarket", headers=server["headers"]).json()
        assert rail["events"] == 30 and rail["complete"] and rail["built_at"]
        assert {c["key"]: c["count"] for c in rail["categories"]} == {"nfl": 10, "politics": 10, "crypto": 10}
        assert "events" not in rail["categories"][0]
        first = client.get("/combos/board?venue=polymarket&category=politics&limit=4&sort=volume",
                           headers=server["headers"]).json()
        assert first["category"] == "politics" and first["total"] == 10 and len(first["events"]) == 4
        assert first["next_cursor"] == "4" and {e["category"] for e in first["events"]} == {"politics"}
        volumes = [float(e["volume"]) for e in first["events"]]
        assert volumes == sorted(volumes, reverse=True)
        assert len(first["categories"]) == 3, "a page carries the rail's categories"
        seen = [e["id"] for e in first["events"]]
        cursor = first["next_cursor"]
        while cursor:
            page = client.get(f"/combos/board?venue=polymarket&category=politics&limit=4&sort=volume&cursor={cursor}",
                              headers=server["headers"]).json()
            seen += [e["id"] for e in page["events"]]
            cursor = page["next_cursor"]
        assert len(seen) == len(set(seen)) == 10
        found = client.get("/combos/board?venue=polymarket&category=all&q=thing%2012", headers=server["headers"]).json()
        assert [e["id"] for e in found["events"]] == ["polymarket:12"] and found["total"] == 1
        bad = client.get("/combos/board?venue=polymarket&category=nfl&cursor=x", headers=server["headers"])
        assert bad.status_code == 400

    async def test_the_whole_board_call_still_works(self, server):
        wait_ready(server)
        whole = server["client"].get("/combos/board?venue=polymarket", headers=server["headers"]).json()
        assert whole["status"] == "ready" and len(whole["events"]) == 30 and whole["next_cursor"] is None
        assert whole["category"] is None and whole["count"] == 30

    async def test_openapi_lists_the_new_routes(self, server):
        paths = server["client"].get("/openapi.json").json()["paths"]
        assert "get" in paths["/combos/board/categories"]
        params = {p["name"] for p in paths["/combos/board"]["get"]["parameters"]}
        assert {"venue", "category", "cursor", "limit", "q", "sort", "pages"} <= params


# -- skip reasons on the two real legs -------------------------------------------------------

class GammaReader:
    """serve's Polymarket read adapter, on Gamma's real rows for both legs."""

    def fetch_markets_by_ids(self, market_ids):
        wanted = {m.partition(":")[2] for m in market_ids}
        return [gamma_market(raw) for raw in SAMPLE["gamma_markets"] if raw["id"] in wanted]

    def fetch_events_by_ids(self, event_ids):
        from synpath.polymarket import normalize_event
        return [normalize_event(e) for e in SAMPLE["gamma_events"] if f"polymarket:{e['id']}" in event_ids]


class Registry:
    def __init__(self, readers):
        self.readers = readers

    def get(self, venue):
        return self.readers[venue]


def catalogue_row(raw: dict) -> ComboMarket:
    tags = ["sports", "games", "soccer", "saudi-professional-league"] if raw.get("sportsMarketType") else ["politics"]
    return ComboMarket(market_id=f"polymarket:{raw['id']}", venue="polymarket", title=raw["question"], tags=tags,
                       position_ids={"yes": f"{raw['id']}-Y", "no": f"{raw['id']}-N"}, info={"volume": raw.get("volume"), "outcome_prices": raw.get("outcomePrices")})


# Hosted matching's answers for Yuki's legs on 2026-10-22: Al Fayha is listed on
# Polymarket US and not on Kalshi; the Iran question is listed nowhere else.
MATCHES = {
    (AL_FAYHA, "polymarket_us"): {"id": AL_FAYHA_US, "venue": "polymarket_us", "side_map": {"yes": "yes", "no": "no"}},
    (AL_FAYHA, "kalshi"): None,
    (IRAN, "polymarket_us"): None,
    (IRAN, "kalshi"): None,
}


def matching(down: bool = False):
    def handler(request: httpx.Request) -> httpx.Response:
        if down:
            return httpx.Response(503, json={"detail": "matching is down"})
        key = (request.url.params["id"], request.url.params.get("venue"))
        if key not in MATCHES:
            return httpx.Response(404, json={"detail": "market not found"})
        return httpx.Response(200, json={"anchor": key[0], "event_id": "evt", "matched": MATCHES[key]})

    return hosted_matcher(client=httpx.Client(transport=httpx.MockTransport(handler)), api_key="spk_test")


def combo_venues() -> dict[str, PaperRfqVenue]:
    rows = [catalogue_row(raw) for raw in SAMPLE["gamma_markets"]]
    poly = ListedVenue(venue="polymarket", eligible=[r.market_id for r in rows])
    poly.eligible = rows
    us = ListedVenue(venue="polymarket_us", eligible=[])           # the US match is not combo-eligible there
    kalshi = CollectionVenue(venue="kalshi", collections=[{"collection_ticker": "KXMVESPORTS", "associated_event_tickers": ["KXNFLGAME-26OCT22NECHI"]}])
    return {"kalshi": kalshi, "polymarket": poly, "polymarket_us": us}


LEGS = [ComboLeg(market_id=AL_FAYHA, outcome="yes"), ComboLeg(market_id=IRAN, outcome="yes")]


async def engine_with(tmp_path, adapters, *, warm: bool = True, down: bool = False) -> Engine:
    engine = Engine(adapters, EngineConfig(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0,
                                           rfq_incoming_interval_s=None),
                    risk=RiskConfig(**RISK), matcher=matching(down))
    await engine.start()
    warmer = ComboBoardWarmer(engine, Registry({"polymarket": GammaReader()}), pause_s=0)
    if warm:
        await warmer.build("polymarket")
    engine.rfq_groups.labeler = warmer.labels_for
    return engine


class CountingGamma(GammaReader):
    def __init__(self):
        self.asked: list[list[str]] = []

    def fetch_markets_by_ids(self, market_ids):
        self.asked.append(list(market_ids))
        return super().fetch_markets_by_ids(market_ids)


class TestGammaCache:
    async def test_a_rebuild_reads_gamma_only_for_new_markets_and_keeps_prices_fresh(self, monkeypatch):
        # Before the sample's games start: a game under way is read again each build (its result can come in).
        import synpath.engine.combo_board as combo_board
        monkeypatch.setattr(combo_board.time, "time", lambda: 1_790_000_000.0)
        rows = [catalogue_row(raw) for raw in SAMPLE["gamma_markets"]]
        venue = ListedVenue(venue="polymarket", eligible=[])
        venue.eligible = rows
        gamma = CountingGamma()
        warmer = ComboBoardWarmer(FakeEngine({"polymarket": venue}), Registry({"polymarket": gamma}), pause_s=0)
        await warmer.build("polymarket")
        assert sum(len(a) for a in gamma.asked) == 4
        first = warmer.board("polymarket")
        assert any(e.kind == "game" for e in first.events), "Gamma's types and the event's teams"
        venue.eligible = rows + [ComboMarket(market_id="polymarket:1", venue="polymarket", title="Will it rain?",
                                             tags=["weather"], info={"outcome_prices": ["0.4", "0.6"]})]
        gamma.asked.clear()
        await warmer.build("polymarket")
        assert gamma.asked == [["polymarket:1"]], "only the new market"
        again = warmer.board("polymarket")
        assert [e.kind for e in again.events if e.kind == "game"] == [e.kind for e in first.events if e.kind == "game"]
        cache = warmer.caches["polymarket"]
        assert all("bestBid" not in m and "outcomePrices" not in m for m in cache.markets.values()), \
            "prices are never kept: each build takes the catalogue's"


class TestSkipReasons:
    def test_the_board_names_al_fayha_by_its_short_name_and_questions_shortened(self):
        rows = [catalogue_row(raw) for raw in SAMPLE["gamma_markets"]]
        board = board_of("polymarket", polymarket_markets(
            rows, {m["id"]: m for m in SAMPLE["gamma_markets"]}, {e["id"]: e for e in SAMPLE["gamma_events"]}))
        labels = market_labels(board)
        assert labels[AL_FAYHA] == "Al Fayha to win" and labels["polymarket:5441699"] == "Al Hazem to win"
        assert labels[IRAN] == "U.S. invades Iran"
        assert short_question("Will Al Fayha Saudi Club win on 2026-10-22?") == "Al Fayha Saudi Club wins"
        assert short_question("Bitcoin above $100k on December 31?") == "Bitcoin above $100k"

    async def test_each_venue_names_every_leg_it_lacks(self, tmp_path):
        engine = await engine_with(tmp_path, combo_venues())
        preview = await engine.preview_rfq_group(legs=LEGS)
        skipped = {s.venue: s for s in preview.skipped}
        assert [v.venue for v in preview.offered] == ["polymarket"]
        kalshi, us = skipped["kalshi"], skipped["polymarket_us"]
        assert kalshi.code == "missing_leg"
        assert kalshi.reason == "no match for “Al Fayha to win”, “U.S. invades Iran”"
        assert [(m.market_id, m.label, m.code) for m in kalshi.missing_legs] == [
            (AL_FAYHA, "Al Fayha to win", "missing_leg"), (IRAN, "U.S. invades Iran", "missing_leg")]
        assert us.code == "missing_leg" and us.reason == "no match for “U.S. invades Iran”"
        assert [m.market_id for m in us.missing_legs] == [IRAN] and us.leg == IRAN
        await engine.stop()

    async def test_without_a_warm_board_the_catalogue_question_is_shortened(self, tmp_path):
        engine = await engine_with(tmp_path, combo_venues(), warm=False)
        preview = await engine.preview_rfq_group(legs=LEGS)
        kalshi = next(s for s in preview.skipped if s.venue == "kalshi")
        assert kalshi.reason == "no match for “Al Fayha Saudi Club wins”, “U.S. invades Iran”"
        await engine.stop()

    async def test_a_matched_leg_the_venue_will_not_combo_is_not_a_missing_match(self, tmp_path):
        global MATCHES
        saved = dict(MATCHES)
        MATCHES[(IRAN, "polymarket_us")] = {"id": "polymarket_us:iran", "venue": "polymarket_us",
                                            "side_map": {"yes": "yes", "no": "no"}}
        try:
            engine = await engine_with(tmp_path, combo_venues())
            us = next(s for s in (await engine.preview_rfq_group(legs=LEGS)).skipped if s.venue == "polymarket_us")
        finally:
            MATCHES.clear()
            MATCHES.update(saved)
        assert us.code == "ineligible_leg"
        assert us.reason == "not combo-eligible on Polymarket US: “Al Fayha to win”, “U.S. invades Iran”"
        assert [m.mapped_id for m in us.missing_legs] == [AL_FAYHA_US, "polymarket_us:iran"]
        await engine.stop()

    async def test_outside_kalshis_collections_and_matching_unavailable_are_their_own_reasons(self, tmp_path):
        global MATCHES
        saved = dict(MATCHES)
        MATCHES[(AL_FAYHA, "kalshi")] = {"id": "kalshi:KXSPL-26OCT22HAZFAY-FAY", "venue": "kalshi",
                                         "side_map": {"yes": "yes", "no": "no"}}
        MATCHES[(IRAN, "kalshi")] = {"id": "kalshi:KXUSAIRAN-27-YES", "venue": "kalshi",
                                     "side_map": {"yes": "yes", "no": "no"}}
        try:
            engine = await engine_with(tmp_path, combo_venues())
            kalshi = next(s for s in (await engine.preview_rfq_group(legs=LEGS)).skipped if s.venue == "kalshi")
        finally:
            MATCHES.clear()
            MATCHES.update(saved)
        assert kalshi.code == "outside_collections"
        assert kalshi.reason.startswith("Kalshi combos are sports and cross-category only: “Al Fayha to win”")
        await engine.stop()
        (tmp_path / "down").mkdir()
        down = await engine_with(tmp_path / "down", combo_venues(), down=True)
        lost = {s.venue: s for s in (await down.preview_rfq_group(legs=LEGS)).skipped}
        assert lost["kalshi"].code == "matching_error"
        assert lost["kalshi"].reason == "matching unavailable for “Al Fayha to win”, “U.S. invades Iran”"
        await down.stop()


class TestServe:
    async def test_serve_warms_every_combo_venue_unless_told_not_to(self, tmp_path):
        from synpath.server import serve

        for warm, home in ((True, "on"), (False, "off")):
            venue = PagedVenue(pages=1)
            venue.account = Account(venue="polymarket", name="paper")
            (tmp_path / home).mkdir()
            stack = await serve.build(
                config={"risk": RISK, "combo_board": {"warm": warm, "refresh_interval_s": 5, "pause_s": 0}},
                journal=str(tmp_path / home / "serve.db"), control=str(tmp_path / home / "control.db"),
                adapters={"polymarket": venue}, streams=False, home_dir=str(tmp_path / home / "home"))
            loops = serve.background(stack)
            try:
                if warm:
                    assert stack.combo_boards.refresh_interval_s == 5 and "combo_boards" in loops
                    assert stack.engine.rfq_groups.labeler == stack.combo_boards.labels_for
                else:
                    assert stack.combo_boards is None and "combo_boards" not in loops
            finally:
                for coro in loops.values():
                    coro.close()
                await serve.shutdown(stack)
