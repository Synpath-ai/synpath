"""Cross-venue RFQ groups in the engine, offline (docs/rfq-combo.md, "Cross-venue RFQ").

Several `PaperRfqVenue`s under real venue names, each with its own makers
and fee schedule, one journal, and the hosted matching service mocked at
the HTTP layer (`httpx.MockTransport` under the real `match_market`).
Covered: a bucket fanned out with a flipped member; a cross-venue combo
mapped by matching, with one venue skipped for a missing leg and another for
an ineligible one, and Kalshi-style collections; a plain market with
`cross_venue`; ranking after fees, by quantity and by amount; accept
cancelling the others (one left to expire); double-accept refusal, in memory
and in the journal; an accept that fails and is retried; a crash between the
cancels and the accept; restart recovery with re-seeding; and the halt.
"""
from __future__ import annotations

from decimal import Decimal as D
from typing import Any

import httpx
import pytest

from synpath import ids
from synpath.bucket import Bucket, BucketMember
from synpath.engine import Engine, EngineConfig, RiskConfig
from synpath.engine.paper import PaperMaker, PaperRfqVenue, no_fees, quadratic_fee
from synpath.engine.rfq_group import hosted_matcher
from synpath.engine.storage_types import IntentState
from synpath.errors import BadRequest
from synpath.trading.errors import RiskRejected
from synpath.trading.types import AcceptQuote, ComboLeg, ComboMarket, OrderStatus, QuoteStatus, Side
from synpath.types import Page

pytestmark = pytest.mark.anyio

RISK = dict(price_collar=None, duplicate_window_ms=0, closing_soon_s=None)
K_A, K_B = "kalshi:KXA-26-X", "kalshi:KXB-26-Y"
P_A, P_B = "polymarket:111", "polymarket:222"
U_A, U_B = "polymarket_us:aaa", "polymarket_us:bbb"
L_A, L_B = "limitless:la", "limitless:lb"


class Crash(BaseException):
    """The process dies: nothing in the engine catches it."""


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def no_saved_key(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNPATH_API_KEY", raising=False)
    monkeypatch.setenv("SYNPATH_CREDENTIALS_FILE", str(tmp_path / "no-credentials.json"))


# -- venues -------------------------------------------------------------------

class CollectionVenue(PaperRfqVenue):
    """A Kalshi-like paper venue: combos are built in collections."""

    def __init__(self, *, collections: list[dict[str, Any]] = (), **kw: Any):
        super().__init__(**kw)
        self.collections = list(collections)
        self.reseeded: list[list[str]] = []

    async def fetch_combo_collections(self, *, status: str | None = "open", event_id: str | None = None,
                                      series_id: str | None = None, limit: int | None = None,
                                      cursor: str | None = None) -> Page:
        event = ids.split(event_id)[1] if event_id else None
        return Page([r for r in self.collections if event is None or event in r["associated_event_tickers"]])

    def restore_rfqs(self, rfqs, quotes=None) -> None:
        self.reseeded.append(sorted(r.id for r in rfqs))


class ListedVenue(PaperRfqVenue):
    """A Polymarket-like paper venue: a list of combo-eligible markets."""

    def __init__(self, *, eligible: list[str] = (), **kw: Any):
        super().__init__(**kw)
        self.eligible = [ComboMarket(market_id=m, venue=self.venue, position_ids={"yes": f"{m}-Y", "no": f"{m}-N"})
                         for m in eligible]

    async def fetch_combo_eligible_markets(self, *, event_id=None, tag=None, limit=None, cursor=None) -> Page:
        return Page(self.eligible)


def flat_fee(per_contract: str):
    def fee(market_id, price, quantity, liquidity):
        return D(per_contract) * quantity
    return fee


def venues(**over: Any) -> dict[str, PaperRfqVenue]:
    """Three venues, three fee schedules, three makers' prices on YES:
    Kalshi asks 0.43 (quadratic 7% fee), Polymarket 0.455 (free, no
    requester cancel), Polymarket US 0.44 (2c a contract)."""
    kalshi = CollectionVenue(
        venue="kalshi", fees=quadratic_fee(), auto_advance=True,
        makers=over.pop("kalshi_makers", [PaperMaker(name="k1", bid=D("0.40"), ask=D("0.43"))]),
        collections=[{"collection_ticker": "KXMVE-1", "associated_event_tickers": ["KXA-26", "KXB-26"],
                      "size_min": 2, "size_max": 10}])
    poly = ListedVenue(venue="polymarket", fees=no_fees, auto_advance=True, eligible=[P_A, P_B],
                       makers=over.pop("poly_makers", [PaperMaker(name="p1", bid=D("0.42"), ask=D("0.455"))]))
    poly.rfq_requester_cancel = False
    us = ListedVenue(venue="polymarket_us", fees=flat_fee("0.02"), auto_advance=True, eligible=[U_A],
                     makers=over.pop("us_makers", [PaperMaker(name="u1", bid=D("0.39"), ask=D("0.44"))]))
    out = {"kalshi": kalshi, "polymarket": poly, "polymarket_us": us}
    if over.pop("limitless", False):
        out["limitless"] = ListedVenue(venue="limitless", eligible=[L_A], makers=[PaperMaker(name="l1")])
    return out


# -- the matching service, mocked over HTTP -------------------------------------

MATCHES = {
    (K_A, "polymarket"): {"id": P_A, "venue": "polymarket", "side_map": {"yes": "yes", "no": "no"}},
    (K_B, "polymarket"): {"id": P_B, "venue": "polymarket", "side_map": {"yes": "no", "no": "yes"}},
    (K_A, "polymarket_us"): {"id": U_A, "venue": "polymarket_us", "side_map": {"yes": "yes", "no": "no"}},
    (K_B, "polymarket_us"): None,                                   # US lists nothing like KXB: a missing leg
    (K_A, "limitless"): {"id": L_A, "venue": "limitless", "side_map": {"yes": "yes", "no": "no"}},
    (K_B, "limitless"): {"id": L_B, "venue": "limitless", "side_map": {"yes": "yes", "no": "no"}},
}


def matching(seen: list | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        anchor, venue = request.url.params["id"], request.url.params.get("venue")
        if seen is not None:
            seen.append((anchor, venue, request.headers.get("authorization")))
        if (anchor, venue) not in MATCHES:
            return httpx.Response(404, json={"detail": "market not found"})
        return httpx.Response(200, json={"anchor": anchor, "event_id": "evt", "matched": MATCHES[(anchor, venue)]})

    return hosted_matcher(client=httpx.Client(transport=httpx.MockTransport(handler)), api_key="spk_test")


async def started(tmp_path, adapters: dict[str, PaperRfqVenue], *, seen: list | None = None, **risk: Any) -> Engine:
    engine = Engine(adapters, EngineConfig(journal_path=str(tmp_path / "engine.db"), in_doubt_timeout_s=0,
                                           rfq_poll_interval_s=0.01, rfq_incoming_interval_s=None),
                    risk=RiskConfig(**{**RISK, **risk}), matcher=matching(seen))
    await engine.start()
    return engine


async def crashed(engine: Engine, adapters: dict[str, PaperRfqVenue]) -> None:
    for venue in adapters.values():
        venue.quote_listeners.clear()
    await engine.journal.close()


def by_venue(group) -> dict[str, Any]:
    return {m.venue: m for m in group.members}


def quote_on(group, venue: str):
    return next(q for q in group.quotes if q.venue == venue)


def drain(sub) -> list:
    out = []
    while not sub.queue.empty():
        out.append(sub.queue.get_nowait())
    return out


# ---------------------------------------------------------------------------

class TestBucket:
    async def test_fans_out_to_each_member_and_turns_a_flipped_members_quote_over(self, tmp_path):
        adapters = venues(poly_makers=[PaperMaker(name="p1", bid=D("0.60"), ask=D("0.65"))])
        engine = await started(tmp_path, adapters)
        bucket = await engine.save_bucket(Bucket(book="alpha", name="A", members=[
            BucketMember(market_id=K_A), BucketMember(market_id=P_A, flip=True),
            BucketMember(market_id=U_A, enabled=False)]))
        group = await engine.create_rfq_group(bucket.market_id, quantity=D("10"), book="alpha")

        assert group.status == "open"
        members = by_venue(group)
        assert set(members) == {"kalshi", "polymarket"}
        assert members["polymarket"].flip and not members["kalshi"].flip
        assert [(s.venue, s.code) for s in group.skipped] == [("polymarket_us", "member_disabled")]
        flipped = quote_on(group, "polymarket")
        # Buying the bucket's YES is buying the flipped member's NO: its bid 0.60 is the bucket's 0.40.
        assert flipped.raw_price == D("0.60") and flipped.price == D("0.40") and flipped.fee == 0
        assert quote_on(group, "kalshi").price == D("0.43")
        assert group.best().venue == "polymarket"

        done = await engine.accept_group_quote(group.id, flipped.quote_id, venue=flipped.venue, worst_price=D("0.45"), wait=True)
        assert done.quote.status == QuoteStatus.EXECUTED and done.group.status == "executed"
        order = (await engine.journal.order_by_client_id(done.client_order_id))
        assert order.side == Side.SELL and order.market_id == P_A and order.price == D("0.60")
        assert adapters["kalshi"].rfqs[members["kalshi"].rfq_id].status.value == "closed"
        await engine.stop()


class TestCombo:
    async def test_maps_legs_by_matching_and_skips_a_missing_and_an_ineligible_leg(self, tmp_path):
        seen: list = []
        adapters = venues(limitless=True)
        engine = await started(tmp_path, adapters, seen=seen)
        legs = [ComboLeg(market_id=K_A, outcome="yes"), ComboLeg(market_id=K_B, outcome="no")]
        group = await engine.create_rfq_group(legs=legs, quantity=D("10"))

        members = by_venue(group)
        assert set(members) == {"kalshi", "polymarket"}
        kalshi, poly = members["kalshi"], members["polymarket"]
        assert kalshi.collection_id == "KXMVE-1" and adapters["kalshi"].combo_collections == ["KXMVE-1"]
        assert [l.event_id for l in kalshi.legs] == ["KXA-26", "KXB-26"]
        # The match swaps KXB's sides on Polymarket: KXB NO is 222 YES.
        assert [(l.market_id, l.outcome) for l in poly.legs] == [(P_A, "yes"), (P_B, "yes")]
        assert poly.legs[1].info["position_id"] == f"{P_B}-Y"
        assert poly.combo.market_id == poly.instrument_id and poly.instrument_id.startswith("polymarket:combo-")
        skipped = {s.venue: s for s in group.skipped}
        assert skipped["polymarket_us"].code == "missing_leg" and skipped["polymarket_us"].leg == K_B
        assert skipped["limitless"].code == "ineligible_leg" and skipped["limitless"].leg == K_B
        assert skipped["limitless"].reason == f"not combo-eligible on Limitless: “{K_B}”"
        assert "combo-eligible list" in skipped["limitless"].detail
        assert [(m.market_id, m.code, m.mapped_id) for m in skipped["limitless"].missing_legs] == [
            (K_B, "ineligible_leg", L_B)]
        assert all(auth == "Bearer spk_test" for *_, auth in seen)
        assert len(group.quotes) == 2
        await engine.stop()

    async def test_a_kalshi_leg_in_no_collection_is_outside_its_collections(self, tmp_path):
        adapters = venues()
        adapters["kalshi"].collections = [{"collection_ticker": "KXMVE-1", "associated_event_tickers": ["KXA-26"]}]
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(
            legs=[ComboLeg(market_id=K_A, outcome="yes"), ComboLeg(market_id=K_B, outcome="no")], quantity=D("10"),
            venues=["kalshi", "polymarket"])
        skipped = {s.venue: s for s in group.skipped}
        assert skipped["kalshi"].code == "outside_collections" and skipped["kalshi"].leg == K_B
        assert skipped["kalshi"].reason == f"Kalshi combos are sports and cross-category only: “{K_B}” is in none"
        assert "KXB-26" in skipped["kalshi"].detail
        assert skipped["polymarket_us"].code == "excluded"
        assert [m.venue for m in group.members] == ["polymarket"]
        await engine.stop()

    async def test_a_collection_with_size_max_zero_has_no_maximum(self, tmp_path):
        adapters = venues()
        adapters["kalshi"].collections = [{"collection_ticker": "KXMVE-0", "associated_event_tickers": ["KXA-26", "KXB-26"],
                                           "size_min": 2, "size_max": 0}]
        engine = await started(tmp_path, adapters)
        preview = await engine.preview_rfq_group(legs=[ComboLeg(market_id=K_A, outcome="yes"),
                                                       ComboLeg(market_id=K_B, outcome="no")], venues=["kalshi"])
        assert [(v.venue, v.collection_id) for v in preview.offered] == [("kalshi", "KXMVE-0")]
        await engine.stop()

    async def test_no_venue_left_is_refused_with_the_reasons(self, tmp_path):
        engine = await started(tmp_path, venues())
        with pytest.raises(BadRequest) as caught:
            await engine.create_rfq_group(legs=[ComboLeg(market_id=K_A, outcome="yes"),
                                                ComboLeg(market_id=K_B, outcome="no")],
                                          quantity=D("10"), venues=["polymarket_us"])
        assert [s["code"] for s in caught.value.skipped if s["venue"] == "polymarket_us"] == ["missing_leg"]
        assert await engine.journal.intents() == []
        await engine.stop()


class TestPreview:
    async def test_plans_like_a_create_and_creates_nothing(self, tmp_path):
        seen: list = []
        adapters = venues(limitless=True)
        engine = await started(tmp_path, adapters, seen=seen)
        legs = [ComboLeg(market_id=K_A, outcome="yes"), ComboLeg(market_id=K_B, outcome="no")]
        preview = await engine.preview_rfq_group(legs=legs)

        offered = {v.venue: v for v in preview.offered}
        assert set(offered) == {"kalshi", "polymarket"}
        assert offered["kalshi"].collection_id == "KXMVE-1" and offered["kalshi"].via == "combo"
        assert [(l.market_id, l.outcome) for l in offered["polymarket"].legs] == [(P_A, "yes"), (P_B, "yes")]
        assert {s.venue: s.code for s in preview.skipped} == {"polymarket_us": "missing_leg",
                                                              "limitless": "ineligible_leg"}
        # Nothing created, journaled or sent.
        assert all(not a.rfqs and not a.combos for a in adapters.values())
        assert await engine.journal.intents() == [] and engine.rfq_groups.list() == []
        # The group that follows reuses the matching answers.
        asked = len(seen)
        group = await engine.create_rfq_group(legs=legs, amount=D("10"))
        assert len(seen) == asked and {m.venue for m in group.members} == set(offered)
        await engine.stop()

    async def test_a_plain_market_and_one_sided_sizes(self, tmp_path):
        adapters = venues()
        adapters["polymarket"].rfq_one_sided = True
        engine = await started(tmp_path, adapters)
        unsized = await engine.preview_rfq_group(K_A, cross_venue=True)
        assert {v.venue: v.instrument_id for v in unsized.offered} == {"kalshi": K_A, "polymarket": P_A,
                                                                      "polymarket_us": U_A}
        by_quantity = await engine.preview_rfq_group(K_A, cross_venue=True, quantity=D("5"))
        assert {s.venue: s.code for s in by_quantity.skipped} == {"polymarket": "one_sided_size"}
        with pytest.raises(BadRequest):
            await engine.preview_rfq_group()
        await engine.stop()


class TestRanking:
    async def test_by_effective_price_after_each_venues_fee(self, tmp_path):
        engine = await started(tmp_path, venues())
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        assert {m.venue: m.instrument_id for m in group.members} == {"kalshi": K_A, "polymarket": P_A,
                                                                     "polymarket_us": U_A}
        order = [(q.venue, q.raw_price, q.fee, q.effective_price) for q in group.quotes]
        # Raw: Kalshi 0.43 < US 0.44 < Polymarket 0.455. After fees US (2c) falls behind Polymarket (free):
        # Kalshi 0.43 + ceil(0.07 * 10 * 0.43 * 0.57) / 10 = 0.448.
        assert order == [("kalshi", D("0.43"), D("0.18"), D("0.448")),
                         ("polymarket", D("0.455"), D("0"), D("0.455")),
                         ("polymarket_us", D("0.44"), D("0.20"), D("0.46"))]
        assert [q.rank for q in group.quotes] == [1, 2, 3]
        await engine.stop()

    async def test_a_no_buy_ranks_on_the_bid(self, tmp_path):
        engine = await started(tmp_path, venues())
        group = await engine.create_rfq_group(K_A, quantity=D("10"), outcome="no", action="buy", cross_venue=True)
        # NO costs 1 - bid: Polymarket 0.58, Kalshi 0.60 + fee, US 0.61 + 2c.
        assert [(q.venue, q.outcome_price) for q in group.quotes] == [
            ("polymarket", D("0.58")), ("kalshi", D("0.60")), ("polymarket_us", D("0.61"))]
        await engine.stop()

    async def test_by_amount_ranks_on_contracts_after_fees(self, tmp_path):
        engine = await started(tmp_path, venues())
        group = await engine.create_rfq_group(K_A, amount=D("100"), cross_venue=True)
        rows = {q.venue: q for q in group.quotes}
        # US: 227 whole contracts at 0.44 would cost 99.88 + 4.54 fee; after the fee, 216.
        assert rows["polymarket_us"].quantity == D("216") and rows["polymarket_us"].fee == D("4.32")
        assert rows["polymarket"].quantity == D("219.78")
        for row in rows.values():
            assert row.quantity * row.outcome_price + (row.fee or 0) <= D("100")
        assert [q.venue for q in group.quotes] == ["kalshi", "polymarket", "polymarket_us"]
        assert group.quotes[0].quantity > rows["polymarket"].quantity
        await engine.stop()

    async def test_a_one_sided_venue_is_asked_only_the_size_it_takes(self, tmp_path):
        adapters = venues()
        adapters["polymarket"].rfq_one_sided = True        # Polymarket global: a YES buy by amount
        engine = await started(tmp_path, adapters)
        by_quantity = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        assert {s.venue: s.code for s in by_quantity.skipped} == {"polymarket": "one_sided_size"}
        by_amount = await engine.create_rfq_group(K_A, amount=D("50"), cross_venue=True)
        assert "polymarket" in by_venue(by_amount) and not by_amount.skipped
        await engine.stop()

    async def test_quotes_arrive_as_group_events(self, tmp_path):
        engine = await started(tmp_path, venues())
        sub = engine.bus.subscribe("rfq_group")
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        kinds = [e.kind for e in drain(sub)]
        assert kinds[0] == "rfq_group.created"
        assert kinds.count("rfq_group.quote") == 3 and kinds[-1] == "rfq_group.updated"
        assert kinds.index("rfq_group.quote") < kinds.index("rfq_group.updated")
        assert len(group.quotes) == 3
        await engine.stop()


class TestAccept:
    async def test_cancels_the_others_first_and_one_without_a_cancel_is_left_to_expire(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        best = group.best()
        assert best.venue == "kalshi"
        sub = engine.bus.subscribe("rfq_group", "rfq.closed")
        done = await engine.accept_group_quote(group.id, best.quote_id, venue=best.venue, wait=True)
        members = by_venue(done.group)
        assert members["polymarket_us"].status == "canceled"
        assert members["polymarket"].status == "lapsing" and "lapses" in members["polymarket"].detail
        assert members["kalshi"].status == "executed" and done.group.status == "executed"
        kinds = [e.kind for e in drain(sub)]
        assert kinds.index("rfq_group.accepting") < kinds.index("rfq.closed") < kinds.index("rfq_group.accepted")
        assert "rfq_group.executed" in kinds
        assert adapters["polymarket_us"].calls.count("cancel_rfq") == 1
        assert adapters["kalshi"].calls.count("accept_quote") == 1
        claim = await engine.journal.intent(done.claim_key)
        assert claim.state == IntentState.SENT and claim.operation == "rfq_group_accept"
        order = await engine.journal.order_by_client_id(done.client_order_id)
        assert order.status == OrderStatus.CLOSED and order.tags["rfq_group"] == group.id
        await engine.stop()

    async def test_a_second_accept_is_refused_in_memory_and_in_the_journal(self, tmp_path):
        adapters = venues(kalshi_makers=[PaperMaker(name="k1", bid=D("0.40"), ask=D("0.43")),
                                         PaperMaker(name="k2", bid=D("0.39"), ask=D("0.44"))])
        for venue in adapters.values():
            venue.auto_advance = False
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        first, second = [q for q in group.quotes if q.venue == "kalshi"]
        await engine.accept_group_quote(group.id, first.quote_id, venue=first.venue)
        assert group.status == "accepted"
        with pytest.raises(RiskRejected) as caught:
            await engine.accept_group_quote(group.id, second.quote_id, venue=second.venue)
        assert caught.value.rule == "rfq_group_accept_in_flight"
        # Even with the engine's memory wrong, the journal holds the claim.
        group.status = by_venue(group)["kalshi"].status = "open"
        with pytest.raises(RiskRejected):
            await engine.accept_group_quote(group.id, second.quote_id, venue=second.venue)
        # And around the group: a member quote is accepted only through it.
        with pytest.raises(BadRequest, match="through the group"):
            await engine.accept_quote(AcceptQuote(quote_id=second.quote_id, rfq_id=second.rfq_id, outcome="yes",
                                                  action="buy"), venue="kalshi")
        assert adapters["kalshi"].calls.count("accept_quote") == 1
        group.status = by_venue(group)["kalshi"].status = "accepted"
        adapters["kalshi"].advance()
        adapters["kalshi"].advance()
        await engine.rfq.tick()
        assert group.status == "executed"
        with pytest.raises(RiskRejected) as caught:
            await engine.accept_group_quote(group.id, second.quote_id, venue=second.venue)
        assert caught.value.rule == "rfq_group_accepted"
        await engine.stop()

    async def test_an_accept_that_does_not_trade_is_retried_on_another_quote(self, tmp_path):
        adapters = venues(kalshi_makers=[PaperMaker(name="k1", bid=D("0.40"), ask=D("0.43"), confirms=False),
                                         PaperMaker(name="k2", bid=D("0.39"), ask=D("0.44"))])
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        lapsing, other = [q for q in group.quotes if q.venue == "kalshi"]
        done = await engine.accept_group_quote(group.id, lapsing.quote_id, venue=lapsing.venue, wait=True)
        assert done.quote.status == QuoteStatus.EXPIRED
        assert group.status == "open" and group.failures[-1]["stage"] == "accept"
        assert by_venue(group)["polymarket_us"].status == "canceled", "what was cancelled stays cancelled"
        assert [q.quote_id for q in group.quotes if q.rank] == [other.quote_id]
        first_claim = done.claim_key
        done = await engine.accept_group_quote(group.id, other.quote_id, venue=other.venue, wait=True)
        assert done.group.status == "executed" and done.claim_key != first_claim
        assert (await engine.journal.intent(first_claim)).state == IntentState.FAILED
        await engine.stop()

    async def test_a_risk_refusal_leaves_every_venue_standing(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters, max_order_contracts=D("5"))
        group = await engine.create_rfq_group(K_A, quantity=D("5"), cross_venue=True)
        with pytest.raises(RiskRejected) as caught:
            await engine.accept_group_quote(group.id, group.best().quote_id, venue=group.best().venue, worst_price=D("0.40"))
        assert caught.value.rule == "worst_price"
        assert all(m.status == "open" for m in group.members) and group.status == "open"
        assert not [i for i in await engine.journal.intents() if i.operation == "rfq_group_accept"]
        await engine.stop()


class TestRecovery:
    async def test_a_crash_between_the_cancels_and_the_accept_releases_the_claim(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        best = group.best()

        async def dies(*args, **kwargs):
            raise Crash()

        engine.rfq.accept_quote = dies
        with pytest.raises(Crash):
            await engine.accept_group_quote(group.id, best.quote_id, venue=best.venue)
        await crashed(engine, adapters)

        engine = await started(tmp_path, adapters)
        again = await engine.rfq_group(group.id)
        members = by_venue(again)
        assert again.status == "open" and members["kalshi"].status == "open"
        assert members["polymarket_us"].status == "canceled" and members["polymarket"].status == "lapsing"
        assert "accept_quote" not in adapters["kalshi"].calls
        claim = await engine.journal.intent(f"rfq-group-accept:{group.id}:1")
        assert claim.state == IntentState.FAILED and "nothing traded" in claim.detail
        done = await engine.accept_group_quote(group.id, best.quote_id, venue=best.venue, wait=True)
        assert done.group.status == "executed" and done.claim_key.endswith(":2")
        assert adapters["kalshi"].calls.count("accept_quote") == 1
        await engine.stop()

    async def test_restart_restores_the_group_and_reseeds_its_members(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        kalshi_rfq = by_venue(group)["kalshi"].rfq_id
        await crashed(engine, adapters)

        engine = await started(tmp_path, adapters)
        again = await engine.rfq_group(group.id)
        assert again.status == "open" and [q.rank for q in again.quotes] == [1, 2, 3]
        assert kalshi_rfq in adapters["kalshi"].reseeded[-1]
        assert engine.rfq_groups.group_of("kalshi", kalshi_rfq) == group.id
        done = await engine.accept_group_quote(group.id, again.best().quote_id, venue=again.best().venue, wait=True)
        assert done.group.status == "executed"
        await engine.stop()

    async def test_a_member_create_in_doubt_is_adopted_into_its_group(self, tmp_path):
        from synpath.errors import NetworkError

        adapters = venues()
        real = adapters["polymarket_us"].create_rfq

        async def lost(*args, **kwargs):
            await real(*args, **kwargs)           # the venue made it; the answer is lost
            raise NetworkError("polymarket_us: timed out")

        adapters["polymarket_us"].create_rfq = lost
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        assert by_venue(group)["polymarket_us"].status == "creating" and group.status == "open"
        await engine.sweep()
        await engine.rfq.tick()
        assert by_venue(group)["polymarket_us"].status == "open"
        await engine.stop()


class TestHalt:
    async def test_the_halt_cancels_open_groups(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        result = await engine.halt("operator")
        assert result["rfq"]["groups"][group.id]["status"] == "canceled"
        assert group.status == "canceled"
        assert {m.venue: m.status for m in group.members} == {"kalshi": "canceled", "polymarket": "lapsing",
                                                              "polymarket_us": "canceled"}
        with pytest.raises(BadRequest, match="canceled"):
            await engine.accept_group_quote(group.id, group.quotes[0].quote_id, venue=group.quotes[0].venue)
        await engine.stop()

    async def test_an_accept_is_refused_while_halted_before_any_cancel(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        await engine.halt("operator", policy="hold")
        with pytest.raises(RiskRejected):
            await engine.accept_group_quote(group.id, group.best().quote_id, venue=group.best().venue)
        assert all(m.status == "open" for m in group.members)
        await engine.stop()

    async def test_a_group_every_venue_refuses_is_the_refusal(self, tmp_path):
        engine = await started(tmp_path, venues())
        await engine.halt("operator")
        with pytest.raises(RiskRejected):
            await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        (row,) = [g for g in engine.rfq_groups.list()]
        assert row.status == "failed" and {f["stage"] for f in row.failures} == {"create"}
        await engine.stop()


class TestAcceptByVenue:
    async def test_two_venues_with_the_same_quote_id_are_told_apart_by_venue(self, tmp_path):
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        kalshi, us = quote_on(group, "kalshi"), quote_on(group, "polymarket_us")
        assert kalshi.quote_id == us.quote_id, "each paper venue numbers its own quotes: the ids collide"
        with pytest.raises(BadRequest, match="name the quote's venue") as caught:
            await engine.accept_group_quote(group.id, us.quote_id)
        assert caught.value.reason == "venue_required" and "on 3 venues" in str(caught.value)
        assert all(m.status == "open" for m in group.members), "a refused accept leaves every venue standing"
        # Not the best quote (Kalshi's is), but the one named: Polymarket US's.
        done = await engine.accept_group_quote(group.id, us.quote_id, venue="polymarket_us", wait=True)
        assert done.venue == "polymarket_us" and done.group.accepted_venue == "polymarket_us"
        assert adapters["polymarket_us"].calls.count("accept_quote") == 1
        assert adapters["kalshi"].calls.count("accept_quote") == 0
        assert by_venue(done.group)["kalshi"].status == "canceled"
        claim = await engine.journal.intent(done.claim_key)
        assert claim.target_order_id == us.quote_id and claim.venue == "polymarket_us"
        await engine.stop()

    async def test_a_venue_outside_the_group_is_refused(self, tmp_path):
        engine = await started(tmp_path, venues())
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        with pytest.raises(BadRequest, match="no member on limitless") as caught:
            await engine.accept_group_quote(group.id, group.best().quote_id, venue="limitless")
        assert caught.value.reason == "venue_not_in_group"
        await engine.stop()

    async def test_a_one_venue_group_needs_no_venue(self, tmp_path):
        engine = await started(tmp_path, venues())
        group = await engine.create_rfq_group(K_A, quantity=D("10"))
        assert group.venues() == {"kalshi"}
        done = await engine.accept_group_quote(group.id, group.best().quote_id, wait=True)
        assert done.venue == "kalshi" and done.group.status == "executed"
        await engine.stop()


class TestExpiry:
    async def test_a_quote_carries_its_venues_expiry_and_none_is_invented(self, tmp_path):
        adapters = venues(poly_makers=[PaperMaker(name="p1", bid=D("0.42"), ask=D("0.455"), ttl_s=5)])
        adapters["polymarket_us"].rfq_ttl_s = 60
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        poly, us, kalshi = (quote_on(group, v) for v in ("polymarket", "polymarket_us", "kalshi"))
        assert poly.expires_from == "quote" and poly.expires_at == poly_quote(adapters, poly).expires_at
        assert us.expires_from == "rfq" and us.expires_at == adapters["polymarket_us"].rfqs[us.rfq_id].expires_at
        assert kalshi.expires_at is None and kalshi.expires_from is None, "Kalshi gives none: live, no countdown"
        assert group.expires_at == poly.expires_at, "the soonest live quote's expiry"
        await engine.stop()

    async def test_an_expired_quote_is_unranked_and_the_group_closes_when_every_rfq_has(self, tmp_path):
        import time as clock
        now = [clock.time()]
        adapters = venues(poly_makers=[PaperMaker(name="p1", bid=D("0.42"), ask=D("0.455"), ttl_s=5)])
        for venue in adapters.values():
            venue.clock = lambda: now[0]
            venue.rfq_ttl_s = 30
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"), cross_venue=True)
        sub = engine.bus.subscribe("rfq_group", "quote", "rfq.closed")
        now[0] += 6
        await engine.rfq.tick()
        poly = quote_on(group, "polymarket")
        assert poly.status == QuoteStatus.EXPIRED and poly.rank is None
        assert group.status == "open" and group.expires_at == quote_on(group, "kalshi").expires_at
        now[0] += 30
        engine.rfq._rfq_read.clear()   # the follower's next status read (every RFQ_STATUS_INTERVAL_S)
        await engine.rfq.tick()
        assert group.status == "expired", "every member RFQ closed at its venue: no timer of ours"
        assert all(m.status == "closed" for m in group.members)
        kinds = [e.kind for e in drain(sub)]
        assert "quote.expired" in kinds and kinds.count("rfq.closed") == 3 and "rfq_group.updated" in kinds
        await engine.stop()

    async def test_a_venue_that_closes_an_rfq_with_no_stated_expiry_is_seen(self, tmp_path):
        from synpath.trading.types import RfqCloseReason, RfqStatus
        adapters = venues()
        engine = await started(tmp_path, adapters)
        group = await engine.create_rfq_group(K_A, quantity=D("10"))
        kalshi = adapters["kalshi"]
        member = group.members[0]
        assert kalshi.rfqs[member.rfq_id].expires_at is None
        kalshi.rfqs[member.rfq_id] = kalshi.rfqs[member.rfq_id].model_copy(
            update={"status": RfqStatus.CLOSED, "closed_reason": RfqCloseReason.EXPIRED})
        engine.rfq._rfq_read.clear()
        await engine.rfq.tick()
        assert group.status == "expired" and member.status == "closed"
        await engine.stop()


def poly_quote(adapters, entry):
    return adapters["polymarket"].quotes[entry.quote_id]
