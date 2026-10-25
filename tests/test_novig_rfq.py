"""Novig's parlay RFQ, the pricer's side: the webhook signature against the
official BLAKE3 vectors, quotes and last looks, the pot and settlement,
eligibility, the confirm rate, the webhook handlers, the maker WebSocket
against a fake socket, and the REST reads against a scripted venue. Frames
and bodies are the examples in Novig's RFQ documentation
(`tests/samples/novig_rfq_docs.json`); nothing here reaches the venue."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest

from synpath.trading import novig_rfq_signing as signing
from synpath.trading.credentials import CredentialsMissing, NovigRFQCredentials, load_novig_rfq
from synpath.trading.errors import InvalidOrder, PermissionDenied
from synpath.trading.novig_rfq import (
    ConfirmRate, ConfirmStanding, LastLook, NovigPricer, NovigRFQ, ParlayLeg, ParlayQuote, ParlayRFQ, PricerLock,
    PricerRegistration, RFQCollateral, RFQExecution, collateral, eligibility_problems, is_eligible, lock_of_message,
    matches_quote, ms_of, pot, round_price, settle, settlement_scalar,
)
from synpath.ws.base import StreamStatusEvent, VenueEvent
from synpath.ws.novig_rfq import NovigExecutionFeed, NovigPricerStream

from conftest import load

DOCS = load("novig_rfq_docs.json")
WS, HOOK, REST = DOCS["ws"], DOCS["webhook"], DOCS["rest"]
VECTORS = load("blake3_vectors.json")
SECRET = "0f1e2d3c4b5a69788796a5b4c3d2e1f000112233445566778899aabbccddeeff"
"""A made-up shared secret: 64 hex characters, as Novig issues them."""
RFQ_ID = WS["rfq_created"]["data"]["rfq_id"]
HOOK_RFQ = HOOK["quote_request"]["rfq_id"]


def vector_input(length: int) -> bytes:
    return bytes(i % 251 for i in range(length))


def iso(offset: float = 0.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")


def fresh(frame: dict, **data) -> dict:
    """A documented frame with its times moved to now."""
    out = json.loads(json.dumps(frame))
    out["data"].update(data)
    return out


def token_creds(**changes) -> NovigRFQCredentials:
    base = {"access_token": "test-token", "shared_secret": SECRET, "env": "qa"}
    return NovigRFQCredentials(**{**base, **changes})


def constant(price="0.45", wager="250", quote_id="mm-1"):
    calls = []

    async def price_fn(rfq):
        calls.append(rfq)
        return ParlayQuote(price=Decimal(price), max_wager=Decimal(wager), quote_id=quote_id)

    price_fn.calls = calls  # type: ignore[attr-defined]
    return price_fn


# ---------------------------------------------------------------------------
# The webhook signature
# ---------------------------------------------------------------------------

class TestSignature:
    @pytest.mark.parametrize("case", VECTORS["cases"], ids=lambda c: str(c["input_len"]))
    def test_blake3_keyed_hash_matches_the_official_vectors(self, case):
        key = VECTORS["key"].encode()
        assert signing.blake3_keyed_hash_pure(key, vector_input(case["input_len"])).hex() == case["keyed_hash"]

    @pytest.mark.parametrize("case", VECTORS["cases"][:6], ids=lambda c: str(c["input_len"]))
    def test_the_signature_is_the_keyed_hash_under_the_hex_secret(self, case):
        secret_hex = VECTORS["key"].encode().hex()
        body = vector_input(case["input_len"])
        assert signing.sign_webhook(secret_hex, body) == case["keyed_hash"]
        assert signing.verify_webhook(secret_hex, body, case["keyed_hash"])

    def test_the_installed_blake3_package_agrees_when_there_is_one(self):
        blake3 = pytest.importorskip("blake3")
        body = json.dumps(HOOK["quote_request"]).encode()
        key = bytes.fromhex(SECRET)
        assert blake3.blake3(body, key=key).digest() == signing.blake3_keyed_hash_pure(key, body)

    def test_a_body_signs_and_verifies_byte_for_byte(self):
        raw = json.dumps(HOOK["quote_request"], separators=(",", ":")).encode()
        signature = signing.sign_webhook(SECRET, raw)
        assert len(signature) == 64 and signature == signature.lower()
        assert signing.verify_webhook(SECRET, raw, signature)
        assert signing.verify_webhook(SECRET, raw, signature.upper())
        # Re-serialized JSON is different bytes.
        assert not signing.verify_webhook(SECRET, json.dumps(json.loads(raw), indent=1).encode(), signature)
        assert not signing.verify_webhook(SECRET, raw + b" ", signature)
        assert not signing.verify_webhook("ab" * 32, raw, signature)
        assert not signing.verify_webhook(SECRET, raw, None)
        assert not signing.verify_webhook(SECRET, raw, "")

    def test_the_secret_is_64_hex_characters(self):
        assert signing.secret_bytes(SECRET) == bytes.fromhex(SECRET)
        assert signing.secret_bytes(bytes.fromhex(SECRET)) == bytes.fromhex(SECRET)
        for bad in ("abc", "zz" * 32, "ab" * 31):
            with pytest.raises(ValueError):
                signing.secret_bytes(bad)


# ---------------------------------------------------------------------------
# Rounds, quotes and last looks
# ---------------------------------------------------------------------------

class TestRecords:
    def test_a_round_from_the_socket(self):
        rfq = ParlayRFQ.from_ws(WS["rfq_created"]["data"])
        assert rfq.rfq_id == RFQ_ID and rfq.legs == 2 and rfq.transport == "websocket"
        assert rfq.min_wager == Decimal("15")
        assert rfq.expires_at == ms_of("2026-01-01T00:00:03Z")
        assert rfq.outcome_ids[0] == "11111111-1111-1111-1111-111111111111"

    def test_a_round_from_the_webhook_closes_three_seconds_after_it_was_made(self):
        rfq = ParlayRFQ.from_webhook(HOOK["quote_request"])
        assert rfq.min_wager == Decimal("50.00") and rfq.transport == "webhook"
        assert rfq.expires_at - rfq.created_at == 3000
        assert rfq.created_at == ms_of("2026-04-29T18:42:11.123Z")

    def test_without_a_minimum_the_platform_minimum_applies(self):
        body = {k: v for k, v in HOOK["quote_request"].items() if k != "min_wager"}
        assert ParlayRFQ.from_webhook(body).min_wager == Decimal("10.00")
        assert ParlayRFQ.from_ws({**WS["rfq_created"]["data"], "min_wager": "5"}).min_wager == Decimal("10.00")

    def test_times(self):
        assert ms_of("2026-09-23T21:36:32.123456Z") == ms_of("2026-09-23T21:36:32.123Z")
        assert ms_of("2026-01-01T00:00:00.123456789Z") == ms_of("2026-01-01T00:00:00.123Z")
        assert ms_of(None) is None and ms_of("nonsense") is None

    def test_prices_round_up_onto_the_grid(self):
        assert round_price("0.4501") == Decimal("0.451")
        assert round_price("0.45") == Decimal("0.450")
        assert ParlayQuote(Decimal("0.12345"), Decimal("100")).checked().price == Decimal("0.124")

    @pytest.mark.parametrize("price,wager,quote_id", [
        ("0", "100", None), ("0.9995", "100", None), ("1.2", "100", None), ("0.5", "0", None),
        ("0.5", "-1", None), ("0.5", "100", "x" * 257),
    ])
    def test_quotes_novig_would_drop_are_refused(self, price, wager, quote_id):
        with pytest.raises(InvalidOrder):
            ParlayQuote(Decimal(price), Decimal(wager), quote_id).checked()

    def test_a_quote_on_each_transport(self):
        quote = ParlayQuote(Decimal("0.45"), Decimal("250.00"), "pricer-quote-xyz").checked()
        assert quote.webhook_body() == HOOK["quote_response"]
        socket = ParlayQuote(Decimal("0.45"), Decimal("250"), "mm-1").checked().ws_data(RFQ_ID)
        assert socket == {**WS["create_quote"]["data"], "price": "0.450"}
        assert all(isinstance(socket[k], str) for k in ("price", "max_wager"))
        assert "quote_id" not in ParlayQuote(Decimal("0.45"), Decimal("250")).checked().ws_data(RFQ_ID)

    def test_a_last_look_from_each_transport(self):
        look = LastLook.from_ws(WS["quote_accepted"]["data"])
        assert look.quote_id == "0193abcd-0000-7000-8000-00000000000a" and look.external_flag == "mm-1"
        assert look.wager == Decimal("50") and look.price == Decimal("0.55")
        hook = LastLook.from_webhook(HOOK["confirm_request"])
        assert hook.quote_id is None and hook.external_flag == "pricer-quote-xyz"
        assert hook.wager == Decimal("100.00") and hook.transport == "webhook"
        assert hook.deadline() - hook.received_at == 1000

    def test_the_default_policy_confirms_only_its_own_quote(self):
        quote = ParlayQuote(Decimal("0.45"), Decimal("250"), "pricer-quote-xyz").checked()
        look = LastLook.from_webhook(HOOK["confirm_request"])
        from dataclasses import replace
        assert not matches_quote(look)
        assert matches_quote(replace(look, quote=quote))
        assert not matches_quote(replace(look, quote=quote, wager=Decimal("250.01")))
        assert not matches_quote(replace(look, quote=quote, price=Decimal("0.44")))
        assert not matches_quote(replace(look, quote=quote, external_flag="someone-else"))


# ---------------------------------------------------------------------------
# The pot and settlement
# ---------------------------------------------------------------------------

class TestSettlement:
    def test_the_pot_and_collateral(self):
        assert pot("42", "0.42") == Decimal("100.00") and collateral("42", "0.42") == Decimal("58.00")
        assert pot("10", "0.3") == Decimal("33.33") and collateral("10", "0.3") == Decimal("23.33")

    @pytest.mark.parametrize("values,scalar", [
        ([0, None, None], Decimal("0")), ([0, "0.5", 1], Decimal("0")), ([1, "0.5", 1], Decimal("0.5")),
        (["0.5", "0.6", 1], Decimal("0.3")), ([1, 1, 1], Decimal("1")), ([1, None, 1], None),
        (["0.01", "0.01"], Decimal("0.001")),
    ])
    def test_the_documented_scalars(self, values, scalar):
        assert settlement_scalar(values) == scalar

    def test_a_voided_leg_splits_the_pot(self):
        out = settle("42", "0.42", [1, "0.5", 1])
        assert out.result == "fmv" and out.taker_payout == Decimal("50.00") and out.pricer_payout == Decimal("50.00")
        assert out.pricer_pnl == Decimal("-8.00")
        loss = settle("42", "0.42", [0, None])
        assert loss.result == "loss" and loss.pricer_payout == Decimal("100.00") and loss.pricer_pnl == Decimal("42.00")
        assert settle("42", "0.42", [1, None]).result == "open"

    def test_an_execution_and_its_payout(self):
        row = RFQExecution.from_row(REST["executions"][0])
        assert row.pot == Decimal("100.00") and row.liability == Decimal("58.00")
        assert row.result == "fmv" and row.fmv_value == Decimal("0.541")
        assert row.pricer_payout == Decimal("45.90") and row.pricer_pnl == Decimal("-12.10")
        assert row.executed_at == ms_of("2026-06-16T22:13:20Z") and len(row.outcome_ids) == 2


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

class TestEligibility:
    def leg(self, n, **kw):
        return ParlayLeg(outcome_id=f"o{n}", **kw)

    def test_two_to_twenty_legs(self):
        assert eligibility_problems([self.leg(1)])
        assert is_eligible([self.leg(1, event_id="a"), self.leg(2, event_id="b")])
        assert eligibility_problems([self.leg(n, event_id=f"e{n}") for n in range(21)])

    def test_leg_rules(self):
        assert eligibility_problems([self.leg(1, special=True), self.leg(2)])
        assert eligibility_problems([self.leg(1, yes_no=True, no_side=True), self.leg(2)])
        assert not eligibility_problems([self.leg(1, yes_no=True), self.leg(2)])
        assert eligibility_problems([self.leg(1, open=False), self.leg(2)])
        assert eligibility_problems([self.leg(1), self.leg(1)])

    def test_same_game_lines(self):
        game = {"event_id": "g1"}
        assert eligibility_problems([self.leg(1, market_type="spread", **game), self.leg(2, market_type="moneyline", **game)])
        assert eligibility_problems([self.leg(1, market_type="total", **game), self.leg(2, market_type="total", **game)])
        assert is_eligible([self.leg(1, market_type="spread", **game), self.leg(2, market_type="total", **game)])
        assert is_eligible([self.leg(1, market_type="spread", **game),
                            self.leg(2, market_type="spread", period="first_half", **game)])
        assert eligibility_problems([self.leg(1, market_type="first_touchdown_scorer", **game),
                                     self.leg(2, market_type="first_touchdown_scorer", **game)])
        assert eligibility_problems([self.leg(1, stats=frozenset({"power_play_points"}), player_id="p1", **game),
                                     self.leg(2, stats=frozenset({"goals"}), player_id="p2", **game)])

    def test_same_player(self):
        game = {"event_id": "g1", "player_id": "p1"}
        assert eligibility_problems([self.leg(1, market_type="player_points", **game),
                                     self.leg(2, market_type="player_points", **game)])
        assert eligibility_problems([
            self.leg(1, market_type="player_pra", stats=frozenset({"points", "rebounds", "assists"}), **game),
            self.leg(2, market_type="player_points", stats=frozenset({"points"}), **game)])
        assert is_eligible([self.leg(1, market_type="player_rebounds", stats=frozenset({"rebounds"}), **game),
                            self.leg(2, market_type="player_points", stats=frozenset({"points"}), **game)])

    def test_different_games(self):
        assert eligibility_problems([self.leg(1, event_id="g1", team_ids=("a", "b")),
                                     self.leg(2, event_id="g2", team_ids=("b", "c"))])
        assert eligibility_problems([self.leg(1, event_id="f1", future=True, league="NBA"),
                                     self.leg(2, event_id="g2", league="NBA")])
        assert is_eligible([self.leg(1, event_id="f1", future=True, league="NBA"),
                            self.leg(2, event_id="g2", league="NFL")])


# ---------------------------------------------------------------------------
# The confirm rate
# ---------------------------------------------------------------------------

class Clock:
    def __init__(self, at: float = 1_800_000_000.0):
        self.at = at

    def __call__(self) -> float:
        return self.at


def answered(rate: ConfirmRate, clock: Clock, confirms: int, rejects: int, start: int = 0) -> None:
    for i in range(confirms + rejects):
        rate.record(f"r{start + i}", i < confirms)
        clock.at += 1


class TestConfirmRate:
    def test_not_judged_below_twenty(self):
        clock = Clock()
        rate = ConfirmRate(clock=clock)
        answered(rate, clock, 0, 19)
        assert not rate.locked() and rate.standing().counted == 19 and not rate.standing().judged

    def test_below_sixty_percent_of_twenty_locks_for_five_minutes(self):
        clock = Clock()
        rate = ConfirmRate(clock=clock)
        answered(rate, clock, 12, 7)
        assert rate.standing().rejects_left() == 1 and not rate.would_lock(False)
        answered(rate, clock, 0, 1, start=19)
        assert not rate.locked() and rate.standing().rate == Decimal("0.6")
        assert rate.would_lock(False) and not rate.would_lock(True) and rate.standing().rejects_left() == 0
        answered(rate, clock, 0, 1, start=20)
        assert rate.locked() and rate.lock.source == "confirm_rate"
        assert rate.lock.until == int((clock.at - 1 + 300) * 1000)
        # While locked nothing counts, and counting restarts from zero after.
        answered(rate, clock, 0, 5, start=30)
        clock.at += 300
        assert not rate.locked() and rate.standing().counted == 0

    def test_only_the_last_thirty_minutes_count(self):
        clock = Clock()
        rate = ConfirmRate(clock=clock)
        answered(rate, clock, 0, 10)
        clock.at += 30 * 60 + 1
        answered(rate, clock, 15, 0, start=10)
        assert rate.standing().counted == 15 and rate.standing().rate == 1

    def test_a_late_confirm_is_amended_to_a_miss(self):
        clock = Clock()
        rate = ConfirmRate(clock=clock)
        rate.record("r1", True)
        rate.amend("r1", False)
        assert rate.standing().confirmed == 0 and rate.standing().counted == 1

    def test_the_venues_standing(self):
        registration = PricerRegistration.from_row(REST["pricer"])
        standing = registration.standing
        assert standing == ConfirmStanding(30, 34, 29, Decimal("0.60"), 20) and registration.lock is None
        assert standing.rate == Decimal(29) / Decimal(34) and not standing.below
        assert standing.rejects_left() == 14  # 29 / 49 >= 0.6, 29 / 50 < 0.6
        locked = PricerRegistration.from_row(REST["pricer_locked"])
        assert locked.lock == PricerLock(until=ms_of("2026-10-06T18:28:00Z"), source="confirm_rate")
        clock = Clock(ms_of("2026-10-06T18:24:10Z") / 1000)
        rate = ConfirmRate(clock=clock)
        rate.sync(locked.standing, locked.lock)
        assert rate.locked()
        clock.at = ms_of("2026-10-06T18:28:00Z") / 1000
        assert not rate.locked()
        rate.sync(standing)
        rate.record("next", False)
        assert rate.standing().counted == 35 and rate.standing().confirmed == 29

    def test_a_lock_from_the_socket_error(self):
        lock = lock_of_message(WS["error_locked"]["data"]["message"])
        assert lock == PricerLock(until=ms_of("2026-10-06T18:28:00Z"), source="confirm_rate")
        assert lock_of_message(WS["error_rfq_closed"]["data"]["message"]) is None
        assert lock_of_message("Maker is locked out of RFQ quoting.") == PricerLock(until=None, source="admin")


# ---------------------------------------------------------------------------
# The pricer
# ---------------------------------------------------------------------------

def live_rfq(rfq_id: str = RFQ_ID, transport: str = "websocket", **changes) -> ParlayRFQ:
    data = {**WS["rfq_created"]["data"], "rfq_id": rfq_id, "expires_at": iso(3), **changes}
    rfq = ParlayRFQ.from_ws(data)
    from dataclasses import replace
    return replace(rfq, transport=transport)


class TestPricer:
    def test_a_round_is_priced_once_across_transports(self):
        async def go():
            price = constant()
            pricer = NovigPricer(price, credentials=token_creds())
            a, b = await asyncio.gather(pricer.quote(live_rfq()), pricer.quote(live_rfq(transport="webhook")))
            assert a == b and a.price == Decimal("0.450") and len(price.calls) == 1
            assert pricer.round_of(RFQ_ID)[1] == a and pricer.stats.quoted == 1
        asyncio.run(go())

    def test_a_slow_answer_declines(self):
        async def go():
            async def slow(rfq):
                await asyncio.sleep(1)
                return ParlayQuote(Decimal("0.4"), Decimal("100"))
            pricer = NovigPricer(slow, credentials=token_creds(), margin=0.25)
            assert await pricer.quote(live_rfq(expires_at=iso(0.5))) is None
            assert pricer.stats.late == 1 and pricer.stats.declined == 1
        asyncio.run(go())

    def test_an_expired_round_is_not_priced(self):
        async def go():
            price = constant()
            pricer = NovigPricer(price, credentials=token_creds())
            assert await pricer.quote(ParlayRFQ.from_ws(WS["rfq_created"]["data"])) is None
            assert price.calls == []
        asyncio.run(go())

    def test_a_failing_or_invalid_answer_declines(self):
        async def go():
            def boom(rfq):
                raise RuntimeError("model down")
            pricer = NovigPricer(boom, credentials=token_creds())
            assert await pricer.quote(live_rfq()) is None and pricer.stats.errors == 1
            pricer = NovigPricer(lambda rfq: ParlayQuote(Decimal("1"), Decimal("100")), credentials=token_creds())
            assert await pricer.quote(live_rfq()) is None
        asyncio.run(go())

    def test_below_the_rounds_minimum_declines(self):
        async def go():
            pricer = NovigPricer(constant(wager="14"), credentials=token_creds())
            assert await pricer.quote(live_rfq()) is None
            pricer = NovigPricer(constant(wager="14"), credentials=token_creds(), decline_below_min_wager=False)
            assert (await pricer.quote(live_rfq())).max_wager == Decimal("14")
        asyncio.run(go())

    def test_no_pricing_while_locked_out(self):
        async def go():
            price = constant()
            pricer = NovigPricer(price, credentials=token_creds())
            pricer.confirm_rate.locked_out(until=None, source="admin")
            assert await pricer.quote(live_rfq()) is None and price.calls == []
        asyncio.run(go())

    def test_last_looks(self):
        async def go():
            pricer = NovigPricer(constant(price="0.55", wager="250"), credentials=token_creds())
            await pricer.quote(live_rfq())
            assert await pricer.last_look(LastLook.from_ws(WS["quote_accepted"]["data"]))
            await pricer.quote(live_rfq("second"))
            big = LastLook.from_ws({**WS["quote_accepted"]["data"], "rfq_id": "second", "wager": "251"})
            assert not await pricer.last_look(big)
            unknown = LastLook.from_ws({**WS["quote_accepted"]["data"], "rfq_id": "other"})
            assert not await pricer.last_look(unknown)
            standing = pricer.confirm_rate.standing()
            assert (standing.counted, standing.confirmed) == (3, 1)
            assert (pricer.stats.confirmed, pricer.stats.rejected) == (1, 2)
        asyncio.run(go())

    def test_a_confirm_callback_sees_the_quote_and_a_slow_one_rejects(self):
        async def go():
            seen = []

            async def confirm(look):
                seen.append(look)
                return look.quote is not None and look.collateral < 50

            pricer = NovigPricer(constant(price="0.55"), confirm=confirm, credentials=token_creds())
            await pricer.quote(live_rfq())
            assert await pricer.last_look(LastLook.from_ws(WS["quote_accepted"]["data"]))
            assert seen[0].quote.price == Decimal("0.550") and seen[0].collateral == Decimal("40.90")

            async def slow(look):
                await asyncio.sleep(2)
                return True

            pricer = NovigPricer(constant(price="0.55"), confirm=slow, credentials=token_creds(), confirm_timeout=0.1)
            await pricer.quote(live_rfq())
            assert not await pricer.last_look(LastLook.from_ws(WS["quote_accepted"]["data"]))
        asyncio.run(go())


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

def signed(body: dict) -> tuple[bytes, str]:
    raw = json.dumps(body).encode()
    return raw, signing.sign_webhook(SECRET, raw)


class TestWebhooks:
    def body(self, **changes):
        return {**HOOK["quote_request"], "timestamp": iso(), **changes}

    def test_a_bad_signature_is_401_and_never_priced(self):
        async def go():
            price = constant()
            pricer = NovigPricer(price, credentials=token_creds())
            raw, _ = signed(self.body())
            assert (await pricer.handle_webhook("quote", raw, "00" * 32))[0] == 401
            assert (await pricer.handle_webhook("quote", raw, None))[0] == 401
            assert price.calls == []
        asyncio.run(go())

    def test_quote_confirm_and_ping(self):
        async def go():
            pricer = NovigPricer(constant(quote_id="pricer-quote-xyz"), credentials=token_creds())
            raw, sig = signed(self.body())
            status, body = await pricer.handle_webhook("quote", raw, sig)
            assert status == 200 and body == {"price": 0.45, "max_wager": "250", "quote_id": "pricer-quote-xyz"}
            raw, sig = signed(HOOK["confirm_request"])
            assert await pricer.handle_webhook("/novig/confirm", raw, sig) == (200, {"confirmed": True})
            raw, sig = signed(HOOK["ping_request"])
            assert (await pricer.handle_webhook("ping", raw, sig))[0] == 200
        asyncio.run(go())

    def test_a_decline_is_204(self):
        async def go():
            pricer = NovigPricer(lambda rfq: None, credentials=token_creds())
            raw, sig = signed(self.body())
            assert await pricer.handle_webhook("quote", raw, sig) == (204, None)
            raw, sig = signed(HOOK["confirm_request"])
            assert await pricer.handle_webhook("confirm", raw, sig) == (200, {"confirmed": False})
        asyncio.run(go())

    def test_without_a_shared_secret_webhooks_cannot_run(self):
        async def go():
            pricer = NovigPricer(constant(), credentials=token_creds(shared_secret=None))
            with pytest.raises(CredentialsMissing):
                await pricer.handle_webhook("ping", b"{}", "x")
        asyncio.run(go())

    def test_the_app_under_the_registered_path(self):
        from fastapi.testclient import TestClient

        pricer = NovigPricer(constant(quote_id="pricer-quote-xyz"), credentials=token_creds())
        client = TestClient(pricer.webhook_app(prefix="/novig/"))
        raw, sig = signed(self.body())
        answer = client.post("/novig/quote", content=raw, headers={"X-Novig-Signature": sig})
        assert answer.status_code == 200 and answer.json()["max_wager"] == "250"
        raw, sig = signed(HOOK["confirm_request"])
        assert client.post("/novig/confirm", content=raw, headers={"X-Novig-Signature": sig}).json() == {"confirmed": True}
        raw, sig = signed(HOOK["ping_request"])
        assert client.post("/novig/ping", content=raw, headers={"X-Novig-Signature": sig}).status_code == 200
        assert client.post("/novig/ping", content=raw, headers={"X-Novig-Signature": "0" * 64}).status_code == 401
        assert client.post("/novig/quote", content=raw).status_code == 401
        declining = TestClient(NovigPricer(lambda rfq: None, credentials=token_creds()).webhook_app())
        raw, sig = signed(self.body())
        answer = declining.post("/quote", content=raw, headers={"X-Novig-Signature": sig})
        assert answer.status_code == 204 and answer.content == b""


# ---------------------------------------------------------------------------
# The maker WebSocket
# ---------------------------------------------------------------------------

class FakeSocket:
    def __init__(self):
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.sent: list = []
        self.closed = False

    async def recv(self):
        item = await self.inbox.get()
        if item is None:
            raise ConnectionError("closed")
        return item

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def close(self):
        self.closed = True

    def push(self, frame):
        self.inbox.put_nowait(json.dumps(frame))

    def events(self, name):
        return [m for m in self.sent if m["event"] == name]


async def settle_(seconds: float = 0.05):
    for _ in range(5):
        await asyncio.sleep(0)
    await asyncio.sleep(seconds)


def drain(stream) -> list:
    out = []
    while not stream._queue.empty():
        out.append(stream._queue.get_nowait())
    return out


def socket_pricer(price=None, **kwargs):
    socket = FakeSocket()
    headers = []

    async def connect(url, additional_headers=None):
        headers.append((url, additional_headers))
        return socket

    pricer = NovigPricer(price or constant(price="0.55"), credentials=token_creds(), **kwargs)
    return pricer, NovigPricerStream(pricer, connect=connect), socket, headers


class TestPricerStream:
    def test_a_round_won_and_confirmed(self):
        async def go():
            pricer, stream, socket, headers = socket_pricer()
            stream.start()
            await settle_()
            assert headers == [("wss://api-qa.novig.us/rfq/ws", {"Authorization": "Bearer test-token"})]
            assert [m["data"] for m in socket.events("subscribe")] == ["rfq", "quotes"]
            socket.push(WS["subscribed"])
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            assert socket.events("create_quote") == [{"event": "create_quote", "data": {
                "rfq_id": RFQ_ID, "price": "0.550", "max_wager": "250", "quote_id": "mm-1"}}]
            for name in ("quote_submitted", "rfq_closed", "quote_created", "quote_accepted"):
                socket.push(WS[name])
            await settle_()
            assert socket.events("confirm") == [{"event": "confirm", "data": {
                "rfq_id": RFQ_ID, "quote_id": "0193abcd-0000-7000-8000-00000000000a", "confirmed": True}}]
            socket.push(WS["confirm_recorded"])
            socket.push(WS["quote_executed"])
            await settle_()
            names = [e.name for e in drain(stream) if isinstance(e, VenueEvent)]
            assert names == ["rfq_created", "quote_submitted", "rfq_closed", "quote_created", "quote_accepted",
                             "confirm_recorded", "quote_executed"]
            assert pricer.confirm_rate.standing().confirmed == 1
            await stream.close()
        asyncio.run(go())

    def test_a_declined_round_sends_decline(self):
        async def go():
            pricer, stream, socket, _ = socket_pricer(lambda rfq: None)
            stream.start()
            await settle_()
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            assert socket.events("decline") == [{"event": "decline", "data": {"rfq_id": RFQ_ID}}]
            assert socket.events("create_quote") == []
            await stream.close()
        asyncio.run(go())

    def test_a_win_with_the_wrong_terms_is_rejected(self):
        async def go():
            pricer, stream, socket, _ = socket_pricer()
            stream.start()
            await settle_()
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            socket.push(fresh(WS["quote_accepted"], wager="999"))
            await settle_()
            assert socket.events("confirm")[0]["data"]["confirmed"] is False
            await stream.close()
        asyncio.run(go())

    def test_the_auction_closing_stops_pricing(self):
        async def go():
            async def slow(rfq):
                await asyncio.sleep(5)
                return ParlayQuote(Decimal("0.4"), Decimal("100"))
            pricer, stream, socket, _ = socket_pricer(slow)
            stream.start()
            await settle_()
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            socket.push(WS["rfq_closed"])
            await settle_()
            assert socket.events("create_quote") == [] and socket.events("decline") == []
            assert stream._pricing == {}
            await stream.close()
        asyncio.run(go())

    def test_a_confirm_rate_lock_stops_quoting(self):
        async def go():
            pricer, stream, socket, _ = socket_pricer()
            stream.start()
            await settle_()
            future = (datetime.now(timezone.utc) + timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
            socket.push(fresh(WS["error_locked"],
                              message=f"Maker is locked out of RFQ quoting until {future} for a low confirm rate."))
            await settle_()
            assert pricer.confirm_rate.locked() and pricer.confirm_rate.lock.source == "confirm_rate"
            errors = [e for e in drain(stream) if isinstance(e, StreamStatusEvent) and e.state == "error"]
            assert errors and "FORBIDDEN" in errors[0].detail
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            assert socket.events("create_quote") == [] and socket.events("decline") == []
            await stream.close()
        asyncio.run(go())

    def test_a_confirm_too_late_counts_as_a_miss(self):
        async def go():
            pricer, stream, socket, _ = socket_pricer()
            stream.start()
            await settle_()
            socket.push(fresh(WS["rfq_created"], expires_at=iso(3)))
            await settle_()
            socket.push(WS["quote_accepted"])
            await settle_()
            assert pricer.confirm_rate.standing().confirmed == 1
            socket.push({"event": "error", "data": {"code": "NO_CONFIRM_WINDOW", "message": "late",
                                                    "rfq_id": RFQ_ID}, "timestamp": iso()})
            await settle_()
            assert pricer.confirm_rate.standing().confirmed == 0
            await stream.close()
        asyncio.run(go())

    def test_a_refused_upgrade_is_final(self):
        async def go():
            class Refused(Exception):
                def __init__(self, code):
                    self.response = type("R", (), {"status_code": code})()

            async def connect(url, additional_headers=None):
                raise Refused(403)

            pricer = NovigPricer(constant(), credentials=token_creds())
            stream = NovigPricerStream(pricer, connect=connect)
            events = [e async for e in stream]
            assert events[-1].state == "failed" and "register" in events[-1].detail.lower()
        asyncio.run(go())

    def test_a_reconnect_asks_for_reconciliation(self):
        async def go():
            pricer, stream, socket, _ = socket_pricer()
            stream.backoff = lambda attempt: 0.0
            stream.start()
            await settle_()
            socket.inbox.put_nowait(None)
            await settle_()
            connected = [e for e in drain(stream) if isinstance(e, StreamStatusEvent) and e.state == "connected"]
            assert len(connected) == 2 and connected[1].reconcile_required
            await stream.close()
        asyncio.run(go())


class TestExecutionFeed:
    def test_offsets_and_gaps(self):
        async def go():
            socket = FakeSocket()

            async def connect(url, additional_headers=None):
                assert url == "wss://api-qa.novig.us/rfq/executions/feed"
                return socket

            feed = NovigExecutionFeed(token_creds(), connect=connect)
            feed.start()
            await settle_()
            frame = REST["feed_frame"]
            socket.push(frame)
            socket.push({**frame, "data": {**frame["data"], "offset": 12346}})
            socket.push({**frame, "data": {**frame["data"], "offset": 12349}})
            await settle_()
            events = drain(feed)
            assert [e.payload["offset"] for e in events if isinstance(e, VenueEvent)] == [12345, 12346, 12349]
            gaps = [e for e in events if isinstance(e, StreamStatusEvent) and e.state == "gap"]
            assert len(gaps) == 1 and gaps[0].reconcile_required and "12347 to 12348" in gaps[0].detail
            await feed.close()
        asyncio.run(go())


# ---------------------------------------------------------------------------
# REST, against a scripted venue
# ---------------------------------------------------------------------------

class Venue:
    def __init__(self):
        self.requests: list[httpx.Request] = []
        self.executions = [{**REST["executions"][0], "rfq_id": f"00000000-0000-7000-8000-{n:012d}"} for n in range(7, 0, -1)]
        self.tokens = 0
        self.reject_token: str | None = None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/nbx/v1/auth/emm-token":
            self.tokens += 1
            body = json.loads(request.content)
            assert body == {"grant_type": "client_credentials", "client_id": "cid", "client_secret": "csecret"}
            return httpx.Response(200, json={"access_token": f"token-{self.tokens}", "expires_in": 1800})
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth == f"Bearer {self.reject_token}":
            return httpx.Response(401, json={"message": "invalid token"})
        if path == "/rfq/pricer" and request.method == "GET":
            return httpx.Response(200, json=REST["pricer"])
        if path == "/rfq/executions":
            limit = int(request.url.params.get("limit", 500))
            after = request.url.params.get("after")
            rows = self.executions
            if after:
                ids = [r["rfq_id"] for r in rows]
                rows = rows[ids.index(after) + 1:] if after in ids else []
            return httpx.Response(200, json=rows[:limit])
        if path == "/rfq/collateral":
            return httpx.Response(200, json=REST["collateral"])
        return httpx.Response(404, json={"message": "not found"})


def rest(venue: Venue, **creds) -> NovigRFQ:
    credentials = NovigRFQCredentials(**({"client_id": "cid", "client_secret": "csecret", "env": "qa"} | creds))
    transport = httpx.MockTransport(venue)
    return NovigRFQ(credentials, client=httpx.AsyncClient(transport=transport),
                    token_client=httpx.AsyncClient(transport=transport))


class TestREST:
    def test_the_token_is_exchanged_once_and_sent_as_a_bearer(self):
        async def go():
            venue = Venue()
            async with rest(venue) as rfq:
                registration = await rfq.fetch_pricer()
                collateral = await rfq.fetch_collateral()
            assert venue.tokens == 1
            assert venue.requests[0].url == "https://api-qa.novig.us/nbx/v1/auth/emm-token"
            assert all(r.headers["Authorization"] == "Bearer token-1" for r in venue.requests[1:])
            assert registration.pricer_id == REST["pricer"]["pricerId"] and registration.standing.counted == 34
            assert collateral == RFQCollateral(Decimal("300.00"), 2, ms_of("2026-09-23T21:36:32.123456Z"),
                                               REST["collateral"])
        asyncio.run(go())

    def test_a_refused_token_is_renewed_once(self):
        async def go():
            venue = Venue()
            venue.reject_token = "token-1"
            async with rest(venue) as rfq:
                assert (await rfq.fetch_pricer()) is not None
            assert venue.tokens == 2
        asyncio.run(go())

    def test_no_pricer_is_none_and_403_names_registration(self):
        async def go():
            def answer(request):
                if request.url.path == "/rfq/pricer":
                    return httpx.Response(404, json={"message": "no pricer"})
                return httpx.Response(403, json={"message": "forbidden"})
            client = httpx.AsyncClient(transport=httpx.MockTransport(answer))
            async with NovigRFQ(token_creds(), client=client) as rfq:
                assert await rfq.fetch_pricer() is None
                with pytest.raises(PermissionDenied, match="registered pricer"):
                    await rfq.fetch_collateral()
        asyncio.run(go())

    def test_executions_page_and_reconcile(self):
        async def go():
            venue = Venue()
            async with rest(venue) as rfq:
                page = await rfq.fetch_executions(limit=3)
                every = [row async for row in rfq.iter_executions(page=3)]
                known = {venue.executions[4]["rfq_id"], venue.executions[5]["rfq_id"]}
                missing = await rfq.reconcile_executions(known, page=2)
                settled = await rfq.fetch_executions(status="settled", limit=2)
            assert [r.rfq_id for r in page] == [r["rfq_id"] for r in venue.executions[:3]]
            assert [r.rfq_id for r in every] == [r["rfq_id"] for r in venue.executions]
            assert [r.rfq_id for r in missing] == [r["rfq_id"] for r in venue.executions[:4]]
            sent = [r for r in venue.requests if r.url.path == "/rfq/executions"]
            assert sent[1].url.params.get("after") is None and sent[2].url.params["after"] == venue.executions[2]["rfq_id"]
            assert sent[-1].url.params["status"] == "settled" and len(settled) == 2
            with pytest.raises(ValueError):
                await rfq.fetch_executions(limit=501)
        asyncio.run(go())

    def test_registration_and_ping_send_what_the_docs_show(self):
        async def go():
            seen = []

            def answer(request):
                seen.append(request)
                if request.url.path == "/rfq/pricer/ping":
                    return httpx.Response(200, json=REST["ping"])
                return httpx.Response(200, json={**REST["register_response"], "sharedSecret": SECRET})

            client = httpx.AsyncClient(transport=httpx.MockTransport(answer))
            async with NovigRFQ(token_creds(), client=client) as rfq:
                registration = await rfq.register_pricer("https://pricer.example.com/novig")
                socket_only = await rfq.register_pricer()
                ping = await rfq.ping_webhook()
            assert json.loads(seen[0].content) == {"webhookUrl": "https://pricer.example.com/novig"}
            assert json.loads(seen[1].content) == {}
            assert registration.shared_secret == SECRET and "sharedSecret" not in registration.info
            assert SECRET not in repr(registration) and socket_only.pricer_id
            assert ping["ok"] is True and seen[2].method == "POST"
        asyncio.run(go())


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

class TestCredentials:
    def test_from_the_environment(self, tmp_path):
        env = {"NOVIG_RFQ_CLIENT_ID": "cid", "NOVIG_RFQ_CLIENT_SECRET": "csecret",
               "NOVIG_RFQ_SHARED_SECRET": SECRET.upper(), "NOVIG_RFQ_ENV": "QA"}
        creds = load_novig_rfq(env, dotenv=tmp_path / "none.env", redact_logs=False)
        assert creds.client_id == "cid" and creds.shared_secret == SECRET and creds.env == "qa"
        assert "csecret" not in repr(creds) and SECRET not in repr(creds)
        docs_name = load_novig_rfq({"NOVIG_RFQ_SECRET": SECRET}, dotenv=tmp_path / "none.env", redact_logs=False)
        assert docs_name.shared_secret == SECRET and docs_name.env == "production"
        assert load_novig_rfq({}, dotenv=tmp_path / "none.env") is None

    @pytest.mark.parametrize("env", [
        {"NOVIG_RFQ_CLIENT_ID": "cid"},
        {"NOVIG_RFQ_ACCESS_TOKEN": "t", "NOVIG_RFQ_SHARED_SECRET": "short"},
        {"NOVIG_RFQ_ACCESS_TOKEN": "t", "NOVIG_RFQ_ENV": "paper"},
    ])
    def test_mistakes_are_named(self, env, tmp_path):
        with pytest.raises(CredentialsMissing):
            load_novig_rfq(env, dotenv=tmp_path / "none.env", redact_logs=False)

    def test_no_token_source_is_named_on_first_use(self):
        async def go():
            async with NovigRFQ(NovigRFQCredentials(env="qa")) as rfq:
                with pytest.raises(CredentialsMissing, match="NOVIG_RFQ_CLIENT_ID"):
                    await rfq.token.get()
        asyncio.run(go())
