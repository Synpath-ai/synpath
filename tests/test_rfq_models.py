"""RFQ and combo models, the capability keys, and sizing by amount (P1 of docs/rfq-combo.md).

No venue is reached.
"""
from __future__ import annotations

import importlib
from decimal import Decimal

import pytest
from pydantic import ValidationError

import synpath
from synpath.base import CAPABILITIES
from synpath.trading import (
    AcceptQuote, Combo, ComboLeg, InvalidOrder, MakerQuote, Precision, QuoteStatus, Rfq, RfqCloseReason,
    RfqQuote, RfqStatus, Side,
)
from synpath.trading.instruments import VENUE_RULES, venue_precision
from synpath.trading.money import amount_for_contracts, contracts_for_amount, outcome_price
from synpath.trading.types import QUOTE_TERMINAL_STATUSES

D = Decimal

RFQ_KEYS = ("rfq_taker", "rfq_quoter", "combos")

TRADING_ADAPTERS = {
    "kalshi": ("synpath.trading.kalshi", "KalshiTrading"),
    "polymarket": ("synpath.trading.polymarket", "PolymarketTrading"),
    "polymarket_us": ("synpath.trading.polymarket_us", "PolymarketUSTrading"),
    "polymarket_us_exchange": ("synpath.trading.polymarket_us_exchange", "PolymarketUSExchangeTrading"),
    "opinion": ("synpath.trading.opinion", "OpinionTrading"),
    "hyperliquid": ("synpath.trading.hyperliquid", "HyperliquidTrading"),
    "predict_fun": ("synpath.trading.predict_fun", "PredictFunTrading"),
    "limitless": ("synpath.trading.limitless", "LimitlessTrading"),
    "sx_bet": ("synpath.trading.sx_bet", "SxBetTrading"),
    "paper": ("synpath.engine.paper", "PaperVenue"),
}

PLANNED = {
    # docs/rfq-combo.md §2: what each venue may claim once its phase lands.
    "kalshi": {"rfq_taker", "rfq_quoter", "combos"},
    "polymarket": {"rfq_taker", "rfq_quoter", "combos"},
    "polymarket_us": {"rfq_taker", "rfq_quoter", "combos"},
    "sx_bet": {"rfq_quoter", "combos"},
}

LANDED = {
    # Phases done (docs/rfq-combo.md §8).
    "polymarket_us": {"rfq_taker", "rfq_quoter", "combos"},   # P2
    "kalshi": {"rfq_taker", "rfq_quoter", "combos"},          # P6
    "polymarket": {"rfq_taker", "rfq_quoter", "combos"},      # P7
    "sx_bet": {"rfq_quoter", "combos"},                       # P8
}


def adapter(name: str) -> type:
    module, cls = TRADING_ADAPTERS[name]
    return getattr(importlib.import_module(module), cls)


def leg(market: str, outcome: str = "yes") -> ComboLeg:
    return ComboLeg(market_id=market, outcome=outcome)


# -- capability keys -----------------------------------------------------------------


class TestCapabilities:
    def test_keys_are_known_capabilities(self):
        for key in RFQ_KEYS:
            assert key in CAPABILITIES
        assert "rfq" in CAPABILITIES       # the older key stays for one release

    @pytest.mark.parametrize("name", sorted(TRADING_ADAPTERS))
    def test_every_trading_adapter_declares_them(self, name):
        cls = adapter(name)
        declared = cls.__dict__["has"]
        for key in RFQ_KEYS:
            assert key in declared, (name, key)
            assert cls.has[key] in (True, False, "partial")

    @pytest.mark.parametrize("name", sorted(TRADING_ADAPTERS))
    def test_true_only_where_the_plan_supports_it(self, name):
        cls = adapter(name)
        for key in RFQ_KEYS:
            if cls.has[key] is not False:
                assert key in PLANNED.get(name, set()), (name, key)

    @pytest.mark.parametrize("name", sorted(TRADING_ADAPTERS))
    def test_nothing_claimed_before_its_phase(self, name):
        """A key goes True only when the adapter answering it has landed:
        Polymarket US in P2, Kalshi in P6, Polymarket global in P7; the others stay False until their
        phase lands."""
        cls = adapter(name)
        for key in RFQ_KEYS:
            assert (cls.has[key] is True) == (key in LANDED.get(name, set())), (name, key)

    def test_out_of_scope_venues_never_claim(self):
        for name in ("limitless", "opinion", "hyperliquid", "predict_fun", "polymarket_us_exchange", "paper"):
            assert all(adapter(name).has[key] is False for key in RFQ_KEYS), name

    def test_read_adapters_answer_false(self):
        for venue in synpath.exchanges.values():
            for key in RFQ_KEYS:
                assert venue.has[key] is False, (venue.id, key)


# -- combos --------------------------------------------------------------------------


class TestCombo:
    def test_a_combo_has_its_own_market_id_and_legs(self):
        combo = Combo(
            market_id="kalshi:KXMVECROSSCATEGORY-R-ABC", venue="kalshi", collection_id="KXMVECROSSCATEGORY-R",
            legs=[leg("kalshi:KXA-1"), ComboLeg(market_id="kalshi:KXB-2", outcome="no", event_id="KXB")],
            info={"raw": 1},
        )
        assert [l.outcome for l in combo.legs] == ["yes", "no"]
        assert combo.legs[1].event_id == "KXB"
        assert combo.info == {"raw": 1}
        assert combo.status is None

    def test_needs_two_legs(self):
        with pytest.raises(ValidationError, match="at least two legs"):
            Combo(market_id="polymarket_us:caoc-1", venue="polymarket_us", legs=[leg("polymarket_us:a")])

    def test_each_market_once(self):
        with pytest.raises(ValidationError, match="each market once"):
            Combo(market_id="polymarket_us:caoc-1", venue="polymarket_us",
                  legs=[leg("polymarket_us:a"), leg("polymarket_us:a", "no")])

    def test_leg_outcome_is_yes_or_no(self):
        with pytest.raises(ValidationError):
            ComboLeg(market_id="kalshi:X", outcome="maybe")

    def test_status_vocabulary(self):
        Combo(market_id="c", venue="kalshi", legs=[leg("a"), leg("b")], status="settled")
        with pytest.raises(ValidationError):
            Combo(market_id="c", venue="kalshi", legs=[leg("a"), leg("b")], status="active")

    def test_extra_fields_are_refused(self):
        with pytest.raises(ValidationError):
            ComboLeg(market_id="a", outcome="yes", side="buy")


# -- RFQ -----------------------------------------------------------------------------


class TestRfq:
    def test_on_a_market_by_contracts(self):
        rfq = Rfq(id="r1", venue="kalshi", market_id="kalshi:KXA-1", quantity=D("10"), created_at=1, expires_at=2)
        assert rfq.status == RfqStatus.OPEN
        assert rfq.instrument_id == "kalshi:KXA-1"
        assert rfq.amount is None and rfq.outcome is None and rfq.closed_reason is None

    def test_on_a_combo_by_amount(self):
        combo = Combo(market_id="polymarket_us:caoc-1", venue="polymarket_us", legs=[leg("a"), leg("b")])
        rfq = Rfq(id="r2", venue="polymarket_us", combo_id="polymarket_us:caoc-1", combo=combo, amount=D("25"))
        assert rfq.instrument_id == "polymarket_us:caoc-1"
        assert rfq.quantity is None

    def test_market_or_combo_exactly_one(self):
        with pytest.raises(ValidationError, match="exactly one"):
            Rfq(id="r", venue="kalshi", quantity=D("1"))
        with pytest.raises(ValidationError, match="exactly one"):
            Rfq(id="r", venue="kalshi", market_id="a", combo_id="b", quantity=D("1"))

    def test_combo_must_match_combo_id(self):
        combo = Combo(market_id="x", venue="kalshi", legs=[leg("a"), leg("b")])
        with pytest.raises(ValidationError, match="is not combo_id"):
            Rfq(id="r", venue="kalshi", combo_id="y", combo=combo, quantity=D("1"))

    def test_contracts_or_amount_exactly_one(self):
        with pytest.raises(ValidationError, match="not both"):
            Rfq(id="r", venue="kalshi", market_id="a", quantity=D("1"), amount=D("1"))
        with pytest.raises(ValidationError, match="give a size"):
            Rfq(id="r", venue="kalshi", market_id="a")

    def test_an_incoming_rfq_may_arrive_without_a_size(self):
        rfq = Rfq(id="r", venue="kalshi", market_id="a", mine=False)
        assert rfq.quantity is None and rfq.amount is None

    def test_sizes_are_positive(self):
        for field in ("quantity", "amount"):
            with pytest.raises(ValidationError, match="positive"):
                Rfq(id="r", venue="kalshi", market_id="a", **{field: D("0")})

    def test_one_sided_request(self):
        rfq = Rfq(id="r", venue="polymarket", combo_id="polymarket:c", outcome="yes", action="buy", amount=D("5"))
        assert (rfq.outcome, rfq.action) == ("yes", "buy")
        with pytest.raises(ValidationError, match="needs the outcome"):
            Rfq(id="r", venue="polymarket", combo_id="c", action="sell", quantity=D("5"))

    def test_closed_with_a_reason(self):
        rfq = Rfq(id="r", venue="kalshi", market_id="a", quantity=D("1"),
                  status="closed", closed_reason="executed")
        assert rfq.status == RfqStatus.CLOSED and rfq.closed_reason == RfqCloseReason.EXECUTED
        with pytest.raises(ValidationError, match="closed RFQ"):
            Rfq(id="r", venue="kalshi", market_id="a", quantity=D("1"), closed_reason="expired")

    def test_round_trips_through_json(self):
        rfq = Rfq(id="r", venue="kalshi", market_id="a", amount=D("12.50"), info={"id": "r"})
        again = Rfq.model_validate_json(rfq.model_dump_json())
        assert again == rfq and again.amount == D("12.50")

    def test_amount_is_not_the_deprecated_quantity_alias(self):
        rfq = Rfq(id="r", venue="kalshi", market_id="a", amount=D("3"))
        assert rfq.amount == D("3") and rfq.quantity is None


# -- quotes --------------------------------------------------------------------------


def quote(**kw) -> RfqQuote:
    base = dict(id="q1", rfq_id="r1", venue="kalshi", bid=D("0.40"), ask=D("0.45"),
                bid_quantity=D("10"), ask_quantity=D("8"))
    return RfqQuote(**{**base, **kw})


class TestQuote:
    def test_defaults(self):
        q = quote()
        assert q.status == QuoteStatus.OPEN and not q.is_terminal and not q.mine
        assert q.accepted_side is None and q.taker_order_id is None and q.maker_order_id is None

    def test_prices_for_each_outcome_and_action(self):
        q = quote()
        assert q.price_for("yes", "buy") == D("0.45")
        assert q.price_for("no", "sell") == D("0.45")
        assert q.price_for("yes", "sell") == D("0.40")
        assert q.price_for("no", "buy") == D("0.40")
        assert q.quantity_for("yes", "buy") == D("8")
        assert q.quantity_for("no", "buy") == D("10")

    def test_a_declined_side_is_none(self):
        q = quote(bid=None, bid_quantity=None)
        assert q.price_for("yes", "sell") is None and q.price_for("yes", "buy") == D("0.45")
        with pytest.raises(ValidationError, match="bid, an ask or both"):
            quote(bid=None, ask=None)

    def test_prices_are_yes_prices_inside_zero_one(self):
        for bad in (D("0"), D("1"), D("1.2"), D("-0.1")):
            with pytest.raises(ValidationError, match="inside"):
                quote(ask=bad)

    def test_bid_not_above_ask(self):
        quote(bid=D("0.45"), ask=D("0.45"))
        with pytest.raises(ValidationError, match="above ask"):
            quote(bid=D("0.50"), ask=D("0.45"))

    def test_sizes_positive(self):
        with pytest.raises(ValidationError, match="positive"):
            quote(ask_quantity=D("0"))

    @pytest.mark.parametrize("status", list(QuoteStatus))
    def test_terminal_statuses(self, status):
        q = quote(status=status)
        assert q.is_terminal == (status in {"executed", "canceled", "expired", "failed"})

    def test_terminal_set_matches_the_lifecycle(self):
        assert QUOTE_TERMINAL_STATUSES == {QuoteStatus.EXECUTED, QuoteStatus.CANCELED,
                                           QuoteStatus.EXPIRED, QuoteStatus.FAILED}
        assert [s.value for s in QuoteStatus] == [
            "open", "accepted", "confirmed", "executed", "canceled", "expired", "failed"]

    def test_executed_carries_both_orders(self):
        q = quote(status="executed", accepted_side="buy", taker_order_id="o1", maker_order_id="o2",
                  confirm_by=10, execute_by=20, info={"raw": True})
        assert q.accepted_side == Side.BUY and q.info == {"raw": True}

    def test_round_trip(self):
        q = quote(status="confirmed", mine=True)
        assert RfqQuote.model_validate_json(q.model_dump_json()) == q

    def test_not_the_market_data_quote(self):
        assert synpath.RfqQuote is RfqQuote and synpath.Quote is not RfqQuote


# -- accept and maker quote ------------------------------------------------------------


class TestAcceptQuote:
    def test_side_from_outcome_and_action(self):
        assert AcceptQuote(quote_id="q", outcome="yes", action="buy").side == Side.BUY
        assert AcceptQuote(quote_id="q", outcome="no", action="buy").side == Side.SELL
        assert AcceptQuote(quote_id="q", outcome="yes", action="sell").side == Side.SELL
        assert AcceptQuote(quote_id="q", outcome="no", action="sell").side == Side.BUY

    def test_outcome_and_action_required(self):
        with pytest.raises(ValidationError):
            AcceptQuote(quote_id="q", outcome="yes")

    def test_worst_price_refuses_worse(self):
        buy = AcceptQuote(quote_id="q", outcome="yes", action="buy", worst_price=D("0.45"))
        assert not buy.refuses(D("0.45")) and not buy.refuses(D("0.44")) and buy.refuses(D("0.46"))
        # Buying NO is selling YES: the worst YES price is the lowest.
        no = AcceptQuote(quote_id="q", outcome="no", action="buy", worst_price=D("0.40"))
        assert not no.refuses(D("0.41")) and no.refuses(D("0.39"))
        assert not AcceptQuote(quote_id="q", outcome="yes", action="buy").refuses(D("0.99"))

    def test_worst_price_is_a_yes_price(self):
        with pytest.raises(ValidationError, match="inside"):
            AcceptQuote(quote_id="q", outcome="yes", action="buy", worst_price=D("1"))

    def test_at_most_one_partial_size(self):
        AcceptQuote(quote_id="q", outcome="yes", action="buy", quantity=D("2"))
        with pytest.raises(ValidationError, match="not both"):
            AcceptQuote(quote_id="q", outcome="yes", action="buy", quantity=D("2"), amount=D("1"))

    def test_carries_client_id_and_attribution(self):
        a = AcceptQuote(quote_id="q", rfq_id="r", outcome="yes", action="buy", client_order_id="c1",
                        book="desk", trader="yuki", tags={"k": "v"})
        assert a.client_order_id == "c1" and a.book == "desk" and a.tags == {"k": "v"}


class TestMakerQuote:
    def test_two_sided(self):
        m = MakerQuote(rfq_id="r", bid=D("0.40"), ask=D("0.45"), quantity=D("10"), expires_at=5, client_order_id="c")
        assert m.rest_remainder is False

    def test_one_side_is_enough(self):
        MakerQuote(rfq_id="r", ask=D("0.45"))
        with pytest.raises(ValidationError, match="bid, an ask or both"):
            MakerQuote(rfq_id="r")

    def test_bid_below_ask(self):
        with pytest.raises(ValidationError, match="below ask"):
            MakerQuote(rfq_id="r", bid=D("0.45"), ask=D("0.45"))

    def test_prices_and_size_checked(self):
        with pytest.raises(ValidationError, match="inside"):
            MakerQuote(rfq_id="r", ask=D("0"))
        with pytest.raises(ValidationError, match="positive"):
            MakerQuote(rfq_id="r", ask=D("0.5"), quantity=D("-1"))


# -- sizing by amount ----------------------------------------------------------------


class TestSizing:
    def test_outcome_price(self):
        assert outcome_price(D("0.45"), "yes") == D("0.45")
        assert outcome_price(D("0.45"), "no") == D("0.55")
        with pytest.raises(InvalidOrder):
            outcome_price(D("0.45"), "maybe")

    def test_rounds_down_to_the_kalshi_step(self):
        kalshi = venue_precision("kalshi")
        assert contracts_for_amount(D("10"), D("0.30"), kalshi) == D("33.33")
        assert contracts_for_amount(D("10"), D("0.33"), kalshi) == D("30.30")

    def test_whole_contracts_on_polymarket_us(self):
        assert contracts_for_amount(D("10"), D("0.30"), venue_precision("polymarket_us")) == D("33")
        assert contracts_for_amount(D("10"), D("0.334"), venue_precision("polymarket_us")) == D("29")

    def test_whole_contracts_flag_wins_over_a_finer_step(self):
        p = Precision(tick=D("0.01"), min_quantity=D("1"), quantity_step=D("0.01"), whole_contracts=True)
        assert contracts_for_amount(D("10"), D("0.30"), p) == D("33")

    def test_no_step_uses_six_decimals(self):
        p = Precision(tick=D("0.01"), min_quantity=D("0.000001"), quantity_step=None)
        assert contracts_for_amount(D("1"), D("0.3"), p) == D("3.333333")

    @pytest.mark.parametrize("venue", sorted(VENUE_RULES))
    @pytest.mark.parametrize("amount,price", [(D("100"), D("0.37")), (D("17.77"), D("0.613")), (D("55"), D("0.0125"))])
    def test_never_spends_more_than_the_amount(self, venue, amount, price):
        precision = venue_precision(venue)
        contracts = contracts_for_amount(amount, price, precision)
        assert amount_for_contracts(contracts, price) <= amount
        step = D("1") if precision.whole_contracts else precision.quantity_step
        # One more step would overspend: the rounding is down, not truncated early.
        assert amount_for_contracts(contracts + step, price) > amount
        assert (contracts / step) % 1 == 0

    def test_below_the_minimum_quantity_is_refused(self):
        with pytest.raises(InvalidOrder, match="below the minimum 5"):
            contracts_for_amount(D("2"), D("0.50"), venue_precision("polymarket"))
        assert contracts_for_amount(D("2.5"), D("0.50"), venue_precision("polymarket")) == D("5")

    def test_rounding_to_zero_is_refused(self):
        with pytest.raises(InvalidOrder, match="below the minimum"):
            contracts_for_amount(D("0.5"), D("0.90"), venue_precision("polymarket_us"))

    def test_below_the_minimum_notional_is_refused(self):
        opinion = venue_precision("opinion")
        with pytest.raises(InvalidOrder, match="minimum order of 5"):
            contracts_for_amount(D("4.99"), D("0.50"), opinion)
        assert contracts_for_amount(D("5"), D("0.50"), opinion) == D("10")
        # Rounding down can take an amount at the minimum below it.
        with pytest.raises(InvalidOrder, match="minimum order"):
            contracts_for_amount(D("10.00"), D("0.98"), venue_precision("hyperliquid"))

    def test_worst_price_caps_a_buy(self):
        kalshi = venue_precision("kalshi")
        assert contracts_for_amount(D("10"), D("0.40"), kalshi, worst_price=D("0.40")) == D("25")
        assert contracts_for_amount(D("10"), D("0.39"), kalshi, worst_price=D("0.40")) == D("25.64")
        with pytest.raises(InvalidOrder, match="worse than the worst price"):
            contracts_for_amount(D("10"), D("0.41"), kalshi, worst_price=D("0.40"))

    def test_worst_price_caps_a_sell(self):
        kalshi = venue_precision("kalshi")
        assert contracts_for_amount(D("10"), D("0.60"), kalshi, action="sell", worst_price=D("0.55")) == D("16.66")
        with pytest.raises(InvalidOrder, match="worse"):
            contracts_for_amount(D("10"), D("0.50"), kalshi, action="sell", worst_price=D("0.55"))

    def test_spend_at_the_worst_price_stays_within_the_amount(self):
        kalshi = venue_precision("kalshi")
        worst = D("0.47")
        for price in (D("0.40"), D("0.45"), D("0.47")):
            contracts = contracts_for_amount(D("20"), price, kalshi, worst_price=worst)
            assert amount_for_contracts(contracts, price) <= D("20")
            assert price <= worst

    def test_no_side_priced_by_its_complement(self):
        # Buying NO at a YES price of 0.70 costs 0.30 a contract.
        contracts = contracts_for_amount(D("9"), outcome_price(D("0.70"), "no"), venue_precision("kalshi"))
        assert contracts == D("30")

    def test_bad_inputs(self):
        kalshi = venue_precision("kalshi")
        for amount, price in ((D("0"), D("0.5")), (D("-1"), D("0.5")), (D("1"), D("0")), (D("1"), D("1"))):
            with pytest.raises(InvalidOrder):
                contracts_for_amount(amount, price, kalshi)
        with pytest.raises(InvalidOrder, match="action"):
            contracts_for_amount(D("1"), D("0.5"), kalshi, action="hold")

    def test_floats_and_strings_are_read_exactly(self):
        assert contracts_for_amount(10, 0.3, venue_precision("kalshi")) == D("33.33")
        assert contracts_for_amount("10", "0.3", venue_precision("kalshi")) == D("33.33")

    def test_inverse(self):
        assert amount_for_contracts(D("33.33"), D("0.30")) == D("9.9990")
        assert amount_for_contracts(D("0"), D("0.30")) == D("0")
        with pytest.raises(InvalidOrder):
            amount_for_contracts(D("-1"), D("0.30"))

    def test_venue_precision(self):
        p = venue_precision("polymarket_us")
        assert p.whole_contracts and p.min_quantity == D("1") and p.tick == D("0.001")
        assert venue_precision("kalshi", tick=D("0.001")).tick == D("0.001")
        assert venue_precision("opinion").min_notional == D("5")
        with pytest.raises(KeyError):
            venue_precision("nowhere")
