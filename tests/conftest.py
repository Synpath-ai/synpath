"""Fixtures backed by payloads recorded from the live venues.

Every sample under `tests/samples/` is a real response, captured unedited. Tests
run against those rather than hand-written dicts, so a venue changing a field
name breaks a test instead of silently changing a price.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SAMPLES = Path(__file__).parent / "samples"


def load(name: str):
    return json.loads((SAMPLES / name).read_text())


@pytest.fixture
def kalshi_events():
    return load("kalshi_events.json")


@pytest.fixture
def kalshi_market(kalshi_events):
    return kalshi_events["events"][0]["markets"][0]


@pytest.fixture
def kalshi_event(kalshi_events):
    return kalshi_events["events"][0]


@pytest.fixture
def kalshi_orderbook():
    return load("kalshi_orderbook.json")


@pytest.fixture
def kalshi_trades():
    return load("kalshi_trades.json")


@pytest.fixture
def kalshi_candles():
    return load("kalshi_candles.json")


@pytest.fixture
def kalshi_series():
    return load("kalshi_series.json")


@pytest.fixture
def poly_market():
    return load("poly_market.json")[0]


@pytest.fixture
def poly_event():
    return load("poly_events.json")["events"][0]


@pytest.fixture
def poly_book():
    return load("poly_book.json")


@pytest.fixture
def poly_trades():
    return load("poly_trades.json")


@pytest.fixture
def poly_prices():
    return load("poly_prices.json")


@pytest.fixture
def polyus_market():
    return load("polyus_market.json")["market"]


@pytest.fixture
def polyus_closed_market():
    return load("polyus_closed_market.json")["market"]


@pytest.fixture
def polyus_event():
    return load("polyus_events.json")["events"][0]


@pytest.fixture
def polyus_search():
    return load("polyus_search.json")


@pytest.fixture
def polyus_book():
    return load("polyus_book.json")


@pytest.fixture
def polyus_bbo():
    return load("polyus_bbo.json")


@pytest.fixture
def polyus_prices():
    return load("polyus_prices.json")


@pytest.fixture
def polyus_series():
    return load("polyus_series.json")


@pytest.fixture
def opinion_topics():
    return load("opinion_markets.json")["result"]["list"]


@pytest.fixture
def opinion_market():
    return load("opinion_market.json")["result"]["data"]


@pytest.fixture
def opinion_categorical():
    return load("opinion_categorical.json")["result"]["data"]


@pytest.fixture
def opinion_child():
    return load("opinion_child.json")["result"]["data"]


@pytest.fixture
def opinion_resolved():
    return load("opinion_resolved.json")["result"]["list"]
